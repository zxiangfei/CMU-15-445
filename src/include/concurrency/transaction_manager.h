//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/watermark.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * TransactionManager keeps track of all the transactions running in the system.
 */
// 负责管理系统中所有事务的生命周期、时间戳分配、多版本并发控制（MVCC）以及垃圾回收相关的元数据
class TransactionManager {
 public:
 // 默认构造和析构
  TransactionManager() = default;
  ~TransactionManager() = default;

  /**
   * Begins a new transaction.
   * @param isolation_level an optional isolation level of the transaction.
   * @return an initialized transaction
   */
  // 开启新事务，分配 事务 ID、read timestamp 并注册到 Watermark 结构中，返回 Transaction*
  auto Begin(IsolationLevel isolation_level = IsolationLevel::SNAPSHOT_ISOLATION) -> Transaction *;

  /**
   * Commits a transaction.
   * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
   * yourself
   */
  // 为事务分配单调递增的 commit timestamp，执行提交逻辑（写日志、释放锁等），并从 Watermark 中移除
  auto Commit(Transaction *txn) -> bool;

  /**
   * Aborts a transaction
   * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
   */
  // 回滚事务，释放锁，清除 undo log，并从 Watermark 中移除
  void Abort(Transaction *txn);

  /**
   * @brief Update an undo link that links table heap tuple to the first undo log.
   * Before updating, `check` function will be called to ensure validity.
   */
  // 原子地更新某页某槽位的 undo 链接（版本链头），并可选地调用 check 回调做一致性校验
  auto UpdateUndoLink(RID rid, std::optional<UndoLink> prev_link,
                      std::function<bool(std::optional<UndoLink>)> &&check = nullptr) -> bool;

  /** @brief Get the first undo log of a table heap tuple. */
  // 获取某页某槽位的 undo 链接（版本链头），如果不存在则返回 std::nullopt
  auto GetUndoLink(RID rid) -> std::optional<UndoLink>;

  /** @brief Access the transaction undo log buffer and get the undo log. Return nullopt if the txn does not exist. Will
   * still throw an exception if the index is out of range. */
  // 获取事务的 undo log，返回 std::optional<UndoLog>，如果事务不存在则返回 std::nullopt
  auto GetUndoLogOptional(UndoLink link) -> std::optional<UndoLog>;

  /** @brief Access the transaction undo log buffer and get the undo log. Except when accessing the current txn buffer,
   * you should always call this function to get the undo log instead of manually retrieve the txn shared_ptr and access
   * the buffer. */
  // 获取事务的 undo log，返回 UndoLog，如果事务不存在则抛出异常
  auto GetUndoLog(UndoLink link) -> UndoLog;

  /** @brief Get the lowest read timestamp in the system. */
  // 获取系统中所有运行事务的最低 read timestamp
  auto GetWatermark() -> timestamp_t { return running_txns_.GetWatermark(); }

  /** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
   * heap. */
  // 全局停顿式 GC(Gabarage Collector，垃圾回收器)：清理表堆中所有版本链上不可见的旧版本
  void GarbageCollection();

  /** protects txn map */
  std::shared_mutex txn_map_mutex_;    // 保护事务映射的互斥锁
  /** All transactions, running or committed */
  std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> txn_map_;   // 存储所有事务（正在进行或已完成），通过 txn_id 快速访问

  /**
   * 每个页（page）对应一个 PageVersionInfo 对象，
   * prev_link_ 存储该页上每个槽位的最新 undo 链头，
   * 通过 mutex_ 保证并发安全。
   */
  struct PageVersionInfo {
    /** protects the map */
    std::shared_mutex mutex_;
    /** Stores previous version info for all slots. Note: DO NOT use `[x]` to access it because
     * it will create new elements even if it does not exist. Use `find` instead.
     */
    std::unordered_map<slot_offset_t, UndoLink> prev_link_;
  };

  /** protects version info */
  std::shared_mutex version_info_mutex_;  // 保护版本信息的互斥锁
  /** Stores the previous version of each tuple in the table heap. Do not directly access this field. Use the helper
   * functions in `transaction_manager_impl.cpp`. */
  std::unordered_map<page_id_t, std::shared_ptr<PageVersionInfo>> version_info_;  // 存储每个页（page）上所有槽（slot）的最新 undo 链头信息，用于 MVCC 版本查找

  /** Stores all the read_ts of running txns so as to facilitate garbage collection. */
  Watermark running_txns_{0};  // Watermark 结构，存所有活跃事务的 read_ts，支持 O(log N) 查询最小值

  /** Only one txn is allowed to commit at a time */
  std::mutex commit_mutex_;  // 提交互斥锁，确保同一时间只有一个事务可以提交
  /** The last committed timestamp. */
  std::atomic<timestamp_t> last_commit_ts_{0};  // 最后提交的时间戳，原子操作，确保线程安全

  /** Catalog */
  Catalog *catalog_;  // Catalog 实例，提供表、索引等元数据的访问

  std::atomic<txn_id_t> next_txn_id_{TXN_START_ID};  // 下一个事务 ID，原子操作，确保线程安全  从预定义常量 TXN_START_ID 开始增长

 private:
  /** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
   * you want. */
  // 验证事务是否满足可串行化隔离级别的要求，返回 true 表示满足要求
  // 注意：该函数不会被测试，你可以根据需要修改或删除它。
  auto VerifyTxn(Transaction *txn) -> bool;
};

/**
 * @brief Update the tuple and its undo link in the table heap atomically.
 */
// 在插入／更新元组时，原子 地写新版本到 table_heap，并维护版本链头 undo_link；check 回调可用于检测并发冲突
auto UpdateTupleAndUndoLink(
    TransactionManager *txn_mgr, RID rid, std::optional<UndoLink> undo_link, TableHeap *table_heap, Transaction *txn,
    const TupleMeta &meta, const Tuple &tuple,
    std::function<bool(const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>)> &&check = nullptr)
    -> bool;

/**
 * @brief Get the tuple and its undo link in the table heap atomically.
 */
// 读取当前可见版本的元组及其 undo_link，以便事务基于自己 read_ts 做可见性判断
auto GetTupleAndUndoLink(TransactionManager *txn_mgr, TableHeap *table_heap, RID rid)
    -> std::tuple<TupleMeta, Tuple, std::optional<UndoLink>>;

}  // namespace bustub
