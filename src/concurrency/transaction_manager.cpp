//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);  // 确保线程安全地访问事务映射
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load(); // 当前事务的 read_ts 初始化为上次提交的时间戳

  running_txns_.AddTxn(txn_ref->read_ts_);  // 该事务的 read_ts 加入 Watermark 结构
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);   // 确保同一时间只有一个事务可以提交

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;  // 获取下一个提交时间戳

  if (txn->state_ != TransactionState::RUNNING) {  // 事务状态必须是 RUNNING 才能提交
    throw Exception("txn not in running state");
  }

  // 若是 SERIALIZABLE 隔离级别，则调用 VerifyTxn 做冲突检查；若失败，先释放提交锁、回滚该事务并返回 false
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // 将事务中的更新写回表
  for(const auto &[table_id, rids] : txn->GetWriteSets()){
    auto table_info = catalog_->GetTable(table_id);
    for(auto rid : rids){
      auto tuple_info = table_info->table_->GetTuple(rid);
      table_info->table_->UpdateTupleInPlace(
          TupleMeta{commit_ts, tuple_info.first.is_deleted_}, tuple_info.second, rid);  // 更新元组的元数据和内容
    }
  }


  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;  // 设置事务的提交时间戳

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_++;  // 更新最后提交的时间戳

  return true;
}

// 事务回滚
void TransactionManager::Abort(Transaction *txn) {
  // 只有 RUNNING 或者已被标记为 TAINTED（出现冲突）的事务才可回滚
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // 把事务状态设为 ABORTED，并从 Watermark 中移除它的 read_ts
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() { UNIMPLEMENTED("not implemented"); }

}  // namespace bustub
