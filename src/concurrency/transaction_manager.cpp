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

void TransactionManager::GarbageCollection() {
  auto watermark = running_txns_.GetWatermark();
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto txn = it->second;

    // RUNNING和TAINTED状态的事务不清理
    if (txn->state_ == TransactionState::RUNNING || txn->state_ == TransactionState::TAINTED) {
      ++it;
      continue;
    }

    // 有些已提交的事务没有对数据做出修改，或者为Insert操作，没有生成undo_log，则直接将其删除
    if (txn->GetUndoLogNum() == 0) {
      // 删除当前事务，并获取下一个迭代器
      it = txn_map_.erase(it);
      continue;
    }

    // 事务的读时间戳大于活跃事务最小时间戳，也不应该清理
    if (txn->GetReadTs() >= watermark) {
      it++;
      continue;
    }

    
    size_t undo_log_num = txn->GetUndoLogNum(); // 获取当前事务的undo日志数量
    std::vector<UndoLink> undo_links; // 存储当前事务的undo链

    bool can_delete = true; // 标记当前事务中某个log是否可以清理
    for(size_t i = 0;i < undo_log_num;i++){
      auto undo_log = txn->GetUndoLog(i);

      if(undo_log.ts_ > watermark) {
        can_delete = false;
        continue;
      }

      // 当前log可以删除
      auto rid = undo_log.tuple_.GetRid();
      auto undo_link = GetUndoLink(rid).value();
      
      bool flag = false;
      // 判断是否table heap中的值可以满足条件，若满足，则对应undo_log可以删除
      for(auto [table_oid,rids] : txn->GetWriteSets()){
        // 如果当前rid在这个表的写集合中
        if (rids.find(rid) != rids.end()) {
          // 获取表和tuple
          auto [meta, tuple] = catalog_->GetTable(table_oid)->table_->GetTuple(rid);
          if (meta.ts_ <= watermark) {
            flag = true;
          }
          break;
        }
      }
      if(flag) {
        continue;
      }

      while (undo_link.IsValid()) {
        // auto log = GetUndoLog(undo_link);
        // auto next_log = GetUndoLog(log.prev_version_);
        auto txn1 = txn_map_[undo_link.prev_txn_];
        auto log = txn1->GetUndoLog(undo_link.prev_log_idx_);
        if (log.ts_ == undo_log.ts_) {
          can_delete = false;
          break;
        }

        auto txn2 = txn_map_[log.prev_version_.prev_txn_];
        auto next_log = txn2->GetUndoLog(log.prev_version_.prev_log_idx_);
        if (next_log.ts_ == undo_log.ts_) {
          if (log.ts_ > watermark) {
            can_delete = false;
          } else {
            undo_links.emplace_back(undo_link);
          }
          break;
        }
        undo_link = log.prev_version_;
      }

      if (!can_delete) {
        break;
      }
    }

    if (can_delete) {
      // 删除当前事务，并获取下一个迭代器
      it = txn_map_.erase(it);

      // 修改被删除的undo_log前一个undo_log的prev_version为invalid
      for (auto &undo_link : undo_links) {
        // auto log = GetUndoLog(undo_link);
        auto txn1 = txn_map_[undo_link.prev_txn_];
        auto log = txn1->GetUndoLog(undo_link.prev_log_idx_);
        log.prev_version_ = UndoLink{};
        txn_map_[undo_link.prev_txn_]->ModifyUndoLog(undo_link.prev_log_idx_, log);
      }
    } else {
      it++;
    }
  }
}



}  // namespace bustub
