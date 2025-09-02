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
  txn_ref->read_ts_ = last_commit_ts_.load();  // 当前事务的 read_ts 初始化为上次提交的时间戳

  running_txns_.AddTxn(txn_ref->read_ts_);  // 该事务的 read_ts 加入 Watermark 结构
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);  // 确保同一时间只有一个事务可以提交

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
  for (const auto &[table_id, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_id);
    for (auto rid : rids) {
      auto tuple_info = table_info->table_->GetTuple(rid);
      table_info->table_->UpdateTupleInPlace(TupleMeta{commit_ts, tuple_info.first.is_deleted_}, tuple_info.second,
                                             rid);  // 更新元组的元数据和内容
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
// void TransactionManager::Abort(Transaction *txn) {
//   // 只有 RUNNING 或者已被标记为 TAINTED（出现冲突）的事务才可回滚
//   if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
//     throw Exception("txn not in running / tainted state");
//   }

//   // TODO(fall2023): Implement the abort logic!

//   std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
//   // 把事务状态设为 ABORTED，并从 Watermark 中移除它的 read_ts
//   txn->state_ = TransactionState::ABORTED;
//   running_txns_.RemoveTxn(txn->read_ts_);
// }
void TransactionManager::Abort(Transaction *txn) {
  // 只有 RUNNING / TAINTED 可以回滚
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // 先把状态标成 ABORTED，并从 watermark 中移除（避免被当成活跃读者）
  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }

  const timestamp_t my_tmp = txn->GetTransactionTempTs();

  // 遍历“本事务写过的所有 RID”
  for (const auto &kv : txn->GetWriteSets()) {
    table_oid_t oid = kv.first;
    const auto &rid_set = kv.second;

    auto table_info = catalog_->GetTable(oid);
    auto *heap = table_info->table_.get();
    const Schema &schema = table_info->schema_;

    for (const RID &rid : rid_set) {
      // 取当前头 + 最新 undo_link
      TupleMeta cur_meta;
      Tuple cur_tuple;
      std::optional<UndoLink> link_opt;
      std::tie(cur_meta, cur_tuple, link_opt) = GetTupleAndUndoLink(this, heap, rid);

      // 只回滚“头还是我写的（ts==my_tmp）”的情况；否则说明已经被别人推进了，我们不动
      if (cur_meta.ts_ != my_tmp) {
        continue;
      }

      // 目标回滚结果
      TupleMeta target_meta{};
      Tuple target_tuple = cur_tuple;    // 内容占位；删除场景下内容无关
      std::optional<UndoLink> new_link;  // 回退链头

      if (!link_opt.has_value() || !link_opt->IsValid()) {
        // A) 无 undo：典型是“本事务新插入”的行（或插在已删槽但未建 log 的极端）
        // 规则：把它恢复成“删除头”；索引项不动（4.2 要求）
        target_meta = TupleMeta{/*ts=*/0, /*is_deleted=*/true};
        new_link.reset();
      } else {
        // B) 有 undo：用链首 undo 回到“写之前”的样子
        const UndoLog &log = GetUndoLog(*link_opt);

        target_meta.ts_ = log.ts_;
        target_meta.is_deleted_ = log.is_deleted_;

        if (!log.is_deleted_) {
          // 旧状态是“存在”的：把被修改的列替换回去
          std::vector<Column> cols;
          cols.reserve(schema.GetColumnCount());
          for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
            if (log.modified_fields_[i]) {
              cols.emplace_back(schema.GetColumn(i));
            }
          }
          Schema log_schema(cols);

          std::vector<Value> vals;
          vals.reserve(schema.GetColumnCount());
          int idx = 0;
          for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
            if (log.modified_fields_[i]) {
              vals.emplace_back(log.tuple_.GetValue(&log_schema, idx++));
            } else {
              vals.emplace_back(cur_tuple.GetValue(&schema, i));
            }
          }
          target_tuple = Tuple(vals, &schema);
        }
        // 回退链头
        new_link = log.prev_version_.IsValid() ? std::make_optional(log.prev_version_) : std::optional<UndoLink>{};
      }

      // 原子回写（CAS）：仅当当前头仍是我的 tmp_ts 时生效
      (void)UpdateTupleAndUndoLink(
          this, rid, new_link, heap, txn, target_meta, target_tuple,
          [my_tmp](const TupleMeta &m, const Tuple &, RID, std::optional<UndoLink>) { return m.ts_ == my_tmp; });
    }
  }

  // 不需要清理 txn 内部容器：事务对象马上就不再使用。
  // （如果你想做在线 GC，可在此选择性清掉 txn->undo_logs_、write_set_ 等，但项目测试不依赖它）
}

void TransactionManager::GarbageCollection() {
  auto watermark = running_txns_.GetWatermark();  // 系统活跃事务最小读时间
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  for (auto it = txn_map_.begin(); it != txn_map_.end();) {  // 遍历所有事务
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

    // 开始审查该事务的每一条 UndoLog：只要有一条“可能被 watermark 事务访问到”，就不能删
    size_t undo_log_num = txn->GetUndoLogNum();  // 获取当前事务的undo日志数量
    std::vector<UndoLink> undo_links;  // 记录“指向待删日志的前驱链接”，用于事后把 prev_version 置 invalid

    bool can_delete = true;  // 标记当前事务中某个log是否可以清理  假设可删，遇到“可见”则置 false
    for (size_t i = 0; i < undo_log_num; i++) {
      auto undo_log = txn->GetUndoLog(i);  // 取出第 i 条 UndoLog（含旧值 tuple、时间戳、前驱链接等）

      // 对于当前日志 ts > watermark 的情况，作为相对较新的数据，以后的事务肯定能见，所以不能删除
      if (undo_log.ts_ > watermark) {
        can_delete = false;
        continue;
      }

      /**
       * 当前日志 ts ≤ watermark，有可能被看见
       * 读事务 T（read_ts=R）
       * * 若 base.ts≤R,T 直接读基版本；链上所有 UndoLog 都对 T 不可见
       * * 若 base.ts>R,从链头起往后找第一条满足ts≤R 的日志
       */
      //根据它对应的 tuple，拿到这个tuple的版本链头部
      auto rid = undo_log.tuple_.GetRid();
      auto undo_link = GetUndoLink(rid).value();  // 版本链头部

      bool flag = false;  //     flag = true 表示“无需回溯到这条日志”
      // 如果 watermark 直接能看到 base tuple（meta.ts ≤ watermark），就不会回溯任何 UndoLog
      for (auto [table_oid, rids] : txn->GetWriteSets()) {
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
      if (flag) {  // base tuple本身可见，所以不需要回退到当前版本，can_delete仍然等于true，继续判断下一条日志
        continue;
      }

      // 该tuple对于watermark需要回退，直到定位“第一条 ts ≤ watermark 的日志”
      while (undo_link.IsValid()) {
        // 取出版本链中第一条日志
        auto txn1 = txn_map_[undo_link.prev_txn_];
        auto log = txn1->GetUndoLog(undo_link.prev_log_idx_);

        // 表头第一条就是我们正在审查的这条,
        // 此时事务不可删除，因为它可能被后续事务访问
        if (log.ts_ == undo_log.ts_) {
          can_delete = false;
          break;
        }

        // 判断我们的 undo_log 是否紧跟在 log 后面
        auto txn2 = txn_map_[log.prev_version_.prev_txn_];
        auto next_log = txn2->GetUndoLog(log.prev_version_.prev_log_idx_);
        // 这一条就是我们正在审查的这条,
        if (next_log.ts_ == undo_log.ts_) {
          if (log.ts_ > watermark) {  // 日志可能被后面的事务看见，不可删除
            can_delete = false;
          } else {  // 该日志可被删除,记录前驱链接，便于将前驱链接的prev_version 置 invalid
            undo_links.emplace_back(undo_link);
          }
          break;
        }
        undo_link = log.prev_version_;  // 继续回溯到前一个版本
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
