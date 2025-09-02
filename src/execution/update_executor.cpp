// /*
//  * @Author: zxiangfei 2464257291@qq.com
//  * @Date: 2025-07-02 22:24:46
//  * @LastEditors: zxiangfei 2464257291@qq.com
//  * @LastEditTime: 2025-09-02 13:33:34
//  * @FilePath: /CMU-15-445/src/execution/update_executor.cpp
//  */

// #include <memory>

// #include "concurrency/transaction.h"
// #include "concurrency/transaction_manager.h"
// #include "execution/execution_common.h"
// #include "execution/executors/update_executor.h"

// namespace bustub {

// UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
//                                std::unique_ptr<AbstractExecutor> &&child_executor)
//     : AbstractExecutor(exec_ctx) {
//   plan_ = plan;
//   updated_ = false;
//   child_executor_ = std::move(child_executor);
// }

// void UpdateExecutor::Init() { child_executor_->Init(); }

// auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if (updated_) {
//     return false;
//   }
//   updated_ = true;

//   auto *txn_mgr = exec_ctx_->GetTransactionManager();
//   auto *txn = exec_ctx_->GetTransaction();
//   const auto txn_tmp_ts = txn->GetTransactionTempTs();

//   auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
//   const Schema &table_schema = table_info->schema_;

//   // 找主键索引（如果有）
//   std::shared_ptr<IndexInfo> pk_index = nullptr;
//   {
//     auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
//     for (auto &idx : indexes) {
//       if (idx->is_primary_key_) {
//         pk_index = idx;
//         break;
//       }
//     }
//   }

//   struct Item {
//     RID rid_;
//     TupleMeta obs_meta_;
//     Tuple obs_tuple_;
//     std::optional<UndoLink> obs_link_;

//     Tuple new_tuple_;

//     bool pk_changed_{false};
//     Tuple old_key_tuple_;  // 仅当 pk_changed=true 时有效
//     Tuple new_key_tuple_;  // 仅当 pk_changed=true 时有效
//   };

//   std::vector<Item> items;
//   items.reserve(64);

//   // 先把 child 全部拉进来，并计算好 new_tuple / (old_key,new_key)
//   {
//     Tuple child_tup;
//     RID child_rid;
//     while (child_executor_->Next(&child_tup, &child_rid)) {
//       // 头部快照
//       TupleMeta obs_meta;
//       Tuple obs_tuple;
//       std::optional<UndoLink> obs_link;
//       std::tie(obs_meta, obs_tuple, obs_link) = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), child_rid);

//       // 写写冲突（和 4.2 一致）
//       if (obs_meta.ts_ > txn->GetReadTs() && obs_meta.ts_ != txn_tmp_ts) {
//         txn->SetTainted();
//         throw ExecutionException("Write-Write Conflict occurred. Transaction aborted.");
//       }

//       // 计算 new_tuple
//       std::vector<Value> new_vals;
//       new_vals.reserve(table_schema.GetColumnCount());
//       for (const auto &expr : plan_->target_expressions_) {
//         new_vals.push_back(expr->Evaluate(&child_tup, child_executor_->GetOutputSchema()));
//       }
//       Tuple new_tuple(new_vals, &table_schema);

//       // 带上 RID（便于 GC / 调试）
//       child_tup.SetRid(child_rid);
//       obs_tuple.SetRid(child_rid);
//       new_tuple.SetRid(child_rid);

//       Item it{child_rid, obs_meta, obs_tuple, obs_link, new_tuple, false, Tuple{}, Tuple{}};

//       // 判断主键是否改变（若有主键索引）
//       if (pk_index != nullptr) {
//         const Schema &key_schema = pk_index->key_schema_;
//         const auto &key_attrs = pk_index->index_->GetKeyAttrs();
//         Tuple old_key = child_tup.KeyFromTuple(table_schema, key_schema, key_attrs);
//         Tuple new_key = new_tuple.KeyFromTuple(table_schema, key_schema, key_attrs);

//         bool same = true;
//         for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
//           if (old_key.GetValue(&key_schema, i).CompareEquals(new_key.GetValue(&key_schema, i)) != CmpBool::CmpTrue) {
//             same = false;
//             break;
//           }
//         }
//         it.pk_changed_ = !same;
//         if (it.pk_changed_) {
//           it.old_key_tuple_ = std::move(old_key);
//           it.new_key_tuple_ = std::move(new_key);
//         }
//       }

//       items.emplace_back(std::move(it));
//     }
//   }

//   // 先处理“主键不变”的行 —— 直接用你 4.2 的 MVCC 原子更新（不动索引）
//   int updated_count = 0;
//   const TupleMeta new_meta{txn_tmp_ts, false};

//   auto apply_heap_update = [&](const Item &it) {
//     // 复制用于 CAS 检查的快照
//     const TupleMeta obs_meta_copy = it.obs_meta_;
//     const Tuple obs_tuple_copy = it.obs_tuple_;
//     const Tuple new_tuple_copy = it.new_tuple_;

//     if (it.obs_meta_.ts_ == txn_tmp_ts) {
//       // 同事务已写过
//       bool has_link = it.obs_link_.has_value() && it.obs_link_->IsValid();
//       if (has_link) {
//         UndoLog upd =
//             GenerateUpdatedUndoLog(&table_schema, &obs_tuple_copy, &new_tuple_copy,
//             txn_mgr->GetUndoLog(*it.obs_link_));
//         txn->ModifyUndoLog(it.obs_link_->prev_log_idx_, upd);
//       }
//       bool ok = UpdateTupleAndUndoLink(
//           txn_mgr, it.rid_, it.obs_link_, table_info->table_.get(), txn, new_meta, new_tuple_copy,
//           [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
//             return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
//                    IsTupleContentEqual(cur_t, obs_tuple_copy);
//           });
//       if (!ok) {
//         txn->SetTainted();
//         throw ExecutionException("Concurrent update raced (same-txn head).");
//       }
//     } else {
//       // 首次写
//       UndoLog log = GenerateNewUndoLog(&table_schema, &obs_tuple_copy, &new_tuple_copy, it.obs_meta_.ts_,
//                                        it.obs_link_.has_value() ? *it.obs_link_ : UndoLink{});
//       auto new_link = txn->AppendUndoLog(std::move(log));
//       txn->AppendWriteSet(table_info->oid_, it.rid_);

//       bool ok = UpdateTupleAndUndoLink(
//           txn_mgr, it.rid_, std::make_optional(new_link), table_info->table_.get(), txn, new_meta, new_tuple_copy,
//           [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
//             return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
//                    IsTupleContentEqual(cur_t, obs_tuple_copy);
//           });
//       if (!ok) {
//         txn->SetTainted();
//         throw ExecutionException("Concurrent update raced.");
//       }
//     }
//   };

//   // 1) 堆更新：先做所有“主键不变”的
//   for (const auto &it : items) {
//     if (!it.pk_changed_) {
//       apply_heap_update(it);
//       updated_count++;
//     }
//   }

//   // 2) 堆更新：再做所有“主键改变”的（仍不动索引）
//   std::vector<const Item *> pk_changed_items;
//   pk_changed_items.reserve(items.size());
//   for (const auto &it : items) {
//     if (it.pk_changed_) {
//       apply_heap_update(it);
//       pk_changed_items.push_back(&it);
//       updated_count++;
//     }
//   }

//   // 3) 主键索引两阶段：删旧 → 插新
//   if (!pk_changed_items.empty()) {
//     BUSTUB_ENSURE(pk_index != nullptr, "primary key index missing");
//     auto *idx = pk_index->index_.get();

//     // 3.1 批量删旧键（全部）
//     for (const Item *it : pk_changed_items) {
//       // 旧键一定存在；
//       idx->DeleteEntry(it->old_key_tuple_, it->rid_, txn);
//     }

//     // 3.2 批量插新键（若中途失败，做本地回滚）
//     std::vector<const Item *> inserted;
//     inserted.reserve(pk_changed_items.size());
//     bool ok_all = true;
//     for (const Item *it : pk_changed_items) {
//       if (!idx->InsertEntry(it->new_key_tuple_, it->rid_, txn)) {
//         ok_all = false;
//         // 回滚：把已经插入的新键删掉，把所有旧键补回去
//         for (auto rit = inserted.rbegin(); rit != inserted.rend(); ++rit) {
//           idx->DeleteEntry((*rit)->new_key_tuple_, (*rit)->rid_, txn);
//         }
//         for (const Item *jb : pk_changed_items) {
//           idx->InsertEntry(jb->old_key_tuple_, jb->rid_, txn);
//         }
//         break;
//       }
//       inserted.push_back(it);
//     }

//     if (!ok_all) {
//       txn->SetTainted();
//       throw ExecutionException("duplicate primary key");  // 与测试期望一致的报错文案
//     }

//   }

//   Tuple out({Value(TypeId::INTEGER, updated_count)}, &GetOutputSchema());
//   *tuple = std::move(out);
//   return true;
// }

// }  // namespace bustub

/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-09-02 16:42:00
 * @FilePath: /CMU-15-445/src/execution/update_executor.cpp
 */

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  updated_ = false;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }
  updated_ = true;

  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *txn = exec_ctx_->GetTransaction();
  const auto txn_tmp_ts = txn->GetTransactionTempTs();

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto *table_heap = table_info->table_.get();
  const Schema &table_schema = table_info->schema_;

  // 定位主键索引（若存在）
  std::shared_ptr<IndexInfo> pk_index = nullptr;
  {
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto &idx : indexes) {
      if (idx->is_primary_key_) {
        pk_index = idx;
        break;
      }
    }
  }

  struct Item {
    RID rid_;
    TupleMeta obs_meta_;
    Tuple obs_tuple_;
    std::optional<UndoLink> obs_link_;
    Tuple new_tuple_;

    bool pk_changed_{false};
    Tuple old_key_tuple_;  // 当 pk_changed_=true 有效
    Tuple new_key_tuple_;  // 当 pk_changed_=true 有效
  };

  std::vector<Item> items;
  items.reserve(64);

  // 收集 child，全量构造 new_tuple，并判定是否主键变化
  {
    Tuple child_tup;
    RID child_rid;
    while (child_executor_->Next(&child_tup, &child_rid)) {
      TupleMeta obs_meta;
      Tuple obs_tuple;
      std::optional<UndoLink> obs_link;
      std::tie(obs_meta, obs_tuple, obs_link) = GetTupleAndUndoLink(txn_mgr, table_heap, child_rid);

      // 写写冲突保护
      if (obs_meta.ts_ > txn->GetReadTs() && obs_meta.ts_ != txn_tmp_ts) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict occurred. Transaction aborted.");
      }

      // 生成新值
      std::vector<Value> new_vals;
      new_vals.reserve(table_schema.GetColumnCount());
      for (const auto &expr : plan_->target_expressions_) {
        new_vals.push_back(expr->Evaluate(&child_tup, child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple(new_vals, &table_schema);

      // 带上 RID 便于调试
      child_tup.SetRid(child_rid);
      obs_tuple.SetRid(child_rid);
      new_tuple.SetRid(child_rid);

      Item it{child_rid, obs_meta, obs_tuple, obs_link, new_tuple, false, Tuple{}, Tuple{}};

      if (pk_index != nullptr) {
        const Schema &key_schema = pk_index->key_schema_;
        const auto &key_attrs = pk_index->index_->GetKeyAttrs();
        Tuple old_key = child_tup.KeyFromTuple(table_schema, key_schema, key_attrs);
        Tuple new_key = new_tuple.KeyFromTuple(table_schema, key_schema, key_attrs);

        bool same = true;
        for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
          if (old_key.GetValue(&key_schema, i).CompareEquals(new_key.GetValue(&key_schema, i)) != CmpBool::CmpTrue) {
            same = false;
            break;
          }
        }
        it.pk_changed_ = !same;
        if (it.pk_changed_) {
          it.old_key_tuple_ = std::move(old_key);
          it.new_key_tuple_ = std::move(new_key);
        }
      }

      items.emplace_back(std::move(it));
    }
  }

  int updated_count = 0;
  const TupleMeta upd_meta{txn_tmp_ts, false};

  // —— 表堆“就地更新”（主键不变） —— //
  auto apply_heap_update = [&](const Item &it) {
    const TupleMeta obs_meta_copy = it.obs_meta_;
    const Tuple obs_tuple_copy = it.obs_tuple_;
    const Tuple new_tuple_copy = it.new_tuple_;

    if (it.obs_meta_.ts_ == txn_tmp_ts) {
      bool has_link = it.obs_link_.has_value() && it.obs_link_->IsValid();
      if (has_link) {
        UndoLog upd =
            GenerateUpdatedUndoLog(&table_schema, &obs_tuple_copy, &new_tuple_copy, txn_mgr->GetUndoLog(*it.obs_link_));
        txn->ModifyUndoLog(it.obs_link_->prev_log_idx_, upd);
      }
      bool ok = UpdateTupleAndUndoLink(
          txn_mgr, it.rid_, it.obs_link_, table_heap, txn, upd_meta, new_tuple_copy,
          [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
            return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
                   IsTupleContentEqual(cur_t, obs_tuple_copy);
          });
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("Concurrent update raced (same-txn head).");
      }
    } else {
      UndoLog log = GenerateNewUndoLog(&table_schema, &obs_tuple_copy, &new_tuple_copy, it.obs_meta_.ts_,
                                       it.obs_link_.has_value() ? *it.obs_link_ : UndoLink{});
      auto new_link = txn->AppendUndoLog(std::move(log));
      txn->AppendWriteSet(table_info->oid_, it.rid_);

      bool ok = UpdateTupleAndUndoLink(
          txn_mgr, it.rid_, std::make_optional(new_link), table_heap, txn, upd_meta, new_tuple_copy,
          [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
            return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
                   IsTupleContentEqual(cur_t, obs_tuple_copy);
          });
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("Concurrent update raced.");
      }
    }
  };

  // —— 表堆“打删除标记”（主键将变化） —— //
  auto apply_heap_delete = [&](const Item &it) {
    const TupleMeta obs_meta_copy = it.obs_meta_;
    const Tuple obs_tuple_copy = it.obs_tuple_;
    const TupleMeta del_meta{txn_tmp_ts, true};

    if (it.obs_meta_.ts_ == txn_tmp_ts) {
      bool has_valid_link = it.obs_link_.has_value() && it.obs_link_->IsValid();
      if (has_valid_link) {
        UndoLog upd =
            GenerateUpdatedUndoLog(&table_schema, &obs_tuple_copy, nullptr, txn_mgr->GetUndoLog(*it.obs_link_));
        txn->ModifyUndoLog(it.obs_link_->prev_log_idx_, upd);
      }
      bool ok = UpdateTupleAndUndoLink(
          txn_mgr, it.rid_, it.obs_link_, table_heap, txn, del_meta, obs_tuple_copy,
          [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
            return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
                   IsTupleContentEqual(cur_t, obs_tuple_copy);
          });
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("Concurrent delete raced (same-txn head).");
      }
    } else {
      UndoLog log = GenerateNewUndoLog(&table_schema, &obs_tuple_copy, nullptr, it.obs_meta_.ts_,
                                       it.obs_link_.has_value() ? *it.obs_link_ : UndoLink{});
      auto new_link = txn->AppendUndoLog(std::move(log));
      txn->AppendWriteSet(table_info->oid_, it.rid_);

      bool ok = UpdateTupleAndUndoLink(
          txn_mgr, it.rid_, std::make_optional(new_link), table_heap, txn, del_meta, obs_tuple_copy,
          [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
            return (cur_m.ts_ == obs_meta_copy.ts_) && (!cur_m.is_deleted_) &&
                   IsTupleContentEqual(cur_t, obs_tuple_copy);
          });
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("Concurrent delete raced.");
      }
    }
  };

  // 1) 主键不变：直接表堆更新
  for (const auto &it : items) {
    if (!it.pk_changed_) {
      apply_heap_update(it);
      updated_count++;
    }
  }

  // 收集“主键变化”的项
  std::vector<const Item *> pk_changed_items;
  pk_changed_items.reserve(items.size());
  for (const auto &it : items) {
    if (it.pk_changed_) {
      pk_changed_items.push_back(&it);
    }
  }

  if (!pk_changed_items.empty() && pk_index == nullptr) {
    // 没有主键索引却要改主键 —— 本项目不会出现；防卫
    txn->SetTainted();
    throw ExecutionException("primary key index missing");
  }

  // 2) Phase-A：先统一打删除标记（不动索引）
  for (const Item *it : pk_changed_items) {
    apply_heap_delete(*it);
    updated_count++;
  }

  // 辅助：检查一个 RID 的头版本是否“已删”
  auto head_is_deleted = [&](const RID &r) {
    TupleMeta m;
    Tuple t;
    std::optional<UndoLink> l;
    std::tie(m, t, l) = GetTupleAndUndoLink(txn_mgr, table_heap, r);
    return m.is_deleted_;
  };

  // 3) Phase-B：逐项插回（复活 / 真插入）
  if (!pk_changed_items.empty()) {
    auto *idx = pk_index->index_.get();

    // (0) 同一批次两个不同 RID 要写成相同新键，直接冲突
    const Schema &key_schema = pk_index->key_schema_;
    auto key_equal = [&](const Tuple &a, const Tuple &b) {
      for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
        if (a.GetValue(&key_schema, i).CompareEquals(b.GetValue(&key_schema, i)) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    };
    for (size_t i = 0; i + 1 < pk_changed_items.size(); ++i) {
      for (size_t j = i + 1; j < pk_changed_items.size(); ++j) {
        if (key_equal(pk_changed_items[i]->new_key_tuple_, pk_changed_items[j]->new_key_tuple_) &&
            !(pk_changed_items[i]->rid_ == pk_changed_items[j]->rid_)) {
          txn->SetTainted();
          throw ExecutionException("duplicate primary key");
        }
      }
    }

    // (1) 逐行插回
    for (const Item *it : pk_changed_items) {
      std::vector<RID> hits;
      idx->ScanKey(it->new_key_tuple_, &hits, txn);

      if (hits.empty()) {
        // 目标键不存在 —— 真插入（分配新 RID + 新建索引项）
        Tuple t = it->new_tuple_;
        TupleMeta m{txn_tmp_ts, false};
        std::optional<RID> new_rid = table_heap->InsertTuple(m, t);
        if (!new_rid.has_value()) {
          txn->SetTainted();
          throw ExecutionException("failed to insert tuple");
        }
        txn->AppendWriteSet(table_info->oid_, *new_rid);
        exec_ctx_->GetTransactionManager()->UpdateUndoLink(*new_rid, std::make_optional(UndoLink{}), nullptr);

        if (!idx->InsertEntry(it->new_key_tuple_, *new_rid, txn)) {
          txn->SetTainted();
          throw ExecutionException("duplicate primary key");
        }
        continue;
      }

      // 命中（主键唯一，命中数应为 1）
      RID target = hits[0];

      if (!head_is_deleted(target)) {
        // 命中的是“外部稳定的未删版本” —— 真冲突
        txn->SetTainted();
        throw ExecutionException("duplicate primary key");
      }

      // 复活该 RID：不新增索引项，只把该 RID 的头版本从 <del> 改为新 tuple
      TupleMeta obs_meta;
      Tuple obs_tuple;
      std::optional<UndoLink> obs_link;
      std::tie(obs_meta, obs_tuple, obs_link) = GetTupleAndUndoLink(txn_mgr, table_heap, target);

      // 拷贝以用于 lambda 捕获
      const TupleMeta obs_meta_copy = obs_meta;
      const Tuple obs_tuple_copy = obs_tuple;
      Tuple new_tuple_copy = it->new_tuple_;
      new_tuple_copy.SetRid(target);
      TupleMeta ins_meta{txn_tmp_ts, false};

      if (obs_meta.ts_ == txn_tmp_ts) {
        bool has_valid_link = obs_link.has_value() && obs_link->IsValid();
        if (has_valid_link) {
          UndoLog upd = GenerateUpdatedUndoLog(&table_schema, &obs_tuple_copy, &new_tuple_copy,
                                               txn_mgr->GetUndoLog(*obs_link));  // 合并到已有 undo
          txn->ModifyUndoLog(obs_link->prev_log_idx_, upd);
        }
        bool ok = UpdateTupleAndUndoLink(
            txn_mgr, target, obs_link, table_heap, txn, ins_meta, new_tuple_copy,
            [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
              return (cur_m.ts_ == obs_meta_copy.ts_) && (cur_m.is_deleted_) &&
                     IsTupleContentEqual(cur_t, obs_tuple_copy);
            });
        if (!ok) {
          txn->SetTainted();
          throw ExecutionException("concurrent resurrect raced (same-txn head).");
        }
      } else {
        // 本事务首次把已删头版本改为有效 —— 生成“从删除到插入”的 undo
        UndoLog log = GenerateNewUndoLog(&table_schema, /*base_tuple*/ nullptr, /*target_tuple*/ &new_tuple_copy,
                                         obs_meta.ts_, obs_link.has_value() ? *obs_link : UndoLink{});
        auto new_link = txn->AppendUndoLog(std::move(log));
        txn->AppendWriteSet(table_info->oid_, target);

        bool ok = UpdateTupleAndUndoLink(
            txn_mgr, target, std::make_optional(new_link), table_heap, txn, ins_meta, new_tuple_copy,
            [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
              return (cur_m.ts_ == obs_meta_copy.ts_) && (cur_m.is_deleted_) &&
                     IsTupleContentEqual(cur_t, obs_tuple_copy);
            });
        if (!ok) {
          txn->SetTainted();
          throw ExecutionException("concurrent resurrect raced.");
        }
      }
    }
  }

  Tuple out({Value(TypeId::INTEGER, updated_count)}, &GetOutputSchema());
  *tuple = std::move(out);
  return true;
}

}  // namespace bustub
