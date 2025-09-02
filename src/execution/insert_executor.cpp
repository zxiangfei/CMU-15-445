// /*
//  * @Author: zxiangfei 2464257291@qq.com
//  * @Date: 2025-07-02 22:24:46
//  * @LastEditors: zxiangfei 2464257291@qq.com
//  * @LastEditTime: 2025-08-21 00:52:32
//  * @FilePath: /CMU-15-445/src/execution/insert_executor.cpp
//  * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
//  * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
//  */
// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // insert_executor.cpp
// //
// // Identification: src/execution/insert_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <memory>

// #include "execution/executors/insert_executor.h"
// #include "concurrency/transaction_manager.h"

// namespace bustub {

// InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
//                                std::unique_ptr<AbstractExecutor> &&child_executor)
//     : AbstractExecutor(exec_ctx) {
//   this->plan_ = plan;
//   this->inserted_ = false;                            // 初始化为未插入状态
//   this->child_executor_ = std::move(child_executor);  // 移动子执行器
// }

// void InsertExecutor::Init() {
//   this->child_executor_->Init();  // 初始化子执行器
// }

// auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if (inserted_) {
//     return false;  // 如果已经插入过数据，则不再插入
//   }

//   inserted_ = true;        // 标记为已插入状态
//   int inserted_count = 0;  // 记录插入的元组数量

//   auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取要插入的表信息
//   auto schema = table_info->schema_;                                          // 获取表的描述
//   auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取表的索引

//   // MVCC事务实现所需要的数据
//   auto txn = exec_ctx_->GetTransaction();  // 获取当前事务
//   auto temp_ts = txn->GetTransactionTempTs();  // 获取事务的临时时间戳

//   // 设置插入新的tuple的 tuplemeta
//   TupleMeta meta = {temp_ts, false};  // 创建一个元组元数据，设置时间戳和删除标志

//   // insert执行中是将所有要插入的tuple一次性插完，然后返回true
//   // 之后再进入这里的next函数时，返回false，结束insert执行
//   while (child_executor_->Next(tuple, rid)) {

//     std::optional<RID> insert_rid = table_info->table_->InsertTuple(meta, *tuple);  // 插入元组

//     *rid = insert_rid.value();  // 获取插入的RID
//     txn->AppendWriteSet(table_info->oid_, *rid);  // 将插入的RID添加到事务的写集

//     // 更新txn mgr版本链
//     exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, std::make_optional(UndoLink{}), nullptr);
//     inserted_count++;              // 记录插入的元组数量

//     for (auto &index : indexs) {
//       // 获取索引的键值
//       auto key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
//       // 插入索引
//       index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
//     }
//   }

//   //构造一个单列整形行，表示插入的元组数量
//   Tuple count_tuple({Value(TypeId::INTEGER, inserted_count)}, &GetOutputSchema());
//   *tuple = std::move(count_tuple);  // 将插入的元组数量赋值给输出元组

//   return true;  // 成功插入，返回true
// }

// }  // namespace bustub

#include <memory>

#include "common/exception.h"  // ExecutionException
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  inserted_ = false;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  inserted_ = true;

  int inserted_count = 0;

  // 基本对象
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  const Schema &schema = table_info->schema_;
  std::vector<std::shared_ptr<bustub::IndexInfo>> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  // 只找主键索引
  std::shared_ptr<bustub::IndexInfo> pk_index = nullptr;
  for (const auto &idx : indexs) {
    if (idx->is_primary_key_) {
      pk_index = idx;
      break;
    }
  }

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto temp_ts = txn->GetTransactionTempTs();
  TupleMeta meta{temp_ts, false};

  Tuple input_tuple;
  RID child_rid;
  while (child_executor_->Next(&input_tuple, &child_rid)) {
    if (pk_index != nullptr) {
      // 1) 主键索引点查
      Tuple key_tuple = input_tuple.KeyFromTuple(schema, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
      std::vector<RID> exist_rids;
      pk_index->index_->ScanKey(key_tuple, &exist_rids, txn);

      if (exist_rids.empty()) {
        // 2A) 索引不存在 -> 常规插入（写表堆 + 写索引）
        std::optional<RID> new_rid = table_info->table_->InsertTuple(meta, input_tuple);
        *rid = new_rid.value();
        txn->AppendWriteSet(table_info->oid_, *rid);
        // 初始化 undo_link（按你现有风格设置一个无效 link）
        exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, std::make_optional(UndoLink{}), nullptr);

        // 写主键索引；如被并发抢先导致唯一性冲突，InsertEntry 返回 false
        bool ok = pk_index->index_->InsertEntry(key_tuple, *rid, txn);
        if (!ok) {
          txn->SetTainted();
          throw ExecutionException("duplicate primary key");
        }

        inserted_count++;
        continue;
      }

      // 2B) 索引存在：区分“未删/已删”
      const RID target_rid = exist_rids[0];

      TupleMeta obs_meta;
      Tuple obs_tuple;
      std::optional<UndoLink> obs_link;
      std::tie(obs_meta, obs_tuple, obs_link) = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), target_rid);

      if (!obs_meta.is_deleted_) {
        // 未删 -> 真重复键
        txn->SetTainted();
        throw ExecutionException("duplicate primary key");
      }

      // 已删 -> 复活：同一 RID 上写回新内容，索引项不动
      Tuple new_tuple = input_tuple;
      new_tuple.SetRid(target_rid);
      TupleMeta new_meta{temp_ts, false};

      // 拷贝成可捕获变量（避免 structured binding 捕获限制）
      const TupleMeta obs_meta_copy = obs_meta;
      const Tuple obs_tuple_copy = obs_tuple;

      if (obs_meta.ts_ == temp_ts) {
        // 同一事务此前已写过该 RID（例如同 txn 先删后插）
        bool has_valid_link = obs_link.has_value() && obs_link->IsValid();
        if (has_valid_link) {
          // 合并到那一条 undo：保持“第一次前镜像”
          UndoLog upd = GenerateUpdatedUndoLog(&schema, &obs_tuple_copy, &new_tuple, txn_mgr->GetUndoLog(*obs_link));
          txn->ModifyUndoLog(obs_link->prev_log_idx_, upd);
        }

        bool ok = UpdateTupleAndUndoLink(
            txn_mgr, target_rid, obs_link, table_info->table_.get(), txn, new_meta, /*new tuple*/ new_tuple,
            // check：仍是我们观察到的“删除头版本”
            [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
              return (cur_m.ts_ == obs_meta_copy.ts_) && (cur_m.is_deleted_) &&
                     IsTupleContentEqual(cur_t, obs_tuple_copy);
            });
        if (!ok) {
          txn->SetTainted();
          throw ExecutionException("concurrent resurrect raced (same-txn head).");
        }

        inserted_count++;
        continue;
      }
      // 本事务首次写这个 RID（删除是别人/已提交的）
      // 生成“之前是删除”的 undo（传 base_tuple=nullptr 表示插入到已删位置）
      UndoLog log = GenerateNewUndoLog(&schema, /*base_tuple*/ nullptr, /*target_tuple*/ &new_tuple, obs_meta.ts_,
                                       obs_link.has_value() ? *obs_link : UndoLink{});
      auto new_link = txn->AppendUndoLog(std::move(log));
      txn->AppendWriteSet(table_info->oid_, target_rid);

      bool ok = UpdateTupleAndUndoLink(
          txn_mgr, target_rid, std::make_optional(new_link), table_info->table_.get(), txn, new_meta,
          /*new tuple*/ new_tuple,
          // check：仍是我们观察到的“删除头版本”
          [obs_meta_copy, obs_tuple_copy](const TupleMeta &cur_m, const Tuple &cur_t, RID, std::optional<UndoLink>) {
            return (cur_m.ts_ == obs_meta_copy.ts_) && (cur_m.is_deleted_) &&
                   IsTupleContentEqual(cur_t, obs_tuple_copy);
          });
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("concurrent resurrect raced.");
      }

      // 注意：不需要再写索引；索引项一直指向这个 RID
      inserted_count++;
      continue;
    }
    // 没有主键索引（兜底）
    std::optional<RID> new_rid = table_info->table_->InsertTuple(meta, input_tuple);
    *rid = new_rid.value();
    txn->AppendWriteSet(table_info->oid_, *rid);
    exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, std::make_optional(UndoLink{}), nullptr);
    inserted_count++;
  }

  // 输出一行：插入条数
  Tuple count_tuple({Value(TypeId::INTEGER, inserted_count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);
  return true;
}

}  // namespace bustub
