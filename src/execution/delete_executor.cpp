/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-26 21:37:36
 * @FilePath: /CMU-15-445/src/execution/delete_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->deleted_ = false;                             // 初始化为未删除状态
  this->child_executor_ = std::move(child_executor);  // 移动子执行
}

void DeleteExecutor::Init() {
  this->child_executor_->Init();  // 初始化子执行器
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;  // 如果已经删除过数据，则不再删除
  }

  deleted_ = true;        // 标记为已删除状态
  int deleted_count = 0;  // 记录删除的元组数量

  auto txn_mgr = exec_ctx_->GetTransactionManager();  // 获取事务管理器
  auto txn = exec_ctx_->GetTransaction();             // 获取当前事务
  auto txn_tmp_ts = txn->GetTransactionTempTs();        // 获取事务的临时时间戳

  TupleMeta meta = TupleMeta{txn_tmp_ts, true}; // 创建元组元数据



  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取要删除的表信息
  auto schema = table_info->schema_;                                          // 获取表的描述
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取表的索引

  while (child_executor_->Next(tuple, rid)) {
    // // 执行删除操作,逻辑删除，更新TupleMeta的is_deleted_为true
    // table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);  // 更新元组的元数据，标记为已删除
    // deleted_count++;                                                // 记录删除的元组数量

    // // 更新索引
    // for (auto &index : indexs) {
    //   // 获取索引的键值
    //   auto key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
    //   // 删除索引
    //   index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    // }

    // mvcc版本
    // 1.检测写写冲突
    auto base_meta = table_info->table_->GetTupleMeta(*rid);
    if(base_meta.ts_ > txn->GetReadTs() && base_meta.ts_ != txn_tmp_ts) {
      // 发生写写冲突，事务回滚
      // 但此时还为实现Abort，先当前事务为Tainted
      txn->SetTainted();
      throw ExecutionException("Write-Write Conflict occurred. Transaction aborted.");
    }
    // 这里是为了在garbage collection中更方便
    tuple->SetRid(*rid);

    // 2.生成undo_log
    UndoLog log;
    if (base_meta.ts_ == txn_tmp_ts) {
      // 如果原本tuple对应的最新undo_link为invalid，表示原本为insert，没有生成undo_log
      if (!txn_mgr->GetUndoLink(*rid).value().IsValid()) {
        table_info->table_->UpdateTupleMeta(meta, *rid);
      } else {
        log = GenerateUpdatedUndoLog(&schema, tuple, nullptr,
                                     txn_mgr->GetUndoLog(*(txn_mgr->GetUndoLink(*rid))));
        // 更新txn中的undo_logs中的对应undo_log
        txn->ModifyUndoLog(log.prev_version_.prev_log_idx_, log);
        table_info->table_->UpdateTupleInPlace(meta, *tuple, *rid, nullptr);
      }
    } else {
      log = GenerateNewUndoLog(&schema, tuple, nullptr, base_meta.ts_, *(txn_mgr->GetUndoLink(*rid)));
      // 在txn中更新undo_logs和write_set，更新undo_link和tuple meta
      auto undo_link = txn->AppendUndoLog(std::move(log));
      txn->AppendWriteSet(table_info->oid_, *rid);
      UpdateTupleAndUndoLink(txn_mgr, *rid, std::make_optional(undo_link), table_info->table_.get(), txn, meta, *tuple, nullptr);
    }

    // 更新索引
    for (auto &index : indexs) {
      // 获取索引的键值
      auto key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      // 删除索引
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }

    // 记录删除的元组数量
    deleted_count++;
  }

  // 构造一个单列整形行，表示删除的元组数量
  Tuple count_tuple({Value(TypeId::INTEGER, deleted_count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);  // 将删除的元组数量赋值给输出元组

  return true;  // 成功删除，返回true
}

}  // namespace bustub
