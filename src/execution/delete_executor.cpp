/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-18 17:58:09
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

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取要删除的表信息
  auto schema = table_info->schema_;                                          // 获取表的描述
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取表的索引

  while (child_executor_->Next(tuple, rid)) {
    // 执行删除操作,逻辑删除，更新TupleMeta的is_deleted_为true
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);  // 更新元组的元数据，标记为已删除
    deleted_count++;                                                // 记录删除的元组数量

    // 更新索引
    for (auto &index : indexs) {
      // 获取索引的键值
      auto key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      // 删除索引
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  // 构造一个单列整形行，表示删除的元组数量
  Tuple count_tuple({Value(TypeId::INTEGER, deleted_count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);  // 将删除的元组数量赋值给输出元组

  return true;  // 成功删除，返回true
}

}  // namespace bustub
