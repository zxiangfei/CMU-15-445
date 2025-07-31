/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-18 18:13:17
 * @FilePath: /CMU-15-445/src/execution/update_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->updated_ = false;                             // 初始化为未更新状态
  this->child_executor_ = std::move(child_executor);  // 移动子执行器
}

void UpdateExecutor::Init() {
  this->child_executor_->Init();  // 初始化子执行器
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;  // 如果已经更新过数据，则不再更新
  }

  updated_ = true;        // 标记为已更新状态
  int updated_count = 0;  // 记录更新的元组数量

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取要更新的表信息
  auto schema = table_info->schema_;                                          // 获取表的描述
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取表的索引

  while (child_executor_->Next(tuple, rid)) {
    //更新的逻辑是  逻辑删除旧行   然后插入新行

    //逻辑删除旧行
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);  // 更新元组的元数据，标记为已删除

    // 构造新行
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());  // 分配内存
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));  // 计算新行的值
    }
    auto new_tuple = Tuple(new_values, &schema);  // 创建新行

    // 插入新行
    std::optional<RID> insert_rid = table_info->table_->InsertTuple(TupleMeta{0, false}, new_tuple);  // 插入新行
    updated_count++;  // 记录更新的元组数量

    RID new_rid = insert_rid.value();  // 获取新行的RID

    for (auto &index : indexs) {
      // 获取新旧索引的键值
      auto new_key_tuple = new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      auto old_key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      // 删除旧索引
      index->index_->DeleteEntry(old_key_tuple, *rid, exec_ctx_->GetTransaction());
      // 插入新索引
      index->index_->InsertEntry(new_key_tuple, new_rid, exec_ctx_->GetTransaction());
    }
  }

  // 构造一个单列整形行，表示更新的元组数量
  Tuple count_tuple({Value(TypeId::INTEGER, updated_count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);  // 将更新的元组数量赋值给输出元组

  return true;  // 返回true表示有数据被更新
}

}  // namespace bustub
