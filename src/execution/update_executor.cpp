/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 15:44:44
 * @FilePath: /CMU-15-445/src/execution/update_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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

/**
 * 构造函数
 * @param exec_ctx 执行上下文
 * @param plan 更新计划节点
 * @param child_executor 子执行器
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), updated_(false) {
}

/**
 * 初始化更新算子
 * 
 * 在执行 Next() 前调用，用来：
 * * 初始化子执行器；
 * * 从目录中获取表元信息（TableInfo），包括 TableHeap 和索引列表；
 * * 准备任何额外状态
 */
void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

/**
 * 执行更新操作
 * 
 * 循环调用子执行器的 Next(&old_tuple, &old_rid)，取得需要更新的每一行
 * 根据 plan_->GetUpdateClauses() 构造 new_values，对指定列应用更新表达式，其余列保留旧值
 * 标记删除 旧行，并在每个索引上调用 DeleteEntry
 * 插入 新行，并在每个索引上调用 InsertEntry
 * 构造单列整型输出（记录跟新行数），首次执行返回 true，后续执行返回false
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(updated_) {
    return false;  // 已经更新过了，直接返回 false
  }

  updated_ = true;  // 标记为已更新
  int count = 0;  // 记录更新的行数

  auto schema = table_info_->schema_;  //获取表列的描述
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);  //获取表的索引列表

  while(child_executor_->Next(tuple,rid)){
    //更新逻辑是删除旧行并插入新行

    //删除旧行
    table_info_->table_->UpdateTupleMeta(TupleMeta{0,true}, *rid);  //标记旧行为删除

    //构造新行
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());//为新行分配空间
    for (const auto &expr : plan_->target_expressions_) {
      //对每个更新表达式计算新值
      new_values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple(new_values, &schema);  //构造新行

    //插入新行
    std::optional<RID> insert_rid = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple);
    count++;  //更新计数器

    RID new_rid = insert_rid.value();  //获取新行的 RID
    //对每个相关索引就行更新，先删除旧索引，再插入新索引
    for (const auto &index : indexes) {
      auto old_index_key = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      auto new_index_key = new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());

      //删除旧索引条目
      index->index_->DeleteEntry(old_index_key, *rid, exec_ctx_->GetTransaction());
      //插入新索引条目
      index->index_->InsertEntry(new_index_key, new_rid, exec_ctx_->GetTransaction());
    }
  }

  //构造一个单列整型行，表示更新的总行数
  Tuple count_tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);  //将更新行数赋值给输出参数 tuple

  return true;  //返回 true，表示成功执行了更新操作
}

}  // namespace bustub

