/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-28 14:04:29
 * @FilePath: /CMU-15-445/src/execution/nested_index_join_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();      // 初始化外表执行器
  inner_tuples_.clear();        // 清空内表元组
  inner_tuple_index_ = 0;       // 重置内表元组索引
  out_tuple_is_valid_ = false;  // 重置外表元组有效标志
  out_match_ = false;           // 重置内表匹配标志
}

/**
 * 从外表获取元组，并通过索引查询内表，返回匹配的元组。
 */
auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID out_rid;  // 输出的 RID
  while (true) {
    // 当前以获取到外表元组，并且与此外表元组匹配的内标元组还存在没有输出的情况
    if (out_tuple_is_valid_ && inner_tuple_index_ < inner_tuples_.size()) {
      auto inner_rid = inner_tuples_[inner_tuple_index_++];  // 获取当前内表元组的 RID
      auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());  // 获取内表

      // 获取此时的内表元组
      auto inner_pair = inner_table_info->table_->GetTuple(inner_rid);
      auto inner_tuple = inner_pair.second;  // 获取内表元组
      auto inner_meta = inner_pair.first;    // 获取内表元组的元数据

      if (inner_meta.is_deleted_) {
        // 如果内表元组已被删除，则继续获取下一个内表元组
        continue;
      }

      // 此时内表数据有效
      out_match_ = true;  // 标记内表匹配成功
      // 将外表元组和内表元组合并成一个新的元组
      std::vector<Value> values;
      // 将外表元组的值添加到新元组中
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple_.GetValue(&(child_executor_->GetOutputSchema()), i));
      }
      // 将内表元组的值添加到新元组中
      for (uint32_t i = 0; i < inner_table_info->schema_.GetColumnCount(); i++) {
        values.push_back(inner_tuple.GetValue(&(inner_table_info->schema_), i));
      }

      // 创建新的元组
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;  // 返回成功
    }

    // 获取到外表，但没有内表匹配记录，此时需要看是不是左连接
    if (out_tuple_is_valid_ && plan_->GetJoinType() == JoinType::LEFT && !out_match_) {
      // 如果是左连接且没有内表匹配记录，则创建一个空的元组
      std::vector<Value> values;
      // 将外表元组的值添加到新元组中
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple_.GetValue(&(child_executor_->GetOutputSchema()), i));
      }
      // 内表部分填充为 NULL
      auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());  // 获取内表
      for (uint32_t i = 0; i < inner_table_info->schema_.GetColumnCount(); i++) {
        auto column_type = inner_table_info->schema_.GetColumn(i).GetType();  // 获取内表列类型
        values.push_back(ValueFactory::GetNullValueByType(column_type));      // 添加 NULL
      }
      *tuple = Tuple(values, &plan_->OutputSchema());  // 创建新的元组
      out_tuple_is_valid_ = false;                     // 重置外表元组有效标志
      return true;                                     // 返回成功
    }

    // 如果没有外表元组可用，或者内表元组索引超出范围，则需要获取下一个外表元组
    if (!child_executor_->Next(&outer_tuple_, &out_rid)) {
      // 如果外表执行器没有更多元组，则返回 false
      return false;
    }

    out_tuple_is_valid_ = true;
    out_match_ = false;
    inner_tuples_.clear();
    inner_tuple_index_ = 0;

    // 获取外表元组的连接键值
    auto key_predicate = plan_->KeyPredicate();
    // if(key_predicate == nullptr) { // 没有键谓词  即SELECT * FROM t1 INNER JOIN t2 ON t1.v1 = t2.v3;中没有ON t1.v1 =
    // t2.v3
    //   continue;  //
    // }
    auto index_key =
        key_predicate->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());  // 评估外表元组的连接键值
    if (index_key.IsNull()) {
      if (plan_->GetJoinType() == JoinType::INNER) {
        out_tuple_is_valid_ = false;
      }
      continue;
    }

    auto index_info =
        exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), plan_->GetInnerTableOid());  // 获取索引信息
    if (index_info == nullptr) {
      continue;  // 如果索引信息为空，则继续获取下一个外表元组
    }

    // 构造索引键
    std::vector<Value> index_key_values{index_key};
    auto key_schema = index_info->key_schema_;
    Tuple index_key_tuple(index_key_values, &key_schema);  // 创建索引键元组

    // 使用索引查找内表元组
    index_info->index_->ScanKey(index_key_tuple, &inner_tuples_, exec_ctx_->GetTransaction());

    // 如果没有找到匹配的内表元组，并且是内连接，则继续获取下一个外表元组
    if (inner_tuples_.empty() && plan_->GetJoinType() == JoinType::INNER) {
      out_tuple_is_valid_ = false;  // 重置外表元组有效标志
      continue;                     // 继续获取下一个外表元组
    }

    // 如果找到了匹配的内表元组，或者是左连接且没有匹配的内表元组，下一个循环会处理
  }
}
}  // namespace bustub
