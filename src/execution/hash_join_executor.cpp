//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/**
 * 类成员变量初始化
 * 子执行器初始化
 * 从右表中提取连接键，将右表数据存入哈希表
 */
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  // 清空哈希表
  hash_table_.clear();

  // 从右子执行器中提取连接键并填充哈希表
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    std::vector<Value> right_keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      right_keys.push_back(expr->Evaluate(&right_tuple, right_child_->GetOutputSchema()));
    }
    hash_table_[right_keys].push_back(right_tuple);
  }

  right_tuple_index_ = 0;
  left_tuple_fetched_ = false;
  has_matching_right_tuples_ = false;
  right_tuples_.clear();
}

/**
 * 逐行读取 左表的数据；
 * 获取左表tuple key；
 * 查询哈希表，看是否有匹配的 key；
 * 若匹配，将左表tuple 与每个匹配的右表 tuple 拼接成 join tuple，输出。
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 如果当前已取左表tuple，且有匹配的右表tuple，则输出
    if (left_tuple_fetched_ && right_tuple_index_ < right_tuples_.size()) {
      const Tuple &right_tuple = right_tuples_[right_tuple_index_++];  //取右表tuple
      has_matching_right_tuples_ = true;                               //标记有匹配的右表tuple
      // 拼接左表tuple和右表tuple
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(right_tuple.GetValue(&(right_child_->GetOutputSchema()), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());  //创建新的join tuple
      return true;                                 // 返回true表示成功输出一个join tuple
    }

    // 已取左tuple，但没有匹配的右表tuple，且是左连接，则需要用null填充右表部分
    if (left_tuple_fetched_ && plan_->GetJoinType() == JoinType::LEFT && !has_matching_right_tuples_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
      }
      // 右表部分填充为NULL
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(
            right_child_->GetOutputSchema().GetColumn(i).GetType()));  // 使用无效值填充
      }
      left_tuple_fetched_ = false;                 // 重置左表元组已取标志
      *tuple = Tuple(values, &GetOutputSchema());  //创建新的join tuple
      return true;                                 // 返回true表示成功输出一个join tuple
    }

    // 如果没有左表元组，则从左表获取下一个元组
    if (!left_child_->Next(&left_tuple_, rid)) {
      return false;  // 没有更多左表元组，返回false
    }

    left_tuple_fetched_ = true;          // 标记已取到左表元组
    has_matching_right_tuples_ = false;  // 重置匹配标志
    right_tuple_index_ = 0;              // 重置右表元组索引

    // 获取左表的连接键
    std::vector<Value> left_keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_keys.push_back(expr->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }

    // 在哈希表中查找匹配的右表元组
    auto it = hash_table_.find(left_keys);
    if (it != hash_table_.end()) {
      right_tuples_ = it->second;  // 获取匹配的右表元组
      right_tuple_index_ = 0;      // 重置右表元组索引
    } else {
      right_tuples_.clear();  // 没有匹配的右表元组
    }
  }
}

}  // namespace bustub
