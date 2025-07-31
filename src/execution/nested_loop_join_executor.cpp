/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-28 14:45:51
 * @FilePath: /CMU-15-445/src/execution/nested_loop_join_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_fetched_ = false;
  left_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 如果还没有获取左侧元组，则从左侧执行器获取一个元组
    if (!left_tuple_fetched_) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;  // 如果左侧执行器没有更多元组，则返回 false
      }
      left_tuple_fetched_ = true;  // 标记已获取左侧元组
      left_matched_ = false;       // 重置左侧元组匹配状态
      right_executor_->Init();     // 重置右侧执行器
    }

    // 则从右侧执行器获取一个元组
    if (right_executor_->Next(&right_tuple_, rid)) {
      auto join_predicate = plan_->Predicate();  // 获取连接谓词
      Value result;                              //保存谓词判断结果，即左右tuple是否满足连接条件
      if (join_predicate != nullptr) {           //有谓词，评估左右tuple针对连接条件是否成立
        result = join_predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                              right_executor_->GetOutputSchema());
      } else {
        result = ValueFactory::GetBooleanValue(true);  //没有谓词，则为笛卡尔积
      }

      // 检查谓词结果
      if (!result.IsNull() && result.GetAs<bool>()) {  //谓词存在并且为真，说明左右tuple满足连接条件
        left_matched_ = true;                          //标记左侧元组至少匹配一个右侧元组
        std::vector<Value> values;                     // 合并左侧和右侧元组的值
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = Tuple(values, &GetOutputSchema());  // 创建新的元组
        return true;                                 // 返回 true，表示成功获取到一个满足条件的元组
      }
    } else {  // 针对当前的左侧元组，右侧元组遍历完之后也没有匹配到，需要判断是否是左连接，如果是左连接，右侧就补全null，不是左连接直接忽略此左元组
      if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
        // 如果是左连接且当前左侧元组没有匹配到右侧元组，则返回左侧元组和右侧的null值
        std::vector<Value> values;  // 合并左侧元组的值和右侧的null值
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {  // 遍历左侧元组的列
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {  //右侧列全部为null
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }

        *tuple = Tuple(std::move(values), &GetOutputSchema());  // 创建新的元组
        left_tuple_fetched_ = false;                            // 重置左侧元组获取状态
        return true;  // 返回 true，表示成功获取到一个满足条件的元组
      }
      left_tuple_fetched_ = false;  // 重置左侧元组获取状态
    }
  }
}

}  // namespace bustub
