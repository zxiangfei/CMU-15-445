/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-28 20:55:53
 * @FilePath: /CMU-15-445/src/optimizer/nlj_as_hash_join.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ExtractEquiConditions(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_keys,
                           std::vector<AbstractExpressionRef> &right_keys) -> bool {
  // 如果是逻辑表达式and，递归提取子表达式
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    return ExtractEquiConditions(logic_expr->GetChildAt(0), left_keys, right_keys) &&
           ExtractEquiConditions(logic_expr->GetChildAt(1), left_keys, right_keys);
  }

  // 如果是比较表达式，检查是否为等值条件
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
    // 获取等值表达式左右两边的列值
    const auto *left_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    const auto *right_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());

    // 确保左边和右边非空
    if (left_col != nullptr && right_col != nullptr) {
      // 确保左边和右边的列值来自不同的表
      if (left_col->GetTupleIdx() != right_col->GetTupleIdx()) {
        // 归一化方向，将 tuple_idx 为 0 的放在左侧
        if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
          // 左边是左表，右边是右表，分别添加到对应的键列表
          left_keys.emplace_back(std::make_shared<ColumnValueExpression>(left_col->GetTupleIdx(), left_col->GetColIdx(),
                                                                         left_col->GetReturnType()));
          right_keys.emplace_back(std::make_shared<ColumnValueExpression>(
              right_col->GetTupleIdx(), right_col->GetColIdx(), right_col->GetReturnType()));
        } else if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
          // 左边是右表，右边是左表，交换位置
          left_keys.emplace_back(std::make_shared<ColumnValueExpression>(
              right_col->GetTupleIdx(), right_col->GetColIdx(), right_col->GetReturnType()));
          right_keys.emplace_back(std::make_shared<ColumnValueExpression>(
              left_col->GetTupleIdx(), left_col->GetColIdx(), left_col->GetReturnType()));
        }
        return true;  // 成功提取到等值条件
      }
    }
  }
  return false;  // 没有提取到等值条件
}

/**
 * 对于查询语句：SELECT * FROM A, B WHERE A.x = B.x AND A.y = B.y;
 * 原计划NestedLoopJoin：NestedLoopJoin(predicate=(A.x = B.x) AND (A.y = B.y))
 * 优化器将其转换为HashJoin：HashJoin(left_keys=[A.x, A.y], right_keys=[B.x, B.y])
 * 步骤：
 * * 1. 检查是否为NestedLoopJoin计划节点
 * * 2. 检查连接条件是否为等值条件
 * * 3. 提取左表和右表的连接键表达式
 * * 4. 创建HashJoin计划节点并返回
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  // 如果有子执行计划，先优化子执行计划
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 检查是否为NestedLoopJoin计划节点
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    // 转为NestedLoopJoinPlanNode类型
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    // 确保有两个子计划
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // 用于保存左右键
    std::vector<AbstractExpressionRef> left_keys;
    std::vector<AbstractExpressionRef> right_keys;

    // 谓词存在，提取键
    if (nlj_plan.Predicate() != nullptr && ExtractEquiConditions(nlj_plan.Predicate(), left_keys, right_keys) &&
        !left_keys.empty()) {
      // 构造新的HashJoin计划节点
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_keys), std::move(right_keys),
                                                nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;  // 返回未修改的计划
}

}  // namespace bustub
