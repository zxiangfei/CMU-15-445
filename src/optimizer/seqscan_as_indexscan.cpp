/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-30 18:44:37
 * @FilePath: /CMU-15-445/src/optimizer/seqscan_as_indexscan.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * 辅助函数，判断某个谓词 expr 是否完全由目标列的“等值”条件或它们的 OR 组合构成
 * 只承认 = 比较，不支持范围或其他操作符
 * 对于 OR，要求所有子分支均为等值条件
 */
auto IsPredicateOnIndexColumn(const AbstractExpression *expr, uint32_t index_col_idx) -> bool {
  // 谓词不存在
  if (expr == nullptr) {
    return false;
  }

  // 单值比较表达式
  if (auto compare_expr = dynamic_cast<const ComparisonExpression *>(expr)) {
    // 检查是否是等值比较
    if (compare_expr->comp_type_ != ComparisonType::Equal) {
      return false;  // 只接受等值比较
    }

    // 检查左侧或右侧的列是否是目标索引列
    auto left_child = compare_expr->GetChildAt(0).get();
    auto right_child = compare_expr->GetChildAt(1).get();

    auto left_column = dynamic_cast<const ColumnValueExpression *>(left_child);  //将左侧转为 索引列
    auto right_column = dynamic_cast<const ColumnValueExpression *>(right_child);

    auto left_constant = dynamic_cast<const ConstantValueExpression *>(left_child);  //将左侧转为 常量
    auto right_constant = dynamic_cast<const ConstantValueExpression *>(right_child);

    // 索引列 = 常量  的情况
    if (left_column != nullptr && right_constant != nullptr) {
      return left_column->GetColIdx() == index_col_idx;
    }

    // 常量 = 索引列 的情况
    if (left_constant != nullptr && right_column != nullptr) {
      return right_column->GetColIdx() == index_col_idx;
    }

    // 如果是其他情况，则不满足条件
    return false;
  }
  if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr)) {  // 逻辑表达式  OR 表达式
    if (logic_expr->logic_type_ == LogicType::Or) {
      // 对于 OR 表达式，递归检查每个子表达式
      auto left_value = IsPredicateOnIndexColumn(logic_expr->GetChildAt(0).get(), index_col_idx);
      auto right_value = IsPredicateOnIndexColumn(logic_expr->GetChildAt(1).get(), index_col_idx);
      return left_value && right_value;
    }
  }
  return false;
}

/**
 * 辅助函数，提取谓词里等值条件，即索引列 = 常量 或者   常量 = 索引列 中的常量
 * 用于后续构建索引查询plan中pred_keys列表
 */
auto ExtractEqualityValue(const AbstractExpression *expr, uint32_t index_col_idx) -> std::vector<Value> {
  std::vector<Value> values;  // 初始化常量列表

  if (expr == nullptr) {  //如果谓词为空，直接返回
    return values;
  }

  //提取谓词中的常量，分为 单等值谓词 和 逻辑谓词
  if (auto compare_expr = dynamic_cast<const ComparisonExpression *>(expr)) {  // 如果是单等值谓词
    // 检查是否是等值比较
    if (compare_expr->comp_type_ == ComparisonType::Equal) {
      auto left_child = compare_expr->GetChildAt(0).get();
      auto right_child = compare_expr->GetChildAt(1).get();

      auto left_column = dynamic_cast<const ColumnValueExpression *>(left_child);
      auto right_column = dynamic_cast<const ColumnValueExpression *>(right_child);

      auto left_constant = dynamic_cast<const ConstantValueExpression *>(left_child);
      auto right_constant = dynamic_cast<const ConstantValueExpression *>(right_child);

      // 索引列 = 常量 的情况
      if (left_column != nullptr && right_constant != nullptr && left_column->GetColIdx() == index_col_idx) {
        values.push_back(right_constant->val_);
      }

      // 常量 = 索引列 的情况
      if (left_constant != nullptr && right_column != nullptr && right_column->GetColIdx() == index_col_idx) {
        values.push_back(left_constant->val_);
      }
    }
  } else if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr)) {  // 逻辑表达式  OR 表达式
    if (logic_expr->logic_type_ == LogicType::Or) {
      // 对于 OR 表达式，递归检查每个子表达式
      auto left_values = ExtractEqualityValue(logic_expr->GetChildAt(0).get(), index_col_idx);
      auto right_values = ExtractEqualityValue(logic_expr->GetChildAt(1).get(), index_col_idx);
      values.insert(values.end(), left_values.begin(), left_values.end());
      values.insert(values.end(), right_values.begin(), right_values.end());
    }
  }

  return values;
}

/**
 * 实现索引扫描优化器，从seq scan-》index scan
 * 谓词下推
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  //先优化子结点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  //克隆当前结点
  auto cloned_plan = plan->CloneWithChildren(children);

  //判断待优化节点是不是SeqScanPlanNode
  if (cloned_plan->GetType() != PlanType::SeqScan) {
    return cloned_plan;
  }
  const auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*cloned_plan);

  //获取表 元信息 table_info
  auto table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
  if (table_info == nullptr) {
    return cloned_plan;  // 如果表不存在，直接返回原计划
  }

  //必须有过滤谓词
  auto filter_predicate = seq_scan_plan.filter_predicate_;
  if (filter_predicate == nullptr) {
    return cloned_plan;  // 如果没有过滤谓词，直接返回原计划
  }

  //获取该表所有索引
  auto index_infos = catalog_.GetTableIndexes(table_info->name_);
  if (index_infos.empty()) {
    return cloned_plan;  // 如果没有索引，直接返回原计划
  }

  // 遍历所有索引，寻找匹配的索引
  for (const auto &index_info : index_infos) {
    // 检查索引是否满足B+树索引和单列索引
    if (index_info->index_type_ != IndexType::BPlusTreeIndex || index_info->key_schema_.GetColumnCount() != 1) {
      continue;
    }

    // 找到索引对应的列在表模式中的列号
    const auto &index_column_name = index_info->key_schema_.GetColumn(0).GetName();  // 获取索引列名
    uint32_t index_col_idx = UINT32_MAX;
    for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); ++i) {
      if (table_info->schema_.GetColumn(i).GetName() == index_column_name) {
        index_col_idx = i;
        break;
      }
    }
    if (index_col_idx == UINT32_MAX) {
      continue;  // 如果索引列在表中不存在，跳过
    }

    // 找到索引列在表中对应的列后，判断过滤谓词是否完全由目标列的“等值”条件或它们的 OR 组合构成
    if (!IsPredicateOnIndexColumn(filter_predicate.get(), index_col_idx)) {
      continue;  // 如果过滤谓词不是由目标列的等值条件或它们的 OR 组合构成，跳过
    }

    //获取索引列的等值条件，即索引列 = 常量 或者   常量 = 索引列 中的常量
    auto value = ExtractEqualityValue(filter_predicate.get(), index_col_idx);

    // 如果提取到常量，说明谓词有效，此时针对当前遍历到的索引   将seq scan 转为 index scan
    if (!value.empty()) {
      std::vector<AbstractExpressionRef> pred_keys;  // 用于存储索引查询的谓词键
      pred_keys.reserve(value.size());               // 预分配空间以提高性能
      for (const auto &val : value) {
        // 将每个常量值转换为 ColumnValueExpression
        pred_keys.emplace_back(std::make_shared<ConstantValueExpression>(val));
      }

      // 创建 IndexScanPlanNode
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_, index_info->index_oid_,
                                                 nullptr, std::move(pred_keys));
    }
  }

  return cloned_plan;  // 如果没有找到合适的索引，返回原计划
}

}  // namespace bustub
