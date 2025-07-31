/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-28 12:53:11
 * @FilePath: /CMU-15-445/src/include/execution/executors/nested_index_join_executor.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the nested index join should be performed in
   * @param plan the nested index join plan to be executed
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  // 自定义成员变量
  std::unique_ptr<AbstractExecutor>
      child_executor_;  // 外表执行器  SELECT * FROM t1 INNER JOIN t2 ON v1 = v3;中的t1就是外表

  Tuple outer_tuple_;  // 外表的元组

  std::vector<RID> inner_tuples_;  // 当前外表元组在内表（inner table）中通过索引查询到的所有匹配记录的 RID

  size_t inner_tuple_index_ = 0;  // 当前内表元组的索引

  bool out_tuple_is_valid_ = false;  // 是否已从外表成功获取

  bool out_match_ = false;  // 是否已从内表成功获取匹配记录
};
}  // namespace bustub
