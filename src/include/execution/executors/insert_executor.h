/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-18 17:46:21
 * @FilePath: /CMU-15-445/src/include/execution/executors/insert_executor.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  // 自定义添加的成员变量
  bool inserted_;                                     // 是否已经插入过数据
  std::unique_ptr<AbstractExecutor> child_executor_;  // 子执行器，用于获取要插入的元组
};

}  // namespace bustub
