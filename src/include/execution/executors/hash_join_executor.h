//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  // 连接键的hash值获取类
  struct HashKey {
    auto operator()(const std::vector<Value> &keys) const -> std::size_t {
      std::size_t hash = 0;
      for (const auto &value : keys) {
        hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&value));
      }
      return hash;
    }
  };

  // 连接键的相等比较类
  struct HashKeyEqual {
    auto operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const -> bool {
      if (lhs.size() != rhs.size()) {
        return false;
      }
      for (size_t i = 0; i < lhs.size(); ++i) {
        if (!lhs[i].CompareExactlyEquals(rhs[i])) {
          return false;
        }
      }
      return true;
    }
  };

  // 哈希表存储连接键和对应的元组
  std::unordered_map<std::vector<Value>, std::vector<Tuple>, HashKey, HashKeyEqual> hash_table_;

  // 左右子执行器
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  // 当前左子执行器的元组
  Tuple left_tuple_;

  // 当前与左子执行器元组匹配的所有右子执行器元组
  std::vector<Tuple> right_tuples_;

  // 当前右子执行器元组的索引
  size_t right_tuple_index_{0};

  // 是否取得了左子执行器的元组
  bool left_tuple_fetched_{false};

  // 当前左子执行器元组是否有匹配的右子执行器元组
  bool has_matching_right_tuples_{false};
};

}  // namespace bustub
