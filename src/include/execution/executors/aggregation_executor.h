//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
//简化版的聚合哈希表，用于存储分组键（Group Key）与其对应的聚合结果（Aggregate Value）
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return The initial aggregate value for this aggregation executor */
  /**
   * 根据聚合类型，生成每组初始聚合值：
   * COUNT(*) 为 0；
   * 其他聚合类型设为 NULL
   */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  /**
   * TODO部分：你需要实现这个函数逻辑：
   * 将新输入 input 合并到已有结果 result 中。
   * 例如对于 SUM 类型，result += input，注意处理 NULL。
   */
  /**
   * 举例： SELECT COUNT(*), SUM(salary), MIN(age) FROM employee GROUP BY dept_id;
   * 表为：tuple (dept_id=1, salary=5000, age=28)
   *      tuple (dept_id=2, salary=6000, age=30)
   * 则 agg_types_ = { CountStarAggregate, SumAggregate, MinAggregate };
   * result.aggregates_ = {0, NULL, NULL}；初始值。
   * input是当前这条 tuple 的聚合值输入 例如第一行的input.aggregates_ = {1, 5000, 28}；
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate: {  // count(*) 聚合,每行数据加1
          result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          break;
        }
        case AggregationType::CountAggregate: {  // count(列名) 聚合   只在 input中当前列不为 NULL 时才加 1
          if (!input.aggregates_[i].IsNull()) {
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = ValueFactory::GetIntegerValue(1);  // 如果之前是 NULL，则初始化为 1
            } else {
              result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));  // 否则加 1
            }
          }
          break;
        }
        case AggregationType::SumAggregate: {  // sum(列名) 聚合   只在 input中当前列不为 NULL 时才在 result 中加上
                                               // input 的值
          if (!input.aggregates_[i].IsNull()) {
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = input.aggregates_[i];  // 如果之前是 NULL，则直接赋值
            } else {
              result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);  // 否则加上 input 的值
            }
          }
          break;
        }
        case AggregationType::MinAggregate: {  // min(列名) 聚合   只在 input 中当前列不为 NULL 时才更新 result
                                               // 中的最小值
          if (!input.aggregates_[i].IsNull()) {
            if (result->aggregates_[i].IsNull() ||
                input.aggregates_[i].CompareLessThan(result->aggregates_[i]) == CmpBool::CmpTrue) {
              result->aggregates_[i] = input.aggregates_[i];  // 更新为更小的值
            }
          }
          break;
        }
        case AggregationType::MaxAggregate: {  // max(列名) 聚合   只在 input 中当前列不为 NULL 时才更新 result
                                               // 中的最大值
          if (!input.aggregates_[i].IsNull()) {
            if (result->aggregates_[i].IsNull() ||
                input.aggregates_[i].CompareGreaterThan(result->aggregates_[i]) == CmpBool::CmpTrue) {
              result->aggregates_[i] = input.aggregates_[i];  // 更新为更大的值
            }
          }
          break;
        }
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  //若哈希表中还没有该 group key，先插入初始值；然后调用 CombineAggregateValues 合并新值。
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * 添加函数，在无数据，无 GROUP BY 情况下，用来初始化输出行
   */
  void InsertInitialAggregateValue() { ht_.insert({AggregateKey{}, GenerateInitialAggregateValue()}); }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};  // 哈希表，存储分组键到聚合值的映射
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_exprs_;  // 聚合表达式列表
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;  // 聚合类型列表  如 SUM, COUNT, MIN, MAX
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
//聚合执行器，用于从 child executor 中读取元组并执行如 COUNT、SUM、MIN、MAX 等聚合操作
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;

  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Simple aggregation hash table */
  SimpleAggregationHashTable aht_;

  /** Simple aggregation hash table iterator */
  SimpleAggregationHashTable::Iterator aht_iterator_;
};
}  // namespace bustub
