/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-25 14:34:31
 * @FilePath: /CMU-15-445/src/execution/aggregation_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

/**
 * 情况一：有数据，有 GROUP BY      SELECT dept_id, COUNT(*) FROM employee GROUP BY dept_id;
 * * 子执行器不为空，正常情况
 * 情况二：有数据，无 GROUP BY     SELECT COUNT(*), SUM(salary) FROM employee;
 * * 子执行器不为空，聚合函数是 COUNT(*)，此时看成全局聚合，就是全部数据看成一组
 * 情况三：无数据，无 GROUP BY     SELECT COUNT(*) FROM empty_table;
 * * 子执行器不返回任何tuple导致 aht_ 为空，且plan_->GetGroupBys().empty()，直接插入插入空键，对应初始聚合值(COUNT =
 * 0，SUM/MIN/MAX = NULL) 情况四：无数据，有 GROUP BY     SELECT dept_id, COUNT(*) FROM empty_table GROUP BY dept_id;
 * * 子执行器不返回任何tuple导致 aht_ 为空，且plan_->GetGroupBys() 非空，不输出任何结果
 *
 * 为什么无数据，无 GROUP BY输出1行，无数据，有 GROUP BY反而输出0行
 * * 因为无数据，无 GROUP BY 时，聚合函数 默认为COUNT(*)，有聚合函数，输出1行结果，COUNT(*) = 0
 * * 无数据，有 GROUP BY 时，由于没有数据导致聚合函数无法计算，输出0行结果
 */
void AggregationExecutor::Init() {
  aht_.Clear();  // 清空哈希表

  //如果有子执行器  例如 SELECT COUNT(*), SUM(salary) FROM employee GROUP BY dept_id;  有表有GROUP BY/即使没有GROUP
  // BY也有子执行器，因为此时看成全局聚合，就是全部数据看成一组
  if (child_executor_) {
    child_executor_->Init();  // 初始化子执行器
    Tuple tuple;
    RID rid;
    // 从子执行器中获取元组
    while (child_executor_->Next(&tuple, &rid)) {
      //生成聚合键(group by 的值)
      AggregateKey agg_key = MakeAggregateKey(&tuple);
      //生成聚合值(聚合函数的值)
      AggregateValue agg_value = MakeAggregateValue(&tuple);
      //将聚合键值对插入哈希表
      aht_.InsertCombine(agg_key, agg_value);
    }
  }  //执行完成之后，所有的元组都已经被处理完毕，哈希表中存储了每个分组的聚合结果

  // ，aht_ 为空，且 plan_->GetGroupBys().empty()，插入空键，对应初始聚合值(COUNT = 0，SUM/MIN/MAX = NULL)
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertInitialAggregateValue();  // 插入空键和初始聚合值
  }

  aht_iterator_ = aht_.Begin();  // 重置迭代器到哈希表的开始位置
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;  // 如果迭代器到达末尾，表示没有更多的聚合结果
  }

  std::vector<Value> values;                          // 存储输出的值
  const AggregateKey &agg_key = aht_iterator_.Key();  // 获取当前聚合键
  for (const auto &group_by : agg_key.group_bys_) {
    values.push_back(group_by);  // 将分组键的值添加到输出值中
  }

  const AggregateValue &agg_value = aht_iterator_.Val();  // 获取当前聚合值
  for (const auto &agg : agg_value.aggregates_) {
    values.push_back(agg);  // 将聚合值添加到输出值中
  }

  *tuple = Tuple(values, &GetOutputSchema());  // 创建输出元组
  *rid = tuple->GetRid();                      // 设置输出元组的RID

  ++aht_iterator_;  // 移动迭代器到下一个位置
  return true;      // 返回true表示成功获取到一个聚合结果
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
