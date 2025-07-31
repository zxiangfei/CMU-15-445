/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-28 14:54:29
 * @FilePath: /CMU-15-445/src/execution/index_scan_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->current_index_ = 0;  // 初始化当前索引位置
}

void IndexScanExecutor::Init() {
  result_rids_.clear();  // 清空结果RID列表

  // 获取要扫描的表堆
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();

  auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);  // 获取索引信息

  // 获取索引的哈希表
  htable_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  /**
   * Bustub 的优化器／计划生成器会根据可用的索引来决定把哪些等值谓词“下推”到 pred_keys_（也就是索引扫描阶段用）
   * 剩下的都留到 filter_predicate_（拿到行之后再做过滤）
   * 例如：WHERE column1 = value1 AND column2 = value2
   * 如果有索引 column1 和 column2 的组合索引，那么 optimizer 会将这个谓词下推到 pred_keys_ 中
   * 如果没有索引，或者只有 column1 的索引，那么 optimizer 会将 column2 的过滤条件留在 filter_predicate_ 中
   */
  /**
   * 上述描述正确，但是fall2024的任务只需要支持在“单个唯一整数列上支持索引”，也就只只需要点查询和有序扫描就行
   */

  if (!plan_->pred_keys_.empty()) {  //需要点查询
    result_rids_.clear();
    for (const auto &pred_key : plan_->pred_keys_) {
      // 对每个下推的谓词键进行索引点查询
      Value key_value = pred_key->Evaluate(
          nullptr, GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->schema_);  // 计算谓词键的值

      // 创建索引键
      Tuple index_key({key_value}, htable_->GetKeySchema());

      //执行点查询
      std::vector<RID> rids;
      htable_->ScanKey(index_key, &rids, GetExecutorContext()->GetTransaction());

      result_rids_.insert(result_rids_.end(), rids.begin(), rids.end());  // 将查询结果的RID添加到结果列表中
    }
    current_index_ = 0;                              // 重置当前索引位置
  } else if (plan_->filter_predicate_ != nullptr) {  // 没有索引但有过滤条件
    auto value = plan_->filter_predicate_->Evaluate(
        nullptr, GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->schema_);  // 计算过滤条件的值

    Tuple index_key({value}, htable_->GetKeySchema());  // 创建索引键

    // 执行点查询
    std::vector<RID> rids;
    htable_->ScanKey(index_key, &rids, GetExecutorContext()->GetTransaction());  // 扫描索引

    result_rids_.insert(result_rids_.end(), rids.begin(), rids.end());  // 将查询结果的RID添加到结果列表中
    current_index_ = 0;                                                 // 重置当前索引位置
  } else {                                                              // 进行全表扫描
                                                                        // 获取索引的迭代器
    iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(
        htable_->GetBeginIterator());  // 获取索引的开始迭代器
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!plan_->pred_keys_.empty() || plan_->filter_predicate_ != nullptr) {  // 如果有下推的谓词键
    while (current_index_ < result_rids_.size()) {                          // 遍历所有的RID
      *rid = result_rids_[current_index_++];                                // 获取当前RID

      // 从表中获取元素
      auto [meta, table_tuple] = table_heap_->GetTuple(*rid);

      if (!meta.is_deleted_) {  // 如果当前tuple没有被删除
        *tuple = table_tuple;   // 返回当前tuple
        return true;            // 成功获取到一个tuple
      }
    }
    return false;  // 没有更多的tuple可供返回
  }
  // 全表扫描
  if (iterator_ == nullptr || iterator_->IsEnd()) {
    return false;  // 如果迭代器为空或已到达末尾，返回false
  }

  while (!iterator_->IsEnd()) {       // 遍历索引迭代器
    auto [key, value] = **iterator_;  // 获取当前索引键值对

    *rid = value;  // 设置当前RID

    ++(*iterator_);  // 移动到下一个索引位置

    // 从表中获取元素
    auto [meta, table_tuple] = table_heap_->GetTuple(*rid);

    if (!meta.is_deleted_) {  // 如果当前tuple没有被删除
      *tuple = table_tuple;   // 返回当前tuple
      return true;            // 成功获取到一个tuple
    }
  }
  return false;  // 没有更多的tuple可供返回
}

}  // namespace bustub
