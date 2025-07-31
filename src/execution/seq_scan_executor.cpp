/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-20 22:49:30
 * @FilePath: /CMU-15-445/src/execution/seq_scan_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
}

void SeqScanExecutor::Init() {
  this->table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();  // 获取要扫描的table heap

  auto it = this->table_heap_->MakeIterator();  // 获取table heap的迭代器,迭代器指向的是table
                                                // heap的第一个tuple，自增是tuple的自增不是page的自增
  this->rids_.clear();

  while (!it.IsEnd()) {
    auto rid = it.GetRID();
    this->rids_.push_back(rid);
    ++it;
  }
  this->current_rid_ = this->rids_.begin();  // 初始化当前扫描到的RID迭代器为rids_的开始位置
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> current_tuple;
  do {
    if (current_rid_ == rids_.end()) {
      return false;
    }
    current_tuple = table_heap_->GetTuple(*current_rid_);  // 获取当前RID对应的tuple
    if (!current_tuple.first.is_deleted_) {
      *tuple = current_tuple.second;  // 如果当前tuple没有被删除，则返回该tuple
      *rid = *current_rid_;           // 返回当前RID
    }
    ++current_rid_;  // 移动到下一个RID
  } while (current_tuple.first.is_deleted_ ||
           (plan_->filter_predicate_ &&
            !plan_->filter_predicate_
                 ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
                 .GetAs<bool>()));
  // 如果当前tuple被删除，或者当前tuple不满足过滤条件，则继续获取下一个tuple

  return true;  // 返回true表示成功获取到一个tuple
}

}  // namespace bustub
