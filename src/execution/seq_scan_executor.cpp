/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-04 23:14:35
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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  // std::pair<TupleMeta, Tuple> current_tuple;
  // do {
  //   if (current_rid_ == rids_.end()) {
  //     return false;
  //   }
  //   current_tuple = table_heap_->GetTuple(*current_rid_);  // 获取当前RID对应的tuple
  //   if (!current_tuple.first.is_deleted_) {
  //     *tuple = current_tuple.second;  // 如果当前tuple没有被删除，则返回该tuple
  //     *rid = *current_rid_;           // 返回当前RID
  //   }
  //   ++current_rid_;  // 移动到下一个RID
  // } while (current_tuple.first.is_deleted_ ||
  //          (plan_->filter_predicate_ &&
  //           !plan_->filter_predicate_
  //                ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
  //                .GetAs<bool>()));
  // // 如果当前tuple被删除，或者当前tuple不满足过滤条件，则继续获取下一个tuple

  // return true;  // 返回true表示成功获取到一个tuple

  // 下面是MVCC版本的SeqScanExecutor     
  // 每次循环尝试返回下一条对本事务可见的tuple
  while (current_rid_ != rids_.end()) {
    // 获取当前RID
    RID current_rid = *current_rid_++;

    // 从table heap中获取当前RID对应的tuple的最新版本及 undo 链头
    TupleMeta base_meta;
    Tuple base_tuple;
    std::optional<UndoLink> undo_link;
    std::tie(base_meta, base_tuple, undo_link) = 
                        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_heap_, current_rid);
    
    // 通过CollectUndoLogs函数获取需要回退的版本链
    Transaction *txn = exec_ctx_->GetTransaction();  // 获取当前事务
    auto txn_mgr = exec_ctx_->GetTransactionManager();  // 获取事务管理器
    auto undo_logs = CollectUndoLogs(current_rid, base_meta, base_tuple, undo_link, txn, txn_mgr);

    // 重建或直接使用base_tuple
    std::optional<Tuple> out_tuple;
    if(!undo_logs.has_value()) {
      // 如果没有需要回退的版本链
      
      if ((base_meta.ts_ & TXN_START_ID) != 0) {
        auto ts = txn_mgr->txn_map_[base_meta.ts_]->GetReadTs();
        if(ts > txn->GetReadTs()) {
          continue;  // 如果基础元组的时间戳大于读时间戳，跳过该tuple
        }
      }
      if(base_meta.ts_ > txn->GetReadTs()) {
        continue;  // 如果基础元组的时间戳大于读时间戳，跳过该tuple
      }
      if(!base_meta.is_deleted_) {
        // 如果基础元组没有被删除，直接使用基础元组
        out_tuple = base_tuple;
      }
    } else {
      // 如果有需要回退的版本链，调用 ReconstructTuple 函数重建元组
      out_tuple = ReconstructTuple(&plan_->OutputSchema(), base_tuple, base_meta, undo_logs.value());
    }

    // 如果重建后的元组有效，说明已经获取到该回退的版本，并且没有被删除
    if(out_tuple.has_value()) {
      // 判断是否满足过滤条件
      if (plan_->filter_predicate_){
        auto val = plan_->filter_predicate_->Evaluate(&out_tuple.value(), 
                            GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_).GetAs<bool>();
        if (!val) {
          continue;  // 如果不满足过滤条件，继续获取下一个tuple
        }
      }
      // 如果满足过滤条件，返回该元组
      *tuple = out_tuple.value();  // 设置输出的tuple
      *rid = current_rid;  // 设置输出的RID
      return true;  // 返回true表示成功获取到一个tuple
    }
  }
  // 如果没有更多的tuple可供返回，返回false
  return false;
}

}  // namespace bustub
