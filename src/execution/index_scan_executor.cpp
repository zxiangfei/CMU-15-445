/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 16:52:35
 * @FilePath: /CMU-15-445/src/execution/index_scan_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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

/**
 * 构造函数
 * @param exec_ctx 执行上下文
 * @param plan 索引扫描计划节点  包括要使用的索引元信息、要匹配的键和值、以及可能的附加过滤谓词
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_idx_(0) {}

/**
 * 初始化索引扫描执行器
 * 
 * 在第一次调用 Next() 前，用于：
 * * 获取与计划节点中指定的表ID对应的表信息
 * * 获取与计划节点中指定的索引ID对应的索引信息，并将其转化为哈希索引
 * * 使用索引的键模式创建键，并执行索引扫描，将符合的结果放入result_rids_列表中
 */
void IndexScanExecutor::Init() {
    // 获取与计划节点中指定的表ID对应的表信息
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    table_heap_ = table_info->table_.get();

    // 获取与计划节点中指定的索引ID对应的索引信息，并将其转化为哈希索引
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
    htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

    auto key_schema = index_info->key_schema_;//获取索引定义时的键模式，指导如何解析索引条目的字段
    auto value = plan_->pred_key_->val_; // 获取索引扫描计划节点中指定的键值,例如where v = 1 中的 1存储在这里

    //将键值分装成Tuple对象，供后续扫描使用
    Tuple key_tuple({value}, &key_schema);
    result_rids_.clear(); // 清空结果列表
    // 执行索引扫描，将符合的结果放入result_rids_列表中
    htable->ScanKey(key_tuple, &result_rids_,exec_ctx_->GetTransaction());
    cur_idx_ = 0;
}

/**
 * 
 */
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (cur_idx_ < result_rids_.size()) {
        // 获取当前索引位置的 RID
        *rid = result_rids_[cur_idx_];
        cur_idx_++;

        auto [meta, data] = table_heap_->GetTuple(*rid);
        if(!meta.is_deleted_) {
            // 如果元数据未标记为删除，则将数据填充到输出 Tuple 中
            *tuple = data;
            return true;
        }
    }
    return false; // 如果没有更多的结果，返回 false
}

}  // namespace bustub
