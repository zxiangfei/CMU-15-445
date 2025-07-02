/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 20:12:04
 * @FilePath: /CMU-15-445/src/execution/seq_scan_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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

/**
 * 构造函数
 * @param exec_ctx 执行上下文  (包含事务、目录等)
 * @param plan 需要执行的顺序扫描计划   (表 OID、谓词、投影信息)
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),
                                                                                           table_heap_(nullptr) {
    this->plan_ = plan;  // 保存传入的计划节点指针，以便在 Init/Next 中读取要扫描的表 OID、谓词、投影列等信息
}

/**
 * 初始化顺序扫描
 * 在调用 Next() 之前，驱动器负责先调用 Init()，做一些准备工作（如定位表、打开迭代器、下推谓词等）
 */
void SeqScanExecutor::Init() {
    table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();  // 获取表信息
    auto iter = table_heap_->MakeIterator();  // 获取表的迭代器

    rids.clear();  // 清空之前的 RID 列表
    while(!iter.IsEnd()){
        rids.emplace_back(iter.GetRID());  // 将每个元组的 RID 添加到 rids 向量中
        ++iter;  // 移动到下一个元组
    }

    rid_iter_ = rids.begin();  // 初始化迭代器，指向第一个元组的 RID
}

/**
 * 从顺序扫描中获取下一个元组
 * @param tuple 输出参数，存储下一个元组
 * @param rid 输出参数，存储下一个元组的 RID
 * @return 如果有下一个元组，返回 true；如果没有更多元组，返回 false
 * 
 * 
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    TupleMeta meta;  // 用于存储元组的元数据
    do{
        if(rid_iter_ == rids.end()) {
            return false;  // 如果迭代器到达末尾，表示没有更多元组
        }
        meta = table_heap_->GetTuple(*rid_iter_).first;  // 获取当前 RID 对应的元组元数据
        if(!meta.is_deleted_) {
            *tuple = table_heap_->GetTuple(*rid_iter_).second;  // 获取当前 RID 对应的元组
            *rid = *rid_iter_;  // 设置输出参数 rid
        }
        ++rid_iter_;  // 如果当前元组被删除，跳过它，继续查找下一个未删除的元组
    } while (meta.is_deleted_ || 
             (plan_->filter_predicate_ != nullptr &&
             !plan_->filter_predicate_->Evaluate(tuple,GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
             .GetAs<bool>()));  // 如果当前元组被删除，或者不满足谓词条件，则继续查找下一个元组
    return true;  // 返回 true，表示成功获取到一个有效的元组
}

}  // namespace bustub
