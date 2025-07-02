/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 15:43:59
 * @FilePath: /CMU-15-445/src/execution/delete_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * 构造函数
 * @param exec_ctx 执行上下文
 * @param plan 删除计划节点
 * @param child_executor 子执行器
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), deleted_(false) {
}

/**
 * 初始化删除算子
 * 
 * 在第一次执行 Next() 前调用，用来：
 * * 初始化子执行器；
 * * 从目录中获取表元信息（TableInfo），包括 TableHeap 和索引列表；
 * * 重置计数或状态标志
 */
void DeleteExecutor::Init() {
    child_executor_->Init();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

/**
 * 执行删除操作
 * 
 * 循环调用子执行器 child_executor_->Next(&tuple, &rid)，拿到每个要删的行标识 rid 及对应 tuple（可用于索引键提取）
 * 调用 table_heap_->MarkDelete(rid, txn) 标记删除
 * 对该行涉及的每个索引，计算索引键并调用 index_->DeleteEntry(key, rid, txn) 删除索引条目
 * 累加删除计数
 * 
 * 仅在第一次调用时完成所有删除并返回一个单列整型 tuple（删除总行数），随后返回 false
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if(deleted_) {
        return false;  // 已经删除过了，直接返回 false
    }

    deleted_ = true;  // 标记为已删除
    int count = 0;  // 记录删除的行数

    auto schema = table_info_->schema_;  //获取表列的描述
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);  //获取表的索引列表

    while(child_executor_->Next(tuple, rid)) {
        // 删除逻辑是标记删除行并更新索引
        table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);  // 标记旧行为删除

        count++;  // 增加删除计数

        // 更新索引
        for (const auto &index : indexes) {
            // 计算索引键
            auto key = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
            // 删除索引条目
            index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
        }
    }
    
    // 构造单列整型输出，表示删除的行数
    Tuple count_tuple({Value(TypeId::INTEGER, count)}, &schema);
    *tuple = count_tuple;  // 将结果写入输出 tuple

    return true;// 返回 true，表示有结果输出
}

}  // namespace bustub
