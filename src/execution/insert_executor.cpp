/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 11:03:39
 * @FilePath: /CMU-15-445/src/execution/insert_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 10:29:07
 * @FilePath: /CMU-15-445/src/execution/insert_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * 构造函数
 * @param exec_ctx 执行上下文 (包含事务、目录等)
 * @param plan 需要执行的插入计划
 * @param child_executor 子执行器，用于从中拉取要插入的元组，此参数可为空
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), inserted_(false),child_executor_(std::move(child_executor)) {   
}

/**
 * 初始化插入算子
 * 在调用 Next() 之前，驱动器负责先调用 Init()，做一些准备工作
 * 初始化子算子(若存在)
 * 从目录加载表元信息和 TableHeap
 * 准备其他运行时状态（例如索引列表、计数器等）
 */
void InsertExecutor::Init() {
    child_executor_->Init();  //如果有子执行器，先初始化它
}

/**
 * 执行插入操作
 * @param tuple 输出参数，存储插入的元组（通常是一个整数，表示插入的行数）
 * @param rid 输出参数，存储下一个元组的 RID（忽略，不使用）
 * 
 * 对每一行要插入的数据（来自 plan_->RawValues() 或 child_executor->Next()）调用 TableHeap::InsertTuple
 * 对该表的所有索引逐个插入索引条目
 * 统计插入行数
 * 
 * 仅在第一次调用时，构造并通过 tuple 返回一个单列整型行，表示插入的总行数，然后返回 true；后续调用直接返回 false
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if(inserted_) {
        return false;  //如果已经插入过数据，直接返回 false
    }

    inserted_ = true;  //标记为已插入
    int count = 0;  //插入计数器

    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  //获取表元信息
    auto schema = table_info->schema_;  //获取表列的描述
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  //获取表的所有相关索引

    //从在执行器child_executor_中逐行拉取要插入的元组，同时更新所有索引
    while(child_executor_->Next(tuple,rid)){
        std::optional<RID> insert_rid = table_info->table_->InsertTuple(TupleMeta{0,false},*tuple);  //插入元组到表中
        count++;  //更新插入计数器

        RID rid = insert_rid.value();  //获取插入的 RID
        //对每个索引，插入索引条目
        for (const auto &index : indexes) {
            //生成索引条目
            auto index_key = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
            //插入索引条目
            index->index_->InsertEntry(index_key, rid, exec_ctx_->GetTransaction());
        }
    }
    //构造一个单列整型行，表示插入的总行数
    Tuple count_tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
    *tuple = std::move(count_tuple);  //将计数行赋值给输出参数 tuple

    return true;  //返回 true，表示成功插入了数据
    
}

}  // namespace bustub
