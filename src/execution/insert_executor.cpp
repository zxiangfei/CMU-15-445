/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-05 14:05:33
 * @FilePath: /CMU-15-445/src/execution/insert_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->inserted_ = false;                            // 初始化为未插入状态
  this->child_executor_ = std::move(child_executor);  // 移动子执行器
}

void InsertExecutor::Init() {
  this->child_executor_->Init();  // 初始化子执行器
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;  // 如果已经插入过数据，则不再插入
  }

  inserted_ = true;        // 标记为已插入状态
  int inserted_count = 0;  // 记录插入的元组数量

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取要插入的表信息
  auto schema = table_info->schema_;                                          // 获取表的描述
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取表的索引

  // MVCC事务实现所需要的数据
  auto txn = exec_ctx_->GetTransaction();  // 获取当前事务
  auto temp_ts = txn->GetTransactionTempTs();  // 获取事务的临时时间戳

  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuple_meta{temp_ts, false};  // 在MVCC的情况下，tuple_meta.ts_不再为0，而是为事务的临时时间戳，is_deleted_为false
    std::optional<RID> insert_rid = table_info->table_->InsertTuple(tuple_meta, *tuple);  // 插入元组

    RID rid = insert_rid.value();  // 获取插入的RID
    txn->AppendWriteSet(table_info->oid_, rid);  // 将插入的RID添加到事务的写集
    inserted_count++;              // 记录插入的元组数量
    
    
    for (auto &index : indexs) {
      // 获取索引的键值
      auto key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      // 插入索引
      index->index_->InsertEntry(key_tuple, rid, exec_ctx_->GetTransaction());
    }
  }

  //构造一个单列整形行，表示插入的元组数量
  Tuple count_tuple({Value(TypeId::INTEGER, inserted_count)}, &GetOutputSchema());
  *tuple = std::move(count_tuple);  // 将插入的元组数量赋值给输出元组

  return true;  // 成功插入，返回true
}

}  // namespace bustub
