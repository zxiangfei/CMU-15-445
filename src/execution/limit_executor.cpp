/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-30 16:31:34
 * @FilePath: /CMU-15-445/src/execution/limit_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  out_count_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_count_ >= plan_->GetLimit()) {
    return false;  // 已经输出了足够的元组
  }

  if (child_executor_->Next(tuple, rid)) {
    out_count_++;  // 成功输出一个元组，增加计数
    return true;   // 返回true表示成功获取到一个元组
  }
  return false;  // 子执行器没有更多元组可供输出
}

}  // namespace bustub
