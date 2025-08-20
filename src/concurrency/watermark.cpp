/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-01 15:22:30
 * @FilePath: /CMU-15-445/src/concurrency/watermark.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if(current_reads_.count(read_ts) != 0) {
    current_reads_[read_ts]++;  // 如果该 read_ts 已存在，则计数加一
  } else {
    current_reads_[read_ts] = 1;  // 否则初始化计数为 1
    read_queue_.emplace(read_ts);  // 将新的 read_ts 加入优先队列
  }
  if (read_ts < watermark_) {
    watermark_ = read_ts;  // 更新 watermark 为当前 read_ts
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_.count(read_ts) == 0 || current_reads_[read_ts] <= 0) {
    throw Exception("read ts not found in current reads");
  }
  current_reads_[read_ts]--;  // 减少该 read_ts 的计数
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
    while(!read_queue_.empty() && current_reads_.count(read_queue_.top()) == 0) {
      read_queue_.pop();  // 清理优先队列中已不存在的 read_ts
    }
  }

  if (read_ts == watermark_) {
    // 如果移除的 read_ts 是当前 watermark，则需要重新计算 watermark
    if (read_queue_.empty()) {
      watermark_ = commit_ts_;  // 如果没有活跃事务，则 watermark 恢复为 commit ts
      return;
    }
    watermark_ = read_queue_.top();  // 否则取优先队列中的最小值作为新的 watermark
  }
}


}  // namespace bustub
