/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-01 14:53:23
 * @FilePath: /CMU-15-445/src/include/concurrency/watermark.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once

#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"
#include <functional>
#include <queue>

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
  static constexpr auto cmp = [](auto const &a, auto const &b){ return a > b; };
 public:
  explicit Watermark(timestamp_t commit_ts)
      : commit_ts_(commit_ts),
        watermark_(commit_ts),
        read_queue_(cmp) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  timestamp_t commit_ts_;

  timestamp_t watermark_;

  std::unordered_map<timestamp_t, int> current_reads_;

  std::priority_queue<timestamp_t, std::vector<timestamp_t>, decltype(cmp)> read_queue_;

};

};  // namespace bustub
