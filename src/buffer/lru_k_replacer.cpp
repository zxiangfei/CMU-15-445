//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * 找出最大倒数K距离的frame，并驱逐该frame
 * 只有is_evictable_为true的frame才是候选frame
 * 访问次数低于K次的frame距离被定义为+inf
 * 如果有多个+inf的frame，则基于LRU驱逐具有最早时间戳的frame
 * 成功驱逐frame之后应减小replacer_size_，以及删除该frame的访问历史
 * (同类型比较history_.front()，大的说明访问时间距离现在比较近)
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::unique_lock<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;  // 没有可驱逐的frame
  }

  auto lru_frame = node_store_.end();
  for (auto it = node_store_.begin(); it != node_store_.end(); ++it) {
    if (it->second->GetEvictable()) {
      if (lru_frame == this->node_store_.end() ||
          (it->second->GetHistorySize() < k_ && lru_frame->second->GetHistorySize() >= k_)) {
        lru_frame = it;  //找到距离最大的frame
        continue;
      }
      if (it->second->GetHistorySize() >= k_ && lru_frame->second->GetHistorySize() < k_) {
        continue;  //如果当前frame访问次数大于K，且lru_frame小于K，则不更新lru_frame
      }
      if (lru_frame->second->GetKDistance() >= it->second->GetKDistance()) {
        lru_frame = it;  //如果当前frame距离大于等于lru_frame，则更新lru_frame
      }
    }
  }

  if (lru_frame == node_store_.end()) {
    return std::nullopt;  // 没有可驱逐的frame
  }

  frame_id_t evicted_frame_id = lru_frame->first;  // 获取被驱逐的frame id
  if (lru_frame->second->GetEvictable()) {
    curr_size_--;  // 减少可驱逐frame的数量
  }
  node_store_.erase(lru_frame);  // 从node_store_中删除该frame
  return evicted_frame_id;
}

/**
 * 记录当前时间戳访问给定frame id
 * 如果是新的frame id，在node_store_中新建一个条目
 * 如果frame id无效(即大于replacer_size_)，使用BUSTUB_ASSERT中止进程
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_,
                "Invalid frame_id in RecordAccess");  // 确保frame_id有效

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // 如果frame id不存在，则创建一个新的LRUKNode
    auto new_node = std::make_shared<LRUKNode>(frame_id, k_);
    node_store_[frame_id] = new_node;
  }

  // 更新访问时间戳
  it = node_store_.find(frame_id);
  it->second->RecordAccess(current_timestamp_++);
}

/**
 * 设置frame是否是可驱逐的，此函数也控制curr_size_的大小
 * 如果一个frame是原本是可驱逐的，并且要设置为不可驱逐，那么curr_size_应该减少
 * 如果一个frame是原本是不可驱逐的，并且要设置为可驱逐，那么curr_size_应该增加
 * 如果frame id无效，则使用BUSTUB_ASSERT中止进程
 * 对于其他情况，此函数应终止而不进行任何修改
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_,
                "Invalid frame_id in SetEvictable");  // 确保frame_id有效

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;  // frame id 不存在
  }

  bool was_evictable = it->second->GetEvictable();
  it->second->SetEvictable(set_evictable);

  if (was_evictable && !set_evictable) {
    curr_size_--;  // 减少可驱逐frame的数量
  } else if (!was_evictable && set_evictable) {
    curr_size_++;  // 增加可驱逐frame的数量
  }
}

/**
 * 如果frame_id为可删除的frame，删除此frame及其访问历史记录
 * 如果删除成功，还应该减少curr_size_的大小
 * 与Evict函数不同的是，此函数删除固定frame_id的frame，Evict需要先查找具有最大向后K距离的frame
 * 如果在is_evictable_ = false的frame上调用Remove，则抛出异常或者终止程序
 * 如果找不到指定的帧，则直接从该函数返回
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id in Remove");  // 确保frame_id有效

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;  // frame id 不存在
  }

  if (it->second->GetEvictable()) {
    curr_size_--;  // 减少可驱逐frame的数量
  }
  node_store_.erase(it);  // 从node_store_中删除该frame
}

/**
 * 返回可删除frame的size
 */
auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
