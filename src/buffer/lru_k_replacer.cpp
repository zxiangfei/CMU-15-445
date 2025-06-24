/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-24 14:33:14
 * @FilePath: /CMU-15-445/src/buffer/lru_k_replacer.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  //——————————————————————————————————————start——————————————————————————————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  //上锁，保证线程安全

  if (curr_size_ == 0) {
    return false;  //没有可驱逐的frame
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
  *frame_id = lru_frame->first;  //将找到的frame id赋值给输出参数
  lock.unlock();                 //解锁，允许其他线程访问
  Remove(lru_frame->first);      //删除该frame及其访问历史记录
  lock.lock();                   //重新上锁，保证线程安全
  return true;                   //成功驱逐frame
  //——————————————————————————————————————end——————————————————————————————————————————————————————
}

/**
 * 记录当前时间戳访问给定frame id
 * 如果是新的frame id，在node_store_中新建一个条目
 * 如果frame id无效(即大于replacer_size_)，使用BUSTUB_ASSERT中止进程
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  //——————————————————————————————————————start——————————————————————————————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  //上锁，保证线程安全

  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id in RecordAccess");

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = std::make_shared<LRUKNode>(k_, frame_id);
  }
  node_store_[frame_id]->RecordAccess(current_timestamp_);
  current_timestamp_++;
  //——————————————————————————————————————end——————————————————————————————————————————————————————
}

/**
 * 设置frame是否是可驱逐的，此函数也控制curr_size_的大小
 * 如果一个frame是原本是可驱逐的，并且要设置为不可驱逐，那么curr_size_应该减少
 * 如果一个frame是原本是不可驱逐的，并且要设置为可驱逐，那么curr_size_应该增加
 * 如果frame id无效，则使用BUSTUB_ASSERT中止进程
 * 对于其他情况，此函数应终止而不进行任何修改
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  //——————————————————————————————————————start——————————————————————————————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  //上锁，保证线程安全

  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id in SetEvictable");

  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "SetEvictable cannot find frame_id");

  bool was_evictable = node_store_[frame_id]->GetEvictable();
  node_store_[frame_id]->SetEvictable(set_evictable);

  if (!was_evictable && set_evictable) {
    curr_size_++;
  } else if (was_evictable && !set_evictable) {
    curr_size_--;
  }
  //——————————————————————————————————————end——————————————————————————————————————————————————————
}

/**
 * 如果frame_id为可删除的frame，删除此frame及其访问历史记录
 * 如果删除成功，还应该减少curr_size_的大小
 * 与Evict函数不同的是，此函数删除固定frame_id的frame，Evict需要先查找具有最大向后K距离的frame
 * 如果在is_evictable_ = false的frame上调用Remove，则抛出异常或者终止程序
 * 如果找不到指定的帧，则直接从该函数返回
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  //——————————————————————————————————————start——————————————————————————————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  //上锁，保证线程安全

  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id in Remove");

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // 没有这个 frame，直接返回
    return;
  }
  // 如果它当前被标记为可驱逐，我们要把 curr_size_ 减一
  if (it->second->GetEvictable()) {
    --curr_size_;
  }
  node_store_.erase(it);
  //——————————————————————————————————————end——————————————————————————————————————————————————————
}

/**
 * 返回可删除frame的size
 */
auto LRUKReplacer::Size() -> size_t {
  //——————————————————————————————————————start——————————————————————————————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  //上锁，保证线程安全

  return curr_size_;
  //——————————————————————————————————————end——————————————————————————————————————————————————————
}
}  // namespace bustub
