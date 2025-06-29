//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * 在 BufferPoolManager 新分配到页面后必须调用，设置 max_depth_ 上限，并初始化所有本地深度和桶 ID
 * 初始化可扩展哈希目录页
 */
void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  assert(max_depth <= HTABLE_DIRECTORY_MAX_DEPTH);        //断言判断max_depth是否符合范围
  max_depth_ = max_depth;                                 //设置最大深度
  global_depth_ = 0;                                      //初始化全局深度为0
  memset(local_depths_, 0, HTABLE_DIRECTORY_ARRAY_SIZE);  //将所有本地深度初始化为0
  memset(bucket_page_ids_, INVALID_PAGE_ID,
         HTABLE_DIRECTORY_ARRAY_SIZE * sizeof(page_id_t));  //将所有桶页 ID 初始化为无效页 ID
}

/**
 * 获取键被散列到的桶索引
 * 取哈希值的低 GD 位（hash & ((1<<GD)-1)），得到索引
 */
auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (global_depth_ == 0) {
    return 0;  // 如果全局深度为0，所有键都映射到索引0
  }
  return hash & ((1U << global_depth_) - 1);  // 使用全局深度计算目录索引   hash低位表示索引
}

/**
 * 使用目录索引查找桶页
 * 在当前 Size()（2^GD）范围内返回存储的 page_id
 */
auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  assert(bucket_idx < Size());          // 确保索引在有效范围内
  return bucket_page_ids_[bucket_idx];  // 返回对应索引的桶页 ID
}

/**
 * 使用桶索引和page_id更新目录索引
 * 将对应槽位更新为新的桶页 ID，通常在分裂或合并时调用
 */
void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx < Size());                    // 确保索引在有效范围内
  bucket_page_ids_[bucket_idx] = bucket_page_id;  // 更新指定槽的桶页 ID
}

/**
 * 计算分裂伙伴
 * 对一个槽 i，其分裂伙伴索引是 i ^ (1 << (LD-1))，其中 LD 是该槽本地深度
 */
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto ld = GetLocalDepth(bucket_idx);   // 获取指定槽的本地深度
  assert(ld > 0);                        // 确保本地深度大于0
  return bucket_idx ^ (1U << (ld - 1));  // 计算分裂伙伴索引
}

/**
 * 返回global_depth为1和其余0的掩码
 * 在可扩展哈希中，我们使用以下哈希+掩码函数将键映射到目录索引：
 * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
 * 其中 GLOBAL_DEPTH_MASK 是从LSB向上具有 GLOBAL_DEPTH 个1的掩码。
 * 例如，global depth 3 对应于 0x00000007（32位表示）。
 * @return global_depth 1的掩码和其余0（从LSB向上）的掩码
 */
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (1U << global_depth_) - 1;  // 返回全局深度掩码，1左移global_depth-1位
}

/**
 * 与全局深度掩码相同，除了它使用位于bucket_idx的bucket的局部深度
 * 对指定槽 i，返回 (1<<LD[i]) - 1，用于本地分裂操作时掩取位
 */
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < Size());                       // 确保索引在有效范围内
  uint32_t local_depth = GetLocalDepth(bucket_idx);  // 获取指定槽的本地深度
  return (1U << local_depth) - 1;                    // 返回本地深度掩码，1左移local_depth-1位
}

/**
 * 获取哈希表目录的全局深度
 * 返回当前 global_depth_
 */
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;  // 返回当前全局深度
}

/**
 * 返回当前最大深度上限 max_depth_
 */
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  return max_depth_;  // 返回当前最大深度
}

/**
 * 增加global_depth_
 * 需确保不越过上限
 */
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ < max_depth_);
  // 1) 记录旧目录长度
  uint32_t old_size = Size();
  // 2) 增加深度
  global_depth_++;
  // 3) 新目录长度 = 2^global_depth_ = old_size * 2
  //    把 [0, old_size) 的条目复制到 [old_size, 2*old_size)
  for (uint32_t i = 0; i < old_size; i++) {
    bucket_page_ids_[i + old_size] = bucket_page_ids_[i];
    local_depths_[i + old_size] = local_depths_[i];
  }
}

/**
 * 减少global_depth_
 * 需确保不低于0
 */
void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  assert(global_depth_ > 0);  // 确保全局深度大于0
  global_depth_--;            // 减少全局深度
}

/**
 * 可收缩检查：当没有任何槽的本地深度等于当前 GD 时，可将 GD--
 * 如果目录可以收缩，则为True
 */
auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;  // 如果全局深度为0，则不能收缩
  }

  // 检查是否有任何槽的本地深度等于全局深度
  // 如果有，则不能收缩；否则可以收缩
  // 遍历所有槽，检查其本地深度是否等于全局深度
  for (uint32_t i = 0; i < Size(); ++i) {
    if (GetLocalDepth(i) == global_depth_) {
      return false;  // 如果有任何槽的本地深度等于全局深度，则不能收缩
    }
  }
  return true;  // 如果没有槽的本地深度等于全局深度，则可以收缩
}

/**
 * 当前目录大小
 * Size() = 2^GD 为当前可用槽数
 */
auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return 1U << global_depth_;  // 返回当前全局深度对应的槽数，即 2^GD
}

/**
 * 返回目录的最大大小
 * MaxSize() = 2^max_depth_ 为最大可用槽数
 */
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  return 1U << max_depth_;  // 返回最大深度对应的槽数，即 2^max_depth_
}

/**
 * 获取bucket在bucket_idx处的局部深度
 */
auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < Size());       // 确保索引在有效范围内
  return local_depths_[bucket_idx];  // 返回指定槽的本地深度
}

/**
 * 设置bucket在bucket_idx处的局部深度为local_depth
 * 更新指定槽的局部深度，通常在分裂或合并时调用
 */
void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(bucket_idx < Size());              // 确保索引在有效范围内
  assert(local_depth <= max_depth_);        // 确保本地深度不超过最大深度
  local_depths_[bucket_idx] = local_depth;  // 更新指定槽的本地深度
}

/**
 * 增加bucket在bucket_idx处的局部深度
 * LD[i]++
 */
void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx < Size());                        // 确保索引在有效范围内
  assert(local_depths_[bucket_idx] < global_depth_);  // 确保本地深度小于全局深度
  local_depths_[bucket_idx]++;                        // 增加指定槽的本地深度
}

/**
 * 递减bucket在bucket_idx处的局部深度
 * LD[i]--
 */
void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx < Size());            // 确保索引在有效范围内
  assert(local_depths_[bucket_idx] > 0);  // 确保本地深度大于0
  local_depths_[bucket_idx]--;            // 减少指定槽的本地深度
}

/**
 * 完整性校验：
 * 所有 LD[i] <= GD。
 * 每个桶有且正好 2^(GD-LD) 个槽指向它。
 * 指向同一桶的所有槽具有相同 LD。
 */
void ExtendibleHTableDirectoryPage::VerifyIntegrity() const {
  // 全局不变式：当前全局深度不能超过配置的最大深度
  assert(global_depth_ <= max_depth_);
  // 当前目录长度 = 2^global_depth_
  const uint32_t n = Size();

  // (1) 每个槽的本地深度都不能超过全局深度
  for (uint32_t i = 0; i < n; i++) {
    assert(local_depths_[i] <= global_depth_);
  }

  // (2)&(3) 同时统计每个桶出现的次数，并检查同一个桶的 LD 一致
  std::unordered_map<page_id_t, uint32_t> bucket_count;
  std::unordered_map<page_id_t, uint8_t> bucket_ld;
  for (uint32_t i = 0; i < n; i++) {
    page_id_t bpid = bucket_page_ids_[i];
    if (bpid == INVALID_PAGE_ID) {
      continue;
    }
    // 累加该桶在目录中出现的次数
    bucket_count[bpid]++;
    // 第一次见到就记录本地深度，后面再见必须相同
    auto it = bucket_ld.find(bpid);
    if (it == bucket_ld.end()) {
      bucket_ld[bpid] = local_depths_[i];
    } else {
      assert(it->second == local_depths_[i]);
    }
  }

  // (2) 对每个桶，检查出现次数 == 2^(GD - LD)
  for (auto &kv : bucket_count) {
    page_id_t bpid = kv.first;
    uint32_t cnt = kv.second;      // 目录中指向该桶的槽数
    uint8_t ld = bucket_ld[bpid];  // 该桶的本地深度
    uint32_t exp = 1U << (global_depth_ - ld);
    assert(cnt == exp);
  }
}

/**
 * 调试打印：
 * 输出当前 GD/max_depth_/Size()，以及 local_depths_ 和 bucket_page_ids_ 列表
 */
void ExtendibleHTableDirectoryPage::PrintDirectory() const {
  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

}  // namespace bustub
