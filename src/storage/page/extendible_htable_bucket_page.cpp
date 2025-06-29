//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

/**
 * 从缓冲池创建新的bucket页面后，必须调用Init方法来设置默认值
 * 设置 max_size_ 并将 size_ 置 0
 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  assert(max_size > 0 && max_size <= HTableBucketArraySize(sizeof(std::pair<K, V>)));  // 确保 max_size 在合理范围内
  max_size_ = max_size;
  size_ = 0;
}

/**
 * 查找键值对,找到的话将值存入value中
 * 遍历当前已有的 size_ 条映射，匹配成功则输出 value 并返回 true
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

/**
 * 尝试在bucket中插入键和值
 * 若 size_ == max_size_ 或已存在相同 key，则失败；否则追加到末尾，size_++
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (IsFull()) {
    return false;  // 如果已满，插入失败
  }
  V tmp;  // 用于存储查找结果的临时变量
  if (Lookup(key, tmp, cmp)) {
    return false;  // 如果已存在相同 key，插入失败
  }
  array_[size_] = {key, value};
  size_++;
  return true;
}

/**
 * 删除键值对
 * Remove 通过 key 找到索引后调用 RemoveAt，后者将后续元素前移并 size_--
 * 如果未找到 key，则返回 false
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      RemoveAt(i);  // 找到后删除
      return true;
    }
  }
  return false;  // 未找到 key
}
/**
 * 删除指定索引处的键值对
 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  assert(bucket_idx < size_);  // 确保索引在范围内
  for (uint32_t i = bucket_idx; i < size_ - 1; i++) {
    array_[i] = array_[i + 1];  // 将后续元素前移
  }
  size_--;  // 减少有效映射数
}

/**
 * 获取指定索引处的键
 * 做边界 assert(bucket_idx < size_)
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  assert(bucket_idx < size_);       // 确保索引在范围内
  return array_[bucket_idx].first;  // 返回指定索引处的键
}

/**
 * 获取指定索引处的值
 * 做边界 assert(bucket_idx < size_)
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  assert(bucket_idx < size_);        // 确保索引在范围内
  return array_[bucket_idx].second;  // 返回指定索引处的值
}

/**
 * 获取指定索引处的键值对
 * 做边界 assert(bucket_idx < size_)
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  assert(bucket_idx < size_);  // 确保索引在范围内
  return array_[bucket_idx];   // 返回指定索引处的键值对
}

/**
 * bucket 中的映射数
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;  // 返回当前有效映射数
}

/**
 * bucket 是否已满
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ >= max_size_;  // 如果当前映射数已达最大值，则返回 true
}

/**
 * bucket 是否为空
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;  // 如果当前映射数为 0，则返回 true
}

/**
 * 打印 bucket 的占用信息
 * 目前未实现
 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::PrintBucket() const {}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
