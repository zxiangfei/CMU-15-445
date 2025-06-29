//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.h
//
// Identification: src/include/container/disk/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * Implementation of extendible hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table grows/shrinks dynamically as buckets become full/empty.
 */
/**
 * 实现基于缓冲池管理器的可扩展哈希表。支持非唯一键。支持插入和删除。
 * 当桶变满/空时，表会动态增长/缩小
 */
template <typename K, typename V, typename KC>
class DiskExtendibleHashTable {
 public:
  /**
   * @brief Creates a new DiskExtendibleHashTable.
   *
   * @param name
   * @param bpm buffer pool manager to be used
   * @param cmp comparator for keys
   * @param hash_fn the hash function
   * @param header_max_depth the max depth allowed for the header page
   * @param directory_max_depth the max depth allowed for the directory page
   * @param bucket_max_size the max size allowed for the bucket page array
   */
  explicit DiskExtendibleHashTable(const std::string &name,  // NOLINT(modernize-pass-by-value)
                                   BufferPoolManager *bpm, const KC &cmp, const HashFunction<K> &hash_fn,
                                   uint32_t header_max_depth = HTABLE_HEADER_MAX_DEPTH,
                                   uint32_t directory_max_depth = HTABLE_DIRECTORY_MAX_DEPTH,
                                   uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)));

  /** TODO(P2): Add implementation
   * Inserts a key-value pair into the hash table.
   *
   * @param key the key to create
   * @param value the value to be associated with the key
   * @param transaction the current transaction
   * @return true if insert succeeded, false otherwise
   */
  auto Insert(const K &key, const V &value, Transaction *transaction = nullptr) -> bool;

  /** TODO(P2): Add implementation
   * Removes a key-value pair from the hash table.
   *
   * @param key the key to delete
   * @param value the value to delete
   * @param transaction the current transaction
   * @return true if remove succeeded, false otherwise
   */
  auto Remove(const K &key, Transaction *transaction = nullptr) -> bool;

  /** TODO(P2): Add implementation
   * Get the value associated with a given key in the hash table.
   *
   * Note(fall2023): This semester you will only need to support unique key-value pairs.
   *
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @param transaction the current transaction
   * @return the value(s) associated with the given key
   */
  auto GetValue(const K &key, std::vector<V> *result, Transaction *transaction = nullptr) const -> bool;

  /**
   * Helper function to verify the integrity of the extendible hash table's directory.
   */
  void VerifyIntegrity() const;

  /**
   * Helper function to expose the header page id.
   */
  auto GetHeaderPageId() const -> page_id_t;

  /**
   * Helper function to print out the HashTable.
   */
  void PrintHT() const;

 private:
  /**
   * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
   * for extendible hashing.
   *
   * @param key the key to hash
   * @return the down-casted 32-bit hash
   */
  auto Hash(K key) const -> uint32_t;

  auto InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx, uint32_t hash, const K &key,
                            const V &value) -> bool;

  auto InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx, const K &key, const V &value)
      -> bool;

  void UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx,
                              page_id_t new_bucket_page_id, uint32_t new_local_depth, uint32_t local_depth_mask);

  void MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                      ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                      uint32_t local_depth_mask);

  auto SplitBucket(ExtendibleHTableDirectoryPage *directory, ExtendibleHTableBucketPage<K, V, KC> *bucket,
                   uint32_t bucket_idx) -> bool;

  // member variables
  std::string
      index_name_;  //索引名称，用于标识和管理索引文件，在创建buffer_pool_manager时会传入disk_manager，在创建disk_manager时会传入index_name
  BufferPoolManager *bpm_;        //缓冲池管理器，用于管理页面缓存和磁盘I/O
  KC cmp_;                        //比较器，用于比较键值对的键
  HashFunction<K> hash_fn_;       //哈希函数，用于计算键的哈希值
  uint32_t header_max_depth_;     //最大目录页深度，用于限制目录页的深度
  uint32_t directory_max_depth_;  //最大桶页深度，用于限制桶页的深度
  uint32_t bucket_max_size_;      //桶页的最大大小，用于限制每个桶页可以存储的键值对数量
  page_id_t header_page_id_;      // header页ID，用于标识哈希表的头页
};

}  // namespace bustub
