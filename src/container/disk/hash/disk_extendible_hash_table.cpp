//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * 可扩展哈希表的构造函数
 */
template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name,  // NOLINT(modernize-pass-by-value)
                                                           BufferPoolManager *bpm, const KC &cmp,
                                                           const HashFunction<K> &hash_fn, uint32_t header_max_depth,
                                                           uint32_t directory_max_depth, uint32_t bucket_max_size)
    : index_name_(name),
      bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // 创建 header 页
  auto header_page = bpm_->NewPageGuarded(&header_page_id_);  //使用bpm_创建一个新的页面，并获取header页的ID
  assert(header_page.PageId() != INVALID_PAGE_ID);
  auto *header = header_page.AsMut<ExtendibleHTableHeaderPage>();  //将页面转换为ExtendibleHTableHeaderPage类型的指针
  header->Init(header_max_depth);                                  //初始化 header 页，设置最大深度
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * 获取哈希表中与给定key关联的value
 *
 * 在哈希表中查找 key，将结果写入 *result（此处只会有 0 或 1 个元素）
 * 返回是否找到
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  result->clear();              //清空结果向量
  const auto hash = Hash(key);  //计算 key 的哈希值

  // 获取 header 页，查找对应的目录页
  ReadPageGuard header_page = bpm_->FetchPageRead(header_page_id_);  //获取 header 页
  auto header = header_page.As<ExtendibleHTableHeaderPage>();  //将页面转换为 ExtendibleHTableHeaderPage 类型的指针
  auto directory_idx = header->HashToDirectoryIndex(hash);                  //根据哈希值获取目录索引
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);  //获取目录页 ID
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;  //如果目录页 ID 无效，表示未找到
  }
  header_page.Drop();  //释放 header 页的读锁

  // 获取目录页，查找对应的桶页
  ReadPageGuard directory_page = bpm_->FetchPageRead(directory_page_id);  //获取目录页
  auto directory =
      directory_page.As<ExtendibleHTableDirectoryPage>();  //将页面转换为 ExtendibleHTableDirectoryPage 类型的指针
  auto bucket_idx = directory->HashToBucketIndex(hash);          //根据哈希值获取桶索引
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);  //获取桶页 ID
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;  //如果桶页 ID 无效，表示未找到
  }
  directory_page.Drop();  //释放目录页的读锁

  // 获取桶页，查找键值对
  ReadPageGuard bucket_page = bpm_->FetchPageRead(bucket_page_id);  //获取桶页
  auto bucket =
      bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指针

  V value;
  if (!bucket->Lookup(key, value, cmp_)) {  //查找key对应的值
    return false;                           //如果未找到，返回 false
  }
  result->emplace_back(value);  //将找到的值添加到结果向量中
  return true;                  //返回 true，表示找到对应的键值对
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * 插入键值对到可扩展哈希表中
 * transaction为当前事务
 *
 * 插入 (key,value)
 * transaction 仅供并发控制（本轮作业可传 nullptr）
 * 返回 true/false 表示成功或失败（如重复键、分裂失败等）
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> result;  //创建一个结果向量，用于存储查找结果
  if (GetValue(key, &result, transaction)) {
    // 如果已存在相同的 key，返回 false
    return false;  //如果查找结果不为空，表示已存在相同的 key，直接返回 false
  }

  const auto hash = Hash(key);  //计算 key 的哈希值

  // 获取 header 页，查找对应的目录页
  WritePageGuard header_page = bpm_->FetchPageWrite(header_page_id_);  //获取 header 页
  auto header = header_page.AsMut<ExtendibleHTableHeaderPage>();  //将页面转换为 ExtendibleHTableHeaderPage 类型的指针
  auto directory_idx = header->HashToDirectoryIndex(hash);                  //根据哈希值获取目录索引
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);  //获取目录页 ID
  if (directory_page_id == INVALID_PAGE_ID) {
    // 如果目录页 ID 无效，表示该目录槽未分配任何目录页
    // 需要创建一个新的目录页并插入第一条记录
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }
  header_page.Drop();  //释放 header 页的写锁

  //目录页有效，读取目录页
  WritePageGuard directory_page = bpm_->FetchPageWrite(directory_page_id);  //获取目录页
  auto directory =
      directory_page.AsMut<ExtendibleHTableDirectoryPage>();  //将页面转换为 ExtendibleHTableDirectoryPage 类型的指针
  auto bucket_idx = directory->HashToBucketIndex(hash);          //根据哈希值获取桶索引
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);  //获取桶页 ID
  if (bucket_page_id == INVALID_PAGE_ID) {
    // 如果桶页 ID 无效，表示该桶槽未分配任何桶页
    // 需要创建一个新的桶页并插入第一条记录
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  //如果桶页有效，读取桶页
  WritePageGuard bucket_page = bpm_->FetchPageWrite(bucket_page_id);  //获取桶页
  auto bucket =
      bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指针
  //执行插入
  if (bucket->Insert(key, value, cmp_)) {
    return true;  //返回 true，表示插入成功
  }

  //如果插入失败，可能是因为桶已满,因为键已存在这种情况函数开始部分已经讨论了
  if (directory->GetLocalDepth(bucket_idx) ==
      directory->GetGlobalDepth()) {  //如果本地深度 = 全局深度，全局深度也需要扩展
    if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {  // 如果全局深度已达到最大值,无法拓展，返回false
      return false;                                                 //返回 false，表示插入失败
    }
    // 扩展全局深度
    directory->IncrGlobalDepth();  //增加全局深度
    //复制旧的目录信息到新的目录页的任务已经在IncrGlobalDepth实现了
  }

  //目录页更新完成之后，分裂桶
  //分裂桶
  if (!SplitBucket(directory, bucket, bucket_idx)) {
    //如果分裂失败，返回 false
    return false;  //返回 false，表示插入失败
  }

  bucket_page.Drop();     //释放桶页的写锁
  directory_page.Drop();  //释放目录页的写锁

  return Insert(key, value, transaction);  //递归调用 Insert 函数，尝试重新插入键值对
}

/**
 * 插入辅助
 *
 * 当 header 中对应槽无 directory 页时，新建一个 directory 页并插入第一条记录
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  WritePageGuard directory_page = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();  //创建一个新的目录页

  auto directory =
      directory_page.AsMut<ExtendibleHTableDirectoryPage>();  //将页面转换为 ExtendibleHTableDirectoryPage 类型的指针
  directory->Init(directory_max_depth_);                      //初始化目录页
  header->SetDirectoryPageId(directory_idx, directory_page_id);  //更新 header 中的目录页 ID

  // 在新目录页中插入第一条记录
  auto bucket_idx = directory->HashToBucketIndex(hash);         //根据哈希值获取桶索引
  return InsertToNewBucket(directory, bucket_idx, key, value);  //将键值对插入到新创建的桶中
}

/**
 * 插入辅助
 *
 * 在 directory → bucket slot 无 bucket 页或需要分裂时，创建新 bucket 并插入
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  WritePageGuard bucket_page = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();

  auto bucket =
      bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指针
  bucket->Init(bucket_max_size_);                                 //初始化桶页
  directory->SetBucketPageId(bucket_idx, bucket_page_id);  //更新目录页中的桶页 ID

  return bucket->Insert(key, value, cmp_);  //将键值对插入到新创建的桶中
}

/**
 * 更新目录映射
 *
 * 在 bucket 分裂后，更新 directory 中所有指向该 bucket 的槽映射
 */
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); ++i) {
    if (directory->GetBucketPageId(i) == directory->GetBucketPageId(new_bucket_idx)) {  //找到被拓展的桶
      if ((i & local_depth_mask) == new_bucket_idx) {       //如果当前索引与新桶索引匹配
        directory->SetBucketPageId(i, new_bucket_page_id);  //更新目录页中的桶页 ID
        directory->SetLocalDepth(i, new_local_depth);       //更新本地深度
      } else {
        //否则，不改变桶页id，但是更新本地深度
        //因为分裂后，原桶和新桶的本地深度都增加
        directory->SetLocalDepth(i, new_local_depth);
      }
    }
  }
}

/**
 * 迁移条目
 *
 * 将旧 bucket 中需迁移到新 bucket 的条目搬移过去
 */
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * 从可扩展哈希表中删除指定键的键值对
 * transaction为当前事务
 *
 * 根据 key 删除对应条目（本作业中只需支持唯一键）
 * 返回是否成功
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  const auto hash = Hash(key);  //计算 key 的哈希值

  // 获取 header 页，查找对应的目录页
  WritePageGuard header_page = bpm_->FetchPageWrite(header_page_id_);  //获取 header 页
  auto header = header_page.AsMut<ExtendibleHTableHeaderPage>();  //将页面转换为 ExtendibleHTableHeaderPage 类型的指针
  auto directory_idx = header->HashToDirectoryIndex(hash);                  //根据哈希值获取目录索引
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);  //获取目录页 ID
  if (directory_page_id == INVALID_PAGE_ID) {
    // 如果目录页 ID 无效，表示该目录槽未分配任何目录页
    return false;  //返回 false，表示删除失败
  }

  header_page.Drop();  //释放 header 页的写锁

  //目录页有效，读取目录页
  WritePageGuard directory_page = bpm_->FetchPageWrite(directory_page_id);  //获取目录页
  auto directory =
      directory_page.AsMut<ExtendibleHTableDirectoryPage>();  //将页面转换为 ExtendibleHTableDirectoryPage 类型的指针
  auto bucket_idx = directory->HashToBucketIndex(hash);               //根据哈希值获取桶索引
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);  //获取桶页 ID
  if (bucket_page_id == INVALID_PAGE_ID) {
    // 如果桶页 ID 无效，表示该桶槽未分配任何桶页
    return false;  //返回 false，表示删除失败
  }

  //获取桶页，查找键值对
  WritePageGuard bucket_page = bpm_->FetchPageWrite(bucket_page_id);  //获取桶页
  auto bucket =
      bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指针
  bool res = bucket->Remove(key, cmp_);                           //尝试删除键值对

  bucket_page.Drop();  //释放桶页的写锁
  if (!res) {
    return false;  //如果删除失败，返回 false
  }

  // auto check_page_id = bucket_page_id;
  // ReadPageGuard check_guard = bpm_->FetchPageRead(check_page_id);  //检查桶页是否仍然存在
  // auto check_page = check_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为
  // ExtendibleHTableBucketPage 类型的指针

  // 删除成功后，检查桶是否需要合并
  auto local_depth = directory->GetLocalDepth(bucket_idx);  //获取桶的本地深度
  auto global_depth = directory->GetGlobalDepth();          //获取目录的全局深度
  while (local_depth > 0) {                                 //只要local_depth > 0，就继续检查
    // 检查是否可以合并桶
    auto bucket_index_mask = 1 << (local_depth - 1);                           //计算桶索引掩码
    auto split_bucket_idx = bucket_idx ^ bucket_index_mask;                    //计算分裂桶的索引
    auto split_local_depth = directory->GetLocalDepth(split_bucket_idx);       //获取分裂桶的本地深度
    auto split_bucket_page_id = directory->GetBucketPageId(split_bucket_idx);  //获取分裂桶的桶页 ID
    if (split_bucket_page_id == INVALID_PAGE_ID) {
      // 如果分裂桶的桶页 ID 无效，表示该分裂桶槽未分配任何桶页
      break;  //退出循环
    }
    WritePageGuard split_bucket_page = bpm_->FetchPageWrite(split_bucket_page_id);  //获取分裂桶的桶页
    auto split_bucket =
        split_bucket_page
            .AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指
    if (split_local_depth != local_depth || (!split_bucket->IsEmpty() && !bucket->IsEmpty())) {
      // 如果分裂桶的本地深度不等于当前桶的本地深度，或者两个桶都不为空，则无法合并
      break;  //退出循环
    }

    if (bucket->IsEmpty()) {
      //如果当前桶为空，将分裂桶的条目迁移到当前桶
      bpm_->DeletePage(bucket_page_id);            //删除当前桶页
      bucket = split_bucket;                       //将分裂桶指针赋值给当前桶
      bucket_page_id = split_bucket_page_id;       //更新桶页 ID
      bucket_page = std::move(split_bucket_page);  //更新桶页
    } else {
      bpm_->DeletePage(split_bucket_page_id);  //删除分裂桶页
    }

    //减少局部深度并更新目录页
    directory->DecrLocalDepth(bucket_idx);                                 //减少当前桶的本地深度
    local_depth = directory->GetLocalDepth(bucket_idx);                    //更新本地深度
    uint32_t local_depth_mask = directory->GetLocalDepthMask(bucket_idx);  //获取本地深度掩码
    uint32_t mask_idx = bucket_idx & local_depth_mask;                     //计算掩码索引
    uint32_t update_count = 1 << (global_depth - local_depth);             //计算更新计数
    for (uint32_t i = 0; i < update_count; ++i) {
      auto idx = (i << local_depth) + mask_idx;                                //计算更新索引
      UpdateDirectoryMapping(directory, idx, bucket_page_id, local_depth, 0);  //更新目录映射
    }
  }
  while (directory->CanShrink()) {  //如果目录可以缩小
    directory->DecrGlobalDepth();   //减少全局深度
  }
  return true;  //返回 true，表示删除成功
}

/*****************************************************************************
 * UTILITY
 *****************************************************************************/

/**
 * 帮助函数，用于验证可扩展散列表目录的完整性
 *
 * 递归遍历所有 directory 页面并调用其 VerifyIntegrity()，保证分裂/合并后的目录与深度不变量成立
 */
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::VerifyIntegrity() const {}

/**
 * 返回保存在成员 header_page_id_ 中的 header 页 Page ID，便于外部测试或调试
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetHeaderPageId() const -> page_id_t {
  return header_page_id_;
}

/**
 * 打印哈希表的内容
 */
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::PrintHT() const {}

/**
 * 分裂桶
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                                    ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)
    -> bool {
  // 新建一个桶页
  page_id_t new_bucket_page_id;
  WritePageGuard new_bucket_page = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  auto *new_bucket =
      new_bucket_page
          .AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  //将页面转换为 ExtendibleHTableBucketPage 类型的指针
  new_bucket->Init(bucket_max_size_);                      //初始化新桶页

  uint32_t old_local_depth = directory->GetLocalDepth(bucket_idx);  //拆分前深度
  directory->IncrLocalDepth(bucket_idx);                            //增加桶的本地深度
  uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);   //获取分裂桶的索引
  directory->IncrLocalDepth(split_idx);                             //增加分裂桶的本地深度

  uint32_t new_local_depth = old_local_depth + 1;                        //新的本地深度
  uint32_t old_bucket_page_id = directory->GetBucketPageId(bucket_idx);  //获取原桶页 ID

  uint32_t directory_size = directory->Size();  //获取目录页的大小
  for (uint32_t i = 0; i < directory_size; ++i) {
    uint32_t prefix = i & ((1U << new_local_depth) - 1);  //计算前缀

    //前 new_local 位 == 原桶低 new_local 位 → 指回老桶
    if (prefix == (bucket_idx & ((1U << new_local_depth) - 1))) {
      directory->SetBucketPageId(i, old_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
    } else if (prefix == ((bucket_idx ^ (1U << old_local_depth)) & ((1U << new_local_depth) - 1))) {
      directory->SetBucketPageId(i, new_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
    }
  }

  auto size = bucket->Size();          //获取原桶页的大小
  std::list<std::pair<K, V>> entries;  //创建一个列表用于存储条目
  for (uint32_t i = 0; i < size; ++i) {
    entries.emplace_back(bucket->EntryAt(i));  //将原桶页中的条目添加到列表中
  }
  bucket->Clear();  //清空原桶页
  for (const auto &entry : entries) {
    // 对每个条目，根据哈希值判断应该放入哪个桶
    auto entry_hash = Hash(entry.first);  //计算条目的哈希值

    auto entry_bucket_idx = directory->HashToBucketIndex(entry_hash);  //根据哈希值获取桶索引
    if (entry_bucket_idx == bucket_idx) {
      // 如果条目属于原桶，插入到原桶中
      bucket->Insert(entry.first, entry.second, cmp_);
    } else {
      // 否则，插入到新桶中
      new_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
