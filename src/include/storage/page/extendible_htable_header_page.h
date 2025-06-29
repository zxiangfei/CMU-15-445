/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-28 23:12:23
 * @FilePath: /CMU-15-445/src/include/storage/page/extendible_htable_header_page.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.h
//
// Identification: src/include/storage/page/extendible_htable_header_page.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Header page format:
 *  ---------------------------------------------------
 * | DirectoryPageIds(2048) | MaxDepth (4) | Free(2044)
 *  ---------------------------------------------------
 */

/**
┌──────────────┐
│ Header Page  │ ←── page_id = 0
│──────────────│
│ • max_depth           │ // 当前目录页的最大深度
│* directory_page_ids_[]   │  目录页ID数组
└──────────────┘
       │
       ▼
┌───────────────────────────────┐
│ Directory Page(s)             │ ←── page(s) 1…N
│───────────────────────────────│
| * max_depth_                │  // 当前目录页的最大深度
│ • local_depths_[ ]            │  // 每个槽的本地深度
│ • bucket_page_ids_[ ]         │  // 指向 BucketPage 的 page_id
│ • global_depth                │  // 当前的全局深度
└───────────────────────────────┘
       │
       ▼
┌───────────────────────────────┐
│ Bucket Page(s)                │ ←── page_id 自由分配
│───────────────────────────────│
│ • array< KeyType, ValueType > │  // 固定大小的键值槽
│ • size                         │  // 当前桶的大小
│ • max_size_                   │  // 最大桶大小
└───────────────────────────────┘
 */

#pragma once

#include <cstdlib>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

static constexpr uint64_t HTABLE_HEADER_PAGE_METADATA_SIZE = sizeof(uint32_t);
static constexpr uint64_t HTABLE_HEADER_MAX_DEPTH = 9;
static constexpr uint64_t HTABLE_HEADER_ARRAY_SIZE = 1 << HTABLE_HEADER_MAX_DEPTH;

class ExtendibleHTableHeaderPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  ExtendibleHTableHeaderPage() = delete;  //禁止无参构造，确保页内存由缓冲池管理
  DISALLOW_COPY_AND_MOVE(ExtendibleHTableHeaderPage);  //通过宏删除拷贝/移动构造和赋值，避免重复或意外复制内部大数组

  /**
   * After creating a new header page from buffer pool, must call initialize
   * method to set default values
   * @param max_depth Max depth in the header page
   */
  void Init(uint32_t max_depth = HTABLE_HEADER_MAX_DEPTH);

  /**
   * Get the directory index that the key is hashed to
   *
   * @param hash the hash of the key
   * @return directory index the key is hashed to
   */
  auto HashToDirectoryIndex(uint32_t hash) const -> uint32_t;

  /**
   * Get the directory page id at an index
   *
   * @param directory_idx index in the directory page id array
   * @return directory page_id at index
   */
  auto GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t;

  /**
   * @brief Set the directory page id at an index
   *
   * @param directory_idx index in the directory page id array
   * @param directory_page_id page id of the directory
   */
  void SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id);

  /**
   * @brief Get the maximum number of directory page ids the header page could handle
   */
  auto MaxSize() const -> uint32_t;

  /**
   * Prints the header's occupancy information
   */
  void PrintHeader() const;

 private:
  page_id_t directory_page_ids_[HTABLE_HEADER_ARRAY_SIZE];  //定长数组，存放每个目录槽对应的目录页 ID
  uint32_t max_depth_;  //当前使用的目录深度（决定数组有效前缀长度）
};

static_assert(sizeof(page_id_t) == 4);

static_assert(sizeof(ExtendibleHTableHeaderPage) ==
              sizeof(page_id_t) * HTABLE_HEADER_ARRAY_SIZE + HTABLE_HEADER_PAGE_METADATA_SIZE);

static_assert(sizeof(ExtendibleHTableHeaderPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
