/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-29 16:52:40
 * @FilePath: /CMU-15-445/src/storage/page/extendible_htable_header_page.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

/**
 * 从缓冲池创建新的header page后，必须调用初始化方法来设置默认值
 * Init 用于设置 max_depth_ 并将目录 ID 数组初始化为无效页 ID
 */
void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  assert(max_depth <= HTABLE_HEADER_MAX_DEPTH && max_depth >= 0);
  max_depth_ = max_depth;
  for (auto &directory_page_id : directory_page_ids_) {
    directory_page_id = INVALID_PAGE_ID;  // 初始化所有目录页 ID 为无效页 ID
  }
}

/**
 * 获取键的哈希值对应的目录索引
 * 根据 max_depth_，取哈希值的高 max_depth_ 位，计算出对应目录槽的索引
 */
auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (max_depth_ == 0) {
    return 0;
  }
  // 左移（32 - max_depth_）位后，低 max_depth_ 位即为原来的高位
  return hash >> (32 - max_depth_);
}

/**
 * 在索引处获取目录页id
 * 返回给定索引处存储的 page_id_t（实为 uint32_t）值，并做边界检查
 */
auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  assert(directory_idx < MaxSize());  // 确保索引在有效范围内
  return directory_page_ids_[directory_idx];
}

/**
 * 在索引处设置目录页id
 * 将给定 directory_page_id 写入索引位置，同样带边界检查
 */
void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  assert(directory_idx < MaxSize());  // 确保索引在有效范围内
  if (directory_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::INVALID, "Cannot set directory page ID to invalid page ID");
  }
  directory_page_ids_[directory_idx] = directory_page_id;  // 设置目录页 ID
}

/**
 * 获取头页可以处理的最大目录页id数
 * 返回 2^max_depth_，即在现有深度下可索引的桶数量
 */
auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return 1U << max_depth_;  // 返回 2^max_depth_
}

/**
 * 打印头页的占用信息
 * 打印 max_depth_、MaxSize() 以及所有 directory_page_ids_ 以便调试
 */
void ExtendibleHTableHeaderPage::PrintHeader() const {}

}  // namespace bustub
