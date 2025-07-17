/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-10 10:56:59
 * @FilePath: /CMU-15-445/src/include/storage/page/b_plus_tree_page.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
/**
 * 页面类型枚举类
 * 枚举类用于表示B+树页面的类型，包括无效页面、叶子页面和内部页面
 * INVALID_INDEX_PAGE: 无效页面，通常用于初始化或错误状态
 * LEAF_PAGE: 叶子页面，存储实际的数据项
 * INTERNAL_PAGE: 内部页面，存储索引信息，用于导航到叶子页面
 */
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
/**
 * 页面基类
 * 包含公有字段12字节
 * PageType (4字节): 页面类型，表示该页面是叶子页面还是内部页面
 * CurrentSize (4字节): 当前页面中存储的键值对数量
 * MaxSize (4字节): 页面可以存储的最大键值对数量
 */
class BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  //删除所有构造函数和析构函数以确保内存安全
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  auto IsLeafPage() const -> bool;            //查询当前是否是叶子页面
  void SetPageType(IndexPageType page_type);  //设置页面类型

  auto GetSize() const -> int;    //返回当前页面中存储的键值对数量
  void SetSize(int size);         //设置当前页面中存储的键值对数量
  void ChangeSizeBy(int amount);  //改变当前页面中存储的键值对数量，增加或减少amount

  auto GetMaxSize() const -> int;  //返回页面可以存储的最大键值对数量
  void SetMaxSize(int max_size);   //设置页面可以存储的最大键值对数量
  auto GetMinSize() const -> int;  //返回页面的最小键值对数量

 private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));  // 页面类型
  // Number of key & value pairs in a page
  int size_ __attribute__((__unused__));  // 当前页面中存储的键值对数量
  // Max number of key & value pairs in a page
  int max_size_ __attribute__((__unused__));  // 页面可以存储的最大键值对数量
};

}  // namespace bustub
