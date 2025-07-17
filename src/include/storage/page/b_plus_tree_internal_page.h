//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12  //内部页头部大小，12字节
#define INTERNAL_PAGE_SLOT_CNT                      \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / \
   ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT   内部页槽位数量

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
/**
 * 内部B+树页面类
 * 继承自BPlusTreePage类，表示B+树的内部节点
 * 在内部页面中存放N个索引键和N+1个子指针（页ID）
 * 子指针PAGE_ID(i)指向一个子树，其中所有键K满足：K(i) <= K < K(i+1)
 * key_array_[0] 保留无效，真正的第一个 key 从下标 1 开始，查找时忽略下标 0
 */
INDEX_TEMPLATE_ARGUMENTS  //模版宏定义，在b_plus_tree_page.h中定义
    class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  //删除所有构造函数和析构函数以确保内存安全
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid `BPlusTreeInternalPage`
   * @param max_size Maximal size of the page
   */
  /**
   * 初始化内部页面
   * 设置 page type、size=0、max_size 等，必须在新页分配后调用
   */
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  /**
   * 获取指定索引处的键
   * 返回 key_array_[index]，索引从 1 到 GetSize()-1
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  /**
   * 修改对应位置的 key
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  /**
   * 查找指定值的索引
   * 在 page_id_array_ 中线性或二分查找 value，返回其下标
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   * @param index The index to search for
   * @return The value at the index
   */
  /**
   * 获取指定索引处的值
   * 返回 page_id_array_[index]，索引从 0 到 GetSize()-1
   */
  auto ValueAt(int index) const -> ValueType;

  void SetValueAt(int index, const ValueType &value);

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  /**
   * 调试用，拼接所有有效 key 的字符串
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // Array members for page data.
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];        //长度为 INTERNAL_PAGE_SLOT_CNT 的 key 数组
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];  //长度为 INTERNAL_PAGE_SLOT_CNT 的数组，存储子指针（页ID）
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
