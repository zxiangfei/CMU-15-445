/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-11 14:43:42
 * @FilePath: /CMU-15-445/src/include/storage/page/b_plus_tree_leaf_page.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE \
  16  //叶页头部大小16字节  4 字节 page_type + 4 字节 size + 4 字节 max_size + 4 字节 next_page_id
#define LEAF_PAGE_SLOT_CNT ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(KeyType) + sizeof(ValueType)))

/**
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
/**
 * 叶子B+树页面类
 * 继承自BPlusTreePage类，表示B+树的叶子节点
 * 在叶子页面中存放N个索引键和N个记录ID（RID）
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  //删除所有构造函数和析构函数以确保内存安全
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  /**
   * 初始化叶子页面
   * 设置 page type、size=0、max_size、next_page_id 等，必须在新页分配后调用
   */
  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;     //获取下一个页面ID
  void SetNextPageId(page_id_t next_page_id);  //设置下一个页面ID
  auto KeyAt(int index) const -> KeyType;      //获取指定索引处的键值

  //自己添加
  auto SetKeyAt(int index, const KeyType &key) -> void;      //设置指定索引处的键值
  auto ValueAt(int index) const -> ValueType;                //获取指定索引处的记录ID（RID）
  auto SetValueAt(int index, const ValueType &rid) -> void;  //设置指定索引处的记录ID（RID）

  /**
   * @brief For test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
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
  page_id_t next_page_id_;  //叶子页链表指针
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];    //长度为 LEAF_PAGE_SLOT_CNT 的 key 数组
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];  //长度为 LEAF_PAGE_SLOT_CNT 的 RID 数组
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
