/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-17 10:47:03
 * @FilePath: /CMU-15-445/test/include/storage/b_plus_tree_utils.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_utils.h
//
// Identification: test/include/storage/b_plus_tree_utils.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

template <typename KeyType, typename KeyValue, typename KeyComparator>
bool TreeValuesMatch(BPlusTree<KeyType, KeyValue, KeyComparator> &tree, std::vector<int64_t> &inserted,
                     std::vector<int64_t> &deleted) {
  std::vector<KeyValue> rids;
  KeyType index_key;
  for (auto &key : inserted) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    if (rids.size() != 1) {
      return false;
    }
  }
  for (auto &key : deleted) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    if (!rids.empty()) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
