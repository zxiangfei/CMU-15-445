//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard page_guard, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() const -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

  // auto operator==(const IndexIterator &itr) const -> bool {
  //   return ((index_ == -1 && itr.index_ == -1) ||
  //           (index_ == itr.index_ && page_guard_.GetPageId() == itr.page_guard_.GetPageId()));
  // }

  // auto operator!=(const IndexIterator &itr) const -> bool {
  //   return (index_ != itr.index_ ||
  //           (!(index_ == -1 && itr.index_ == -1) && page_guard_.GetPageId() != itr.page_guard_.GetPageId()));
  // }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;    //用于读取页面的缓冲区管理器
  ReadPageGuard page_guard_;  //当前迭代器所在页的读锁
  int index_;                 //当前迭代器在叶子页中的索引
};

}  // namespace bustub
