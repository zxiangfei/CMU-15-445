/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-17 11:04:56
 * @FilePath: /CMU-15-445/src/storage/index/index_iterator.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard page_guard, int index)
    : bpm_(bpm), page_guard_(std::move(page_guard)), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool {
  // 判断当前迭代器是否到达末尾
  return index_ == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  // 返回当前迭代器指向的键值对
  assert(!IsEnd());  // 确保迭代器没有到达末尾
  auto leaf_page = page_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  std::pair<const KeyType &, const ValueType &> result(leaf_page->KeyAt(index_), leaf_page->ValueAt(index_));
  return result;  //返回键值对
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;  // 将索引加1，移动到下一个键值对
  auto leaf_page = page_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (index_ >= leaf_page->GetSize()) {
    //如果索引超过当前叶子页的大小，表示需要移动到下一个叶子页
    page_id_t next_page_id = leaf_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      index_ = -1;  // 如果没有下一个叶子页，设置索引为 -1，表示迭代器到达末尾
    } else {
      // 读取下一个叶子页
      page_guard_ = bpm_->ReadPage(next_page_id);
      index_ = 0;  // 重置索引为0，开始遍历
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const INDEXITERATOR_TYPE &itr) const -> bool {
  // 比较两个迭代器是否相等
  if (IsEnd() && itr.IsEnd()) {
    return true;  // 如果两个都是末尾迭代器，认为它们相等
  }
  if (IsEnd() || itr.IsEnd()) {
    return false;  // 如果其中一个是末尾迭代器，认为它们不相等
  }
  return bpm_ == itr.bpm_ && page_guard_.GetPageId() == itr.page_guard_.GetPageId() && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const INDEXITERATOR_TYPE &itr) const -> bool {
  // 比较两个迭代器是否不相等
  return !(*this == itr);  // 使用 == 运算符的结果取反
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
