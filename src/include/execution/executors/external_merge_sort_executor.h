//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  // 初始化页头
  void Init(const Schema *schema) {
    auto header = GetHeader();
    header->tuple_count_ = 0;
    header->tuple_size_ = schema->GetInlinedStorageSize();
    header->tuple_max_count_ = (BUSTUB_PAGE_SIZE - sizeof(SortPageHeader)) / header->tuple_size_;
  }

  // 添加元组到排序页
  auto InsertTuple(const Tuple &tuple) -> bool {
    if (IsFull()) {
      return false;  // 如果页已满，无法添加更多元组
    }

    // 获取插入位置
    auto header = GetHeader();
    char *insert_tuple_data = GetTuplesData() + header->tuple_count_ * header->tuple_size_;

    // 将元组数据复制到页中
    memcpy(insert_tuple_data, tuple.GetData(), header->tuple_size_);
    header->tuple_count_++;
    return true;
  }

  // 根据索引获取元组
  auto GetTuple(size_t index) const -> Tuple {
    auto header = GetHeader();
    if (index >= header->tuple_count_) {
      throw std::out_of_range("Index out of range");
    }
    const char *tuple_data = GetTuplesData() + index * header->tuple_size_;
    return Tuple{RID{}, tuple_data, static_cast<uint32_t>(header->tuple_size_)};
  }

  // 获取已存储的元组数量
  auto GetTupleCount() const -> size_t { return GetHeader()->tuple_count_; }

  // 获取最大可存储的元组数量
  auto GetMaxTupleCount() const -> size_t { return GetHeader()->tuple_max_count_; }

  // 检查页是否已满
  auto IsFull() const -> bool { return GetHeader()->tuple_count_ >= GetHeader()->tuple_max_count_; }

  // 清空排序页
  void Clear() {
    GetHeader()->tuple_count_ = 0;  // 重置元组计数
  }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  struct SortPageHeader {
    size_t tuple_count_;      // 现有的tuple数量
    size_t tuple_size_;       // 每个tuple的大小
    size_t tuple_max_count_;  // 最大tuple数量
  };

  // 获取排序页头
  auto GetHeader() -> SortPageHeader * { return reinterpret_cast<SortPageHeader *>(data_); }
  auto GetHeader() const -> const SortPageHeader * { return reinterpret_cast<const SortPageHeader *>(data_); }

  // 获取排序页中存储的元组数据
  auto GetTuplesData() -> char * { return data_ + sizeof(SortPageHeader); }
  auto GetTuplesData() const -> const char * { return data_ + sizeof(SortPageHeader); }

  char data_[BUSTUB_PAGE_SIZE];  // 数据存储区域，大小为BUSTUB_PAGE_SIZE
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, const Schema *schema)
      : pages_(std::move(pages)), bpm_(bpm), schema_(schema) {}

  auto GetPageCount() const -> size_t { return pages_.size(); }
  auto GetPageId(size_t index) const -> page_id_t { return pages_[index]; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    // 迭代器前++
    auto operator++() -> Iterator & {
      current_tuple_index_++;

      // 检查当前页是否已遍历完
      if (current_page_ != nullptr && current_tuple_index_ >= current_page_->GetTupleCount()) {
        current_page_index_++;     // 移动到下一页
        current_tuple_index_ = 0;  // 重置元组索引

        // 检查是否还有更多页可供迭代
        if (current_page_index_ < run_->GetPageCount()) {
          // 如果还有更多页，加载下一页
          auto page_guard = run_->bpm_->ReadPage(run_->pages_[current_page_index_]);
          current_page_ = reinterpret_cast<const SortPage *>(page_guard.GetData());
          current_page_guard_ = std::move(page_guard);  // 移动 guard
        } else {
          // 如果没有更多页，设置当前页为 nullptr
          current_page_ = nullptr;
          current_page_guard_ = ReadPageGuard();  // 重置 guard，表示没有更多
        }
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    // 迭代器解引用运算符重载，返回当前迭代器所指向的元组
    auto operator*() -> Tuple {
      if (current_page_ == nullptr || current_tuple_index_ >= current_page_->GetTupleCount()) {
        throw std::out_of_range("Iterator out of range");
      }
      return current_page_->GetTuple(current_tuple_index_);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    // 迭代器相等运算符重载，判断两个迭代器是否指向同一个元组
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && current_page_index_ == other.current_page_index_ &&
             current_tuple_index_ == other.current_tuple_index_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    // 迭代器不等运算符重载，判断两个迭代器是否指向不同的元组或不同的归并路
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}  // 构造函数，初始化迭代器指向归并路的开始
    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;  // 指向归并排序中的某一路

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    size_t current_page_index_{0};           // 当前页索引
    size_t current_tuple_index_{0};          // 当前元组索引
    const SortPage *current_page_{nullptr};  // 当前页对象，用于存储当前页的元组数据
    ReadPageGuard current_page_guard_{};     // 当前页的读锁，用于确保线程安全
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  // 返回指向归并路开始的迭代器，即第一个元组
  auto Begin() const -> Iterator {
    Iterator it(this);
    if (!pages_.empty()) {
      // 如果有页存在，加载第一页
      it.current_page_index_ = 0;
      it.current_tuple_index_ = 0;
      auto page_guard = bpm_->ReadPage(pages_[0]);
      it.current_page_ = reinterpret_cast<const SortPage *>(page_guard.GetData());
      it.current_page_guard_ = std::move(page_guard);  // 移动 guard

      // 确保指向第一个有效tuple
      while (it.current_page_ != nullptr && it.current_page_->GetTupleCount() == 0) {
        ++it;
      }
    }
    return it;  // 返回迭代器
  }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() const -> Iterator {
    Iterator it(this);
    it.current_page_index_ = pages_.size();  // 设置为页数，表示超出最后一页
    it.current_tuple_index_ = 0;             // 重置元组索引
    it.current_page_ = nullptr;              // 当前页设置为 nullptr，表示没有更多元组
    return it;                               // 返回迭代器
  }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;

  // 自定义成员变量
  [[maybe_unused]] const Schema *schema_{nullptr};  // 排序页的模式，用于元组的解析
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;  // 指定比较器，用于tuple的排序

  /** TODO: You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;  // 子执行器，用于获取待排序的元组

  std::optional<MergeSortRun> final_run_;  // 最终的归并路，存储排序后的元组

  std::optional<MergeSortRun::Iterator> final_run_iterator_;  // 最终归并路的迭代器

  auto CreateInitialRuns() -> std::vector<MergeSortRun>;  // 创建初始的归并路
  void SortPageTuples(SortPage *page);                    // 对排序页内的元组进行排序
  auto MergeRuns(const std::vector<MergeSortRun> &runs) -> std::vector<MergeSortRun>;     // 两两归并一遍
  auto MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun;  // 二路归并
};

}  // namespace bustub
