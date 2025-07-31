//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();  // 初始化子执行器

  // 构造初始的归并路
  std::vector<MergeSortRun> initial_runs = CreateInitialRuns();

  // 多次归并，直到只剩下一个归并路
  while (initial_runs.size() > 1) {
    initial_runs = MergeRuns(initial_runs);  // 两两归并
  }

  // 归并完成之后设置最终的归并路
  if (!initial_runs.empty()) {
    final_run_ = std::move(initial_runs[0]);    // 取出最后的归并路
    final_run_iterator_ = final_run_->Begin();  // 初始化迭代
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (!final_run_iterator_.has_value() || *final_run_iterator_ == final_run_->End()) {
    return false;  // 如果迭代器已到达末尾，返回 false
  }

  // 获取当前迭代器指向的元组
  *tuple = **final_run_iterator_;
  *rid = tuple->GetRid();    // 设置 RID
  ++(*final_run_iterator_);  // 迭代器前进

  return true;  // 返回 true，表示成功获取到一个元组
}

/**
 * 初始化归并排序，每个归并路对应一个 SortPage。
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::CreateInitialRuns() -> std::vector<MergeSortRun> {
  std::vector<MergeSortRun> initial_runs;  // 存储初始的归并路

  // 创建新SortPage
  page_id_t current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
  auto current_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
  current_page->Init(&plan_->OutputSchema());  // 初始化当前页

  Tuple tuple;
  RID rid;
  // 从子执行器中获取元组，直到没有更多元组
  while (child_executor_->Next(&tuple, &rid)) {
    // 检查当前页是否已满
    if (current_page->IsFull()) {
      // 如果当前页已满，先对页内进行排序，再将当前页加入初始归并路，并创建下一个新SortPage
      SortPageTuples(current_page);

      initial_runs.emplace_back(std::vector<page_id_t>{current_page_id}, exec_ctx_->GetBufferPoolManager(),
                                &plan_->OutputSchema());
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      current_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
      current_page->Init(&plan_->OutputSchema());  // 初始化新页
    }

    // 将元组插入当前页
    current_page->InsertTuple(tuple);
  }

  // 如果最后一个页还有未满的元组，仍然需要将其加入初始归并路
  if (current_page->GetTupleCount() > 0) {
    SortPageTuples(current_page);  // 对最后一个页内的元组进行排序
    initial_runs.emplace_back(std::vector<page_id_t>{current_page_id}, exec_ctx_->GetBufferPoolManager(),
                              &plan_->OutputSchema());
  } else {
    // 如果最后一个页没有元组，则释放该页
    exec_ctx_->GetBufferPoolManager()->DeletePage(current_page_id);  // 删除空页
  }

  return initial_runs;  // 返回初始的归并路列表
}

/**
 * 对SortPage内部进行排序
 */
template <size_t K>
void ExternalMergeSortExecutor<K>::SortPageTuples(SortPage *page) {
  size_t tuple_count = page->GetTupleCount();
  if (tuple_count <= 1) {
    return;  // 如果元组数量小于等于1，则无需排序
  }

  // 将每个tuple转换为SortEntry以便排序
  std::vector<SortEntry> entries;
  entries.reserve(tuple_count);  // 预留空间以提高性能
  for (size_t i = 0; i < tuple_count; ++i) {
    Tuple tuple = page->GetTuple(i);
    auto key = GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());

    entries.emplace_back(std::move(key), tuple);
  }

  // 使用std::sort对entries进行排序
  std::sort(entries.begin(), entries.end(), cmp_);

  // 清空当前页
  page->Clear();
  // 将排序后的元组重新插入到SortPage中
  for (const auto &entry : entries) {
    if (!page->InsertTuple(entry.second)) {
      throw std::runtime_error("Failed to insert sorted tuple into SortPage");
    }
  }
}

/**
 * 两两归并已排序的归并路，返回新的归并路
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeRuns(const std::vector<MergeSortRun> &runs) -> std::vector<MergeSortRun> {
  std::vector<MergeSortRun> merged_runs;

  // 如果只有一个归并路，直接返回
  if (runs.size() <= 1) {
    return runs;
  }

  // 两两归并
  for (size_t i = 0; i < runs.size(); i += 2) {
    if (i + 1 < runs.size()) {
      // 如果有两个归并路，进行二路归并
      merged_runs.push_back(MergeTwoRuns(runs[i], runs[i + 1]));
    } else {
      // 如果只有一个归并路，直接添加到结果中
      merged_runs.push_back(runs[i]);
    }
  }

  // 归并完成之后，删除已被合并的归并路
  for (const auto &run : runs) {
    for (size_t i = 0; i < run.GetPageCount(); ++i) {
      exec_ctx_->GetBufferPoolManager()->DeletePage(run.GetPageId(i));  // 删除已合并的页
    }
  }

  return merged_runs;  // 返回合并后的归并路列表
}

/**
 * 二路归并两个已排序的归并路，返回新的归并路
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun {
  std::vector<page_id_t> out_run_pages;  // 归并结果的页ID列表

  // 两路归并的迭代器
  auto it1 = run1.Begin();
  auto it2 = run2.Begin();

  // 创建新页，用于写归并结果
  page_id_t current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
  auto current_page =
      reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());  // GetDataMut是获取写(可变)类型 GetData是const类型
  current_page->Init(&plan_->OutputSchema());                         // 初始化当前页

  // 迭代两个归并路，直到其中一个归并路耗尽
  while (it1 != run1.End() && it2 != run2.End()) {
    Tuple insert_tuple;  // 用于标记当前循环应该插入的元组

    // 获取两个迭代器元组
    Tuple tuple1 = *it1;
    Tuple tuple2 = *it2;

    // 获取排序键
    auto key1 = GenerateSortKey(tuple1, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
    auto key2 = GenerateSortKey(tuple2, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
    // 构建排序对象
    SortEntry entry1{std::move(key1), tuple1};
    SortEntry entry2{std::move(key2), tuple2};

    // 比较两个两个排序对象
    if (cmp_(entry1, entry2)) {
      insert_tuple = entry1.second;  // 如果entry1小于entry2，插入entry1的元组
      ++it1;                         // 移动迭代器
    } else {
      insert_tuple = entry2.second;  // 否则插入entry2的元组
      ++it2;                         // 移动迭代器
    }

    // 检查当前页是否已满
    if (current_page->IsFull()) {
      // 如果当前页已满，保存当前页ID到结果列表，并创建新页
      out_run_pages.push_back(current_page_id);
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      current_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
      current_page->Init(&plan_->OutputSchema());  // 初始化新页
    }

    current_page->InsertTuple(insert_tuple);  // 将元组插入当前页
  }

  // 将剩余的归并路中的元组全部插入到当前页
  while (it1 != run1.End()) {
    if (current_page->IsFull()) {
      out_run_pages.push_back(current_page_id);  // 如果当前页已满，保存当前页ID
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      current_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
      current_page->Init(&plan_->OutputSchema());  // 初始化新页
    }
    current_page->InsertTuple(*it1);  // 插入剩余的元组
    ++it1;                            // 移动迭代器
  }

  while (it2 != run2.End()) {
    if (current_page->IsFull()) {
      out_run_pages.push_back(current_page_id);  // 如果当前页已满，保存当前页ID
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      current_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      current_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
      current_page->Init(&plan_->OutputSchema());  // 初始化新页
    }
    current_page->InsertTuple(*it2);  // 插入剩余的元组
    ++it2;                            // 移动迭代器
  }

  // 如果当前页有元组，保存当前页ID到结果列表
  if (current_page->GetTupleCount() > 0) {
    out_run_pages.push_back(current_page_id);
  } else {
    exec_ctx_->GetBufferPoolManager()->DeletePage(current_page_id);  // 如果当前页没有元组，删除该页
  }

  // 返回新的归并路
  return {std::move(out_run_pages), exec_ctx_->GetBufferPoolManager(), &plan_->OutputSchema()};
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
