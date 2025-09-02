//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include <vector>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  for (size_t i = 0; i < entry_a.first.size(); i++) {
    if (order_bys_[i].first == OrderByType::INVALID) {
      return false;
    }

    if (order_bys_[i].first == OrderByType::DEFAULT || order_bys_[i].first == OrderByType::ASC) {
      if (entry_a.first[i].CompareLessThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return true;
      }
      if (entry_a.first[i].CompareGreaterThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return false;
      }
    }

    if (order_bys_[i].first == OrderByType::DESC) {
      if (entry_a.first[i].CompareGreaterThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return true;
      }
      if (entry_a.first[i].CompareLessThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return false;
      }
    }
  }
  // order by 中每一项全相等
  return true;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());  // 预留空间以提高性能

  for (const auto &order_by : order_bys) {
    const auto &expr = order_by.second;
    key.push_back(expr->Evaluate(&tuple, schema));
  }

  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
/**
 * 通过提供的 undo 日志从 base_tuple 重建一个元组，undo_logs 中的所有日志都会被应用，从头到尾依次回退，不需要考虑时间戳
 * 因为版本链筛选（哪些日志需要传入）已经在事务管理器或执行器里完成；这里只管把给日志“倒序”应用
 * @param schema 基础元组和返回元组的模式
 * @param base_tuple 基础元组，从这个元组开始重建
 * @param base_meta 基础元组的元数据
 * @param undo_logs 要应用的 undo 日志列表，前面的日志会先被应用
 * @return 一个可选的元组，表示重建后的元组。如果重建的结果是一个被删除的元组，则返回 std::nullopt。
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // 特殊情况：base_tuple为deleted，且undo_logs中为空，则重构后依旧为deleted，则返回nullopt
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }

  // 成员为vector，默认拷贝构造即深拷贝
  Tuple tuple(base_tuple);
  bool delete_flag = false;
  for (const auto &undo_log : undo_logs) {
    if (undo_log.is_deleted_) {
      delete_flag = true;
      continue;
    }

    delete_flag = false;
    std::vector<Value> values;
    std::vector<Column> columns;
    // 先获取目前undo_log的需要修改的列组成的undolog_schema
    for (int i = 0; i < static_cast<int>(undo_log.modified_fields_.size()); i++) {
      if (undo_log.modified_fields_[i]) {
        columns.emplace_back(schema->GetColumn(i));
      }
    }
    Schema undo_log_schema(columns);

    int idx = 0;
    for (int i = 0; i < static_cast<int>(undo_log.modified_fields_.size()); i++) {
      if (undo_log.modified_fields_[i]) {
        values.emplace_back(undo_log.tuple_.GetValue(&undo_log_schema, idx++));
      } else {
        values.emplace_back(tuple.GetValue(schema, i));
      }
    }
    tuple = {values, schema};
  }

  if (delete_flag) {
    return std::nullopt;
  }
  return tuple;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
/**
 * 收集需要回退的版本链
 * @param rid 元组的 RID
 * @param base_meta 基础元组的元数据
 * @param base_tuple 基础元组
 * @param undo_link 最新的 undo 链接
 * @param txn 当前事务
 * @param txn_mgr 事务管理器
 * @return 一个可选的 undo 日志向量，传递给 ReconstructTuple
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  // p.s. 此函数中不需要管 base_tuple 和 undo_log 的 delete 字段，只需要收集需要用到的UndoLog
  // 对于delete的处理在ReconstructTuple函数中进行

  // 1. 如果table heap中元组是相对事务read_ts_最新的数据
  // p.s. 被删除也是被修改，会修改ts_为tmp_ts，大小大于commit_ts范围，所以不会在此类情况中。所以不需要判断delete
  auto read_ts = txn->GetReadTs();
  if (base_meta.ts_ <= read_ts) {
    return std::vector<UndoLog>();
  }

  // 2. 如果table heap中元组被当前事务修改
  if (base_meta.ts_ >= TXN_START_ID && base_meta.ts_ == txn->GetTransactionTempTs()) {
    return std::vector<UndoLog>();
  }

  // 3. 如果table heap中元组被其他未提交事务修改 or table heap中元组比当前事务read_ts_更新
  if (!undo_link.has_value()) {
    return std::nullopt;
  }
  auto tmp_link = undo_link.value();
  std::vector<UndoLog> undo_logs;
  while (tmp_link.IsValid()) {
    auto undo_log = txn_mgr->GetUndoLog(tmp_link);
    timestamp_t ts = undo_log.ts_;
    undo_logs.emplace_back(undo_log);

    if (ts <= read_ts) {
      return undo_logs;
    }

    tmp_link = undo_log.prev_version_;
  }
  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
/**
 * 某个事务第一次就该某条记录调用
 * 生成一个新的 undo 日志，表示事务第一次尝试修改这个元组时的状态
 * @param schema 表的模式
 * @param base_tuple 更新前的基础元组，从表堆中检索到的元组。若是插入则为nullptr
 * @param target_tuple 更新后的目标元组。若是删除则为nullptr
 * @param ts 基础元组的时间戳
 * @param prev_version 最新的 undo 日志的 undo 链接，可能为空
 * @return 生成的 undo 日志，并已经挂到prev_version 上
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // 1. 如果为insert的情况
  // (目前在insert_executor中，没有使用该函数，
  // 即在一般的insert时，不需要生成UndoLog，
  // 但是在将元组插入到被删除的tuple时，会需要生成UndoLog。所以这里生成一个空的UndoLog)
  // 如果是在被删除的tuple上插入，
  // 生成的UndoLog 会记录{插入之前是删除的，修改了所有字段，修改之前值空tuple，ts, prev_version}
  if (base_tuple == nullptr) {
    std::vector<bool> modified_fields(schema->GetColumnCount(), true);
    return {true, modified_fields, Tuple{}, ts, prev_version};
  }

  // 2. 如果为delete的情况
  // 生成的UndoLog会记录{删除之前是存在的，修改了所有字段，修改之前值为base_tuple，ts, prev_version}
  if (target_tuple == nullptr) {
    std::vector<bool> modified_fields(schema->GetColumnCount(), true);
    return {false, modified_fields, *base_tuple, ts, prev_version};
  }

  // 3. 如果为update的情况
  std::vector<bool> modified_fields;
  std::vector<Value> values;
  std::vector<Column> columns;

  // 遍历每个字段判断是否相同，不相同表示修改，modified_fields[i]标记为修改 并把修改前字段的值放入values中
  for (int i = 0; i < static_cast<int>(schema->GetColumnCount()); i++) {
    if (base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
      modified_fields.emplace_back(false);
    } else {
      modified_fields.emplace_back(true);
      values.emplace_back(base_tuple->GetValue(schema, i));
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  Schema new_schema{columns};
  Tuple tuple{values, &new_schema};
  tuple.SetRid(base_tuple->GetRid());
  return {false, modified_fields, tuple, ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
/**
 * 某个事务再次修改同一条记录时调用
 * 把新改动与“那一条”旧的 UndoLog 合并为仍然只有一条 UndoLog
 * @param schema 表的模式
 * @param base_tuple 更新前的基础元组，从表堆中检索到的元组。之前已经删除为nullptr
 * @param target_tuple 更新后的目标元组。如果此条修改是删除操作为nullptr
 * @param log 原始的 undo 日志
 * @return 更新后的 undo 日志
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // 如果log为insert后的空UndoLog，则按照特殊情况处理，返回的结果依旧为空UndoLog
  if (log.is_deleted_) {
    return log;
  }
  // 先将原本的tuple重建出来
  std::vector<Column> columns;
  for (int i = 0; i < static_cast<int>(log.modified_fields_.size()); i++) {
    if (log.modified_fields_[i]) {
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  Schema undo_log_schema(columns);  // 生成一个新的schema，包含了所有修改的列

  // 如果是删除
  if (target_tuple == nullptr) {
    std::vector<bool> modified_fields(schema->GetColumnCount(), true);

    // 生成旧的元组
    std::vector<Value> values;
    int idx = 0;
    for (int i = 0; i < static_cast<int>(log.modified_fields_.size()); i++) {
      if (log.modified_fields_[i]) {
        values.emplace_back(log.tuple_.GetValue(&undo_log_schema, idx++));
      } else {
        values.emplace_back(base_tuple->GetValue(schema, i));
      }
    }
    Tuple old_tuple = {values, schema};
    old_tuple.SetRid(log.tuple_.GetRid());
    return {log.is_deleted_, modified_fields, old_tuple, log.ts_, log.prev_version_};
  }

  // 不是删除就更新 undo_log
  std::vector<bool> modified_fields;
  std::vector<Value> values;
  columns.clear();
  int idx = 0;
  for (int i = 0; i < static_cast<int>(log.modified_fields_.size()); i++) {
    // 若为false，表示与base_tuple中对应列值一致，则可以直接用base_tuple的对应值进行比较
    if (!log.modified_fields_[i]) {
      if (base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
        modified_fields.emplace_back(false);
      } else {
        modified_fields.emplace_back(true);
        values.emplace_back(base_tuple->GetValue(schema, i));
        columns.emplace_back(schema->GetColumn(i));
      }
    } else {
      // 若为true，则可以在undo_log中直接获取上一版本的值
      modified_fields.push_back(true);
      values.emplace_back(log.tuple_.GetValue(&undo_log_schema, idx++));
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  Schema new_schema{columns};
  Tuple tuple{values, &new_schema};
  // 为了方便garbage collection的设计
  tuple.SetRid(log.tuple_.GetRid());
  return {log.is_deleted_, modified_fields, tuple, log.ts_, log.prev_version_};
}

auto TsToString(timestamp_t ts) {
  if (ts >= TXN_START_ID) {
    return "txn" + fmt::to_string(ts - TXN_START_ID);
  }
  return fmt::to_string(ts);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    auto tuple_info = iter.GetTuple();
    fmt::println("RID={}/{} ts={} {} tuple={} ", std::to_string(rid.GetPageId()), std::to_string(rid.GetSlotNum()),
                 TsToString(tuple_info.first.ts_), tuple_info.first.is_deleted_ ? "<del marker>" : "",
                 tuple_info.second.ToString(&table_info->schema_));

    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      // 把“当前头部”的值先展开到 vec，后面按需要用 undo 覆盖
      std::vector<Value> vec;
      vec.reserve(table_info->schema_.GetColumnCount());
      for (size_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
        vec.push_back(tuple_info.second.GetValue(&table_info->schema_, i));
      }

      // 如果这个 undo 来源的事务已经被清理，就不要往下打印，避免访问无主日志
      if (txn_mgr->txn_map_.count(undo_link->prev_txn_) == 0) {
        ++iter;
        continue;
      }

      auto log = txn_mgr->GetUndoLog(undo_link.value());

      auto print_one = [&](const UndoLog &lg, const UndoLink &lk, const char *prefix) {
        // 构造一份用于打印的向量：删除型 undo -> 打印 NULL；非删除 -> 覆盖被修改的列
        std::vector<Value> to_print = vec;
        if (!lg.is_deleted_) {
          std::vector<Column> cols;
          cols.reserve(lg.modified_fields_.size());
          for (size_t i = 0; i < lg.modified_fields_.size(); i++) {
            if (lg.modified_fields_[i]) {
              cols.push_back(table_info->schema_.GetColumn(i));
            }
          }
          Schema log_schema(cols);
          size_t col_idx = 0;
          for (size_t i = 0; i < lg.modified_fields_.size(); i++) {
            if (lg.modified_fields_[i]) {
              to_print[i] = lg.tuple_.GetValue(&log_schema, col_idx++);
            }
          }
        } else {
          // 删除型 undo：用 NULL 占位，避免访问空 tuple
          to_print.clear();
          to_print.reserve(table_info->schema_.GetColumnCount());
          for (size_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
            to_print.emplace_back(ValueFactory::GetNullValueByType(table_info->schema_.GetColumn(i).GetType()));
          }
        }

        fmt::println("   {}@{} {} tuple={} ts={}", TsToString(lk.prev_txn_), std::to_string(lk.prev_log_idx_),
                     lg.is_deleted_ ? "<del marker>" : "",
                     Tuple(to_print, &table_info->schema_).ToString(&table_info->schema_), std::to_string(lg.ts_));
      };

      // 打印第一条
      print_one(log, *undo_link, "");
      // 继续沿链打印
      while (log.prev_version_.IsValid()) {
        if (txn_mgr->txn_map_.count(log.prev_version_.prev_txn_) == 0) {
          break;
        }
        // 注意：这里为了可读性，我们保持“to_print 从 vec 出发”的逻辑；
        // 如果你希望层层叠加打印，也可以把 vec = to_print 放进循环。
        auto next_link = log.prev_version_;
        log = txn_mgr->GetUndoLog(next_link);
        print_one(log, next_link, "txn");
      }
    }
    ++iter;
  }
  fmt::println("");
}

}  // namespace bustub
