#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto GetUndoLogSchema(const UndoLog &undo_log, const Schema *schema) -> Schema {
  std::vector<Column> undo_logs_column;
  for(size_t i = 0; i < schema->GetColumnCount(); i++){
    if(undo_log.modified_fields_[i]){
      undo_logs_column.push_back(schema->GetColumn(i));
    }
  }
  // 如果没有修改的字段，返回一个包含所有字段的schema
  if(undo_logs_column.empty()){
    for(size_t i = 0; i < schema->GetColumnCount(); i++){
      undo_logs_column.push_back(schema->GetColumn(i));
    }
  }
  return Schema(undo_logs_column);
}

namespace tuple_reconstruction_helper{

void Modify(std::vector<Value>&reconstruct_values, const UndoLog &undo_log, const Schema &undo_logs_schema){
  int col = 0;
  for(size_t i = 0; i < reconstruct_values.size(); i++){
    if (undo_log.modified_fields_[i]) {
      reconstruct_values[i] = undo_log.tuple_.GetValue(&undo_logs_schema, col);
      ++col;
    }
  }
}

}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  int column_count = schema->GetColumnCount();
  int is_deleted = base_meta.is_deleted_;

  std::vector<Value> reconstruct_values;
  reconstruct_values.reserve(column_count);
  for (int i = 0; i < column_count; i++){
    reconstruct_values.push_back(base_tuple.GetValue(schema, i));
  }

  // 应用undo logs
  for(auto &ul : undo_logs){
    is_deleted = ul.is_deleted_;
    if(ul.is_deleted_){
      continue;
    }
    auto undo_logs_schema = GetUndoLogSchema(ul, schema);
    tuple_reconstruction_helper::Modify(reconstruct_values, ul, undo_logs_schema);
  }

  // 如果最终状态是删除的，返回nullopt
  if(is_deleted){
    return std::nullopt;
  }
  return Tuple(reconstruct_values, schema);
}

// Helper functions for update executor
auto CreateUndoLog(const TupleMeta &tuple_meta, const Schema *schema, const Tuple &old_tuple, const Tuple &new_tuple, timestamp_t ts) -> UndoLog {
  std::vector<bool> modified_fields;
  std::vector<Value> modified_values;
  
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto old_value = old_tuple.GetValue(schema, i);
    auto new_value = new_tuple.GetValue(schema, i);
    
    if (old_value.CompareEquals(new_value) == CmpBool::CmpTrue) {
      modified_fields.push_back(false);
    } else {
      modified_fields.push_back(true);
      modified_values.push_back(old_value);
    }
  }
  
  std::vector<Column> columns;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (modified_fields[i]) {
      columns.push_back(schema->GetColumn(i));
    }
  }
  
  // 如果没有修改的字段，使用所有字段并保存所有值
  if (columns.empty()) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      columns.push_back(schema->GetColumn(i));
      modified_values.push_back(old_tuple.GetValue(schema, i));
      modified_fields[i] = true;  // 标记所有字段为已修改
    }
  }
  
  Schema partial_schema(columns);
  
  return UndoLog{
    false,  // is_deleted_
    modified_fields,
    Tuple(modified_values, &partial_schema),
    ts,  // Use the provided timestamp instead of tuple_meta.ts_
    UndoLink{}  // prev_version_
  };
}

auto CreateDeleteUndoLog(const TupleMeta &tuple_meta, const Schema *schema, const Tuple &old_tuple) -> UndoLog {
  // 对于删除操作，我们需要保存完整的元组信息
  std::vector<bool> modified_fields(schema->GetColumnCount(), true);
  std::vector<Value> modified_values;
  
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    modified_values.push_back(old_tuple.GetValue(schema, i));
  }
  
  return UndoLog{
    true,  // is_deleted_ = true for delete operation
    modified_fields,
    Tuple(modified_values, schema),
    tuple_meta.ts_,
    UndoLink{}  // prev_version_
  };
}

auto MergeUndoLog(const Schema *schema, const UndoLog &old_undo_log, const UndoLog &new_undo_log) -> UndoLog {
  std::vector<bool> merged_modified_fields;
  std::vector<Value> merged_values;
  
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (old_undo_log.modified_fields_[i]) {
      // 旧undo log修改了这个字段，使用旧undo log的值（旧undo log具有更高优先级）
      merged_modified_fields.push_back(true);
      // 需要从old_undo_log中提取对应的值
      uint32_t old_col_idx = 0;
      for (uint32_t j = 0; j < i; j++) {
        if (old_undo_log.modified_fields_[j]) {
          old_col_idx++;
        }
      }
      
      std::vector<Column> old_columns;
      for (uint32_t j = 0; j < schema->GetColumnCount(); j++) {
        if (old_undo_log.modified_fields_[j]) {
          old_columns.push_back(schema->GetColumn(j));
        }
      }
      Schema old_schema(old_columns);
      merged_values.push_back(old_undo_log.tuple_.GetValue(&old_schema, old_col_idx));
    } else if (new_undo_log.modified_fields_[i]) {
      // 只有新undo log修改了这个字段，使用新undo log的值
      merged_modified_fields.push_back(true);
      // 需要从new_undo_log中提取对应的值
      uint32_t new_col_idx = 0;
      for (uint32_t j = 0; j < i; j++) {
        if (new_undo_log.modified_fields_[j]) {
          new_col_idx++;
        }
      }
      
      std::vector<Column> new_columns;
      for (uint32_t j = 0; j < schema->GetColumnCount(); j++) {
        if (new_undo_log.modified_fields_[j]) {
          new_columns.push_back(schema->GetColumn(j));
        }
      }
      Schema new_schema(new_columns);
      merged_values.push_back(new_undo_log.tuple_.GetValue(&new_schema, new_col_idx));
    } else {
      // 都没有修改这个字段
      merged_modified_fields.push_back(false);
    }
  }
  
  std::vector<Column> columns;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (merged_modified_fields[i]) {
      columns.push_back(schema->GetColumn(i));
    }
  }
  
  // 如果没有修改的字段，这是一个错误情况
  // 因为如果两个undo log都没有修改任何字段，就不应该调用MergeUndoLog
  if (columns.empty()) {
    throw std::runtime_error("Cannot merge undo logs: no fields were modified in either undo log");
  }
  
  Schema merged_schema(columns);
  
  return UndoLog{
    false,  // is_deleted_
    merged_modified_fields,
    Tuple(merged_values, &merged_schema),
    new_undo_log.ts_,
    old_undo_log.prev_version_  // 保持旧的prev_version_
  };
}

auto IsWriteWriteConflict(Transaction *txn, const TupleMeta &meta) -> bool {
  // 如果元组的时间戳是当前事务的ID，说明当前事务已经修改过这个元组，没有冲突
  if (meta.ts_ == txn->GetTransactionId() || meta.ts_ == (txn->GetTransactionId() + TXN_START_ID)) {
    return false;
  }
  
  // 如果元组的时间戳是其他未提交事务的ID（meta.ts_ >= TXN_START_ID），有冲突
  if (meta.ts_ >= TXN_START_ID) {
    return true;
  }
  
  // 如果元组的时间戳是已提交事务的提交时间戳，且在当前事务开始后提交，有冲突
  if (meta.ts_ > txn->GetReadTs()) {
    return true;
  }
  
  return false;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
                // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // 遍历表堆中的所有元组
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();
    
    // 打印当前元组信息
    fmt::print(stderr, "RID={}/{}", rid.GetPageId(), rid.GetSlotNum());
    
    // 打印时间戳
    if (meta.ts_ >= TXN_START_ID) {
      // 这是一个事务ID（临时时间戳）
      fmt::print(stderr, " ts=txn{}", meta.ts_ ^ TXN_START_ID);
    } else {
      // 这是一个提交的时间戳
      fmt::print(stderr, " ts={}", meta.ts_);
    }
    
    // 打印元组内容
    if (meta.is_deleted_) {
      fmt::print(stderr, " <del marker>");
    }
    fmt::print(stderr, " tuple=(");
    
    auto schema = &table_info->schema_;
    for (size_t i = 0; i < schema->GetColumnCount(); i++) {
      if (i > 0) fmt::print(stderr, ", ");
      
      if (meta.is_deleted_) {
        fmt::print(stderr, "<NULL>");
      } else {
        auto value = tuple.GetValue(schema, i);
        if (value.IsNull()) {
          fmt::print(stderr, "<NULL>");
        } else {
          fmt::print(stderr, "{}", value.ToString());
        }
      }
    }
    fmt::println(stderr, ")");
    
    // 遍历版本链
    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(*undo_link);
      if (!undo_log_opt.has_value()) {
        // 悬空指针，垃圾回收时产生
        break;
      }
      
      auto undo_log = undo_log_opt.value();
      
      // 打印undo log信息
      fmt::print(stderr, "  txn{}@{}", 
                 undo_link->prev_txn_ ^ TXN_START_ID, 
                 undo_link->prev_log_idx_);
      
      if (undo_log.is_deleted_) {
        fmt::print(stderr, " <del>");
      } else {
        fmt::print(stderr, " (");
        for (size_t i = 0; i < schema->GetColumnCount(); i++) {
          if (i > 0) fmt::print(stderr, ", ");
          
          if (i < undo_log.modified_fields_.size() && undo_log.modified_fields_[i]) {
            // 这个字段被修改了，显示旧值
            auto value = undo_log.tuple_.GetValue(schema, i);
            if (value.IsNull()) {
              fmt::print(stderr, "<NULL>");
            } else {
              fmt::print(stderr, "{}", value.ToString());
            }
          } else {
            // 这个字段没有被修改，显示占位符
            fmt::print(stderr, "_");
          }
        }
        fmt::print(stderr, ")");
      }
      
      // 打印时间戳
      if (undo_log.ts_ >= TXN_START_ID) {
        fmt::println(stderr, " ts=txn{}", undo_log.ts_ ^ TXN_START_ID);
      } else {
        fmt::println(stderr, " ts={}", undo_log.ts_);
      }
      
      // 移动到下一个版本
      undo_link = undo_log.prev_version_;
    }
    
    ++iter;
  }
}

}  // namespace bustub
