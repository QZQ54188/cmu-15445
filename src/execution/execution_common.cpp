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

  if(is_deleted){
    return std::nullopt;
  }
  return Tuple(reconstruct_values, schema);
}

// Helper functions for update executor
auto CreateUndoLog(const TupleMeta &tuple_meta, const Schema *schema, const Tuple &old_tuple, const Tuple &new_tuple) -> UndoLog {
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
  // 如果没有修改的字段，使用所有字段
  if(columns.empty()){
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      columns.push_back(schema->GetColumn(i));
    }
  }
  Schema partial_schema(columns);
  
  return UndoLog{
    false,  // is_deleted_
    modified_fields,
    Tuple(modified_values, &partial_schema),
    tuple_meta.ts_,
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
  // 如果没有修改的字段，使用所有字段
  if(columns.empty()){
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      columns.push_back(schema->GetColumn(i));
    }
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
  if (meta.ts_ == txn->GetTransactionId()) {
    return false;
  }
  
  // 如果元组的时间戳大于等于当前事务的读时间戳，说明有其他事务在当前事务开始后修改了这个元组，有冲突
  if (meta.ts_ >= txn->GetReadTs()) {
    return true;
  }
  
  return false;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
                // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // 遍历表堆中的所有元组
  auto iter = table_heap->MakeEagerIterator();
  const Schema &schema = table_info->schema_;  // 使用引用访问Schema对象
  
  // 从头开始遍历直到结束
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto [meta, tuple] = table_heap->GetTuple(rid);
    std::stringstream ss;
    
    // 打印RID和当前元组信息
    ss << "RID=" << rid.GetPageId() << "/" << rid.GetSlotNum();
    
    // 检查时间戳是否是事务ID，如果是则打印为txnX
    if (meta.ts_ >= TXN_START_ID) {
      auto txn_id = meta.ts_ ^ TXN_START_ID;  // 转换为人类可读格式
      ss << " ts=txn" << txn_id;
    } else {
      ss << " ts=" << meta.ts_;
    }
    
    // 如果元组已删除，则标记
    if (meta.is_deleted_) {
      ss << " <del marker>";
    }
    
    // 打印元组的值
    ss << " tuple=(";
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      if (i > 0) {
        ss << ", ";
      }
      
      const Value &val = tuple.GetValue(&schema, i);
      if (val.IsNull()) {
        ss << "<NULL>";
      } else {
        switch (val.GetTypeId()) {
          case TypeId::INTEGER:
            ss << val.GetAs<int32_t>();
            break;
          case TypeId::DECIMAL:
            ss << val.GetAs<double>();
            break;
          case TypeId::BOOLEAN:
            ss << (val.GetAs<bool>() ? "true" : "false");
            break;
          default:
            ss << val.ToString();
        }
      }
    }
    ss << ")";
    fmt::println(stderr, "{}", ss.str());
    
    // 获取并打印版本链
    auto version_link = txn_mgr->GetVersionLink(rid);
    if (version_link.has_value()) {
      auto undo_link = version_link->prev_;
      while (undo_link.IsValid()) {
        std::stringstream undo_ss;
        auto undo_log = txn_mgr->GetUndoLog(undo_link);
        
        // 缩进显示
        undo_ss << "  ";
        
        // 显示事务ID
        if (undo_link.prev_txn_ >= TXN_START_ID) {
          auto txn_id = undo_link.prev_txn_ ^ TXN_START_ID;
          undo_ss << "txn" << txn_id;
        } else {
          undo_ss << "ts" << undo_link.prev_txn_;
        }
        
        undo_ss << "@" << undo_link.prev_log_idx_ << " ";
        
        // 如果是删除标记
        if (undo_log.is_deleted_) {
          undo_ss << "<del>";
        } else {
          // 构建部分修改字段
          std::vector<Column> modified_columns;
          std::vector<uint32_t> modified_indexes;
          
          for (uint32_t i = 0; i < undo_log.modified_fields_.size(); i++) {
            if (undo_log.modified_fields_[i]) {
              modified_columns.push_back(schema.GetColumn(i));
              modified_indexes.push_back(i);
            }
          }
          
          // 打印修改的值
          undo_ss << "(";
          for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
            if (i > 0) {
              undo_ss << ", ";
            }
            
            auto it = std::find(modified_indexes.begin(), modified_indexes.end(), i);
            if (it != modified_indexes.end()) {
              size_t idx = it - modified_indexes.begin();
              Schema partial_schema(modified_columns);
              const Value &val = undo_log.tuple_.GetValue(&partial_schema, idx);
              
              if (val.IsNull()) {
                undo_ss << "<NULL>";
              } else {
                switch (val.GetTypeId()) {
                  case TypeId::INTEGER:
                    undo_ss << val.GetAs<int32_t>();
                    break;
                  case TypeId::DECIMAL:
                    undo_ss << val.GetAs<double>();
                    break;
                  case TypeId::BOOLEAN:
                    undo_ss << (val.GetAs<bool>() ? "true" : "false");
                    break;
                  default:
                    undo_ss << val.ToString();
                }
              }
            } else {
              undo_ss << "_";  // 未修改的字段用下划线表示
            }
          }
          undo_ss << ")";
        }
        
        // 打印时间戳
        if (undo_log.ts_ >= TXN_START_ID) {
          auto txn_id = undo_log.ts_ ^ TXN_START_ID;  // 转换为人类可读格式
          undo_ss << " ts=txn" << txn_id;
        } else {
          undo_ss << " ts=" << undo_log.ts_;
        }
        
        fmt::println(stderr, "{}", undo_ss.str());
        
        // 获取下一个版本
        undo_link = undo_log.prev_version_;
      }
    }
    
    // 移到下一个元组
    ++iter;
  }
}

}  // namespace bustub
