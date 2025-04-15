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

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }

  bool is_deleted = base_meta.is_deleted_;
  Tuple current_tuple = base_tuple;
  uint32_t column_count = schema->GetColumnCount();

  for (const auto &undo_log : undo_logs) {
    // 检查时间戳: 如果ts >= TXN_START_ID，说明是未提交事务的undo log
    // 只应用已提交事务的undo log (ts < TXN_START_ID)
    if (undo_log.ts_ >= TXN_START_ID) {
      continue;
    }
    
    if (undo_log.is_deleted_) {
      is_deleted = true;
      continue;
    }

    std::vector<Value> values(column_count);
    
    for (uint32_t i = 0; i < column_count; i++) {
      values[i] = current_tuple.GetValue(schema, i);
    }

    std::vector<Column> modified_columns;
    std::vector<uint32_t> modified_indexes;
    
    for (uint32_t i = 0; i < column_count; i++) {
      if (undo_log.modified_fields_[i]) {
        modified_columns.push_back(schema->GetColumn(i));
        modified_indexes.push_back(i);
      }
    }
    
    Schema partial_schema(modified_columns);
    for (uint32_t i = 0; i < modified_indexes.size(); i++) {
      values[modified_indexes[i]] = undo_log.tuple_.GetValue(&partial_schema, i);
    }
    
    current_tuple = Tuple(values, schema);
    is_deleted = false;
  }

  // 如果最终状态是已删除，返回nullopt
  if (is_deleted) {
    return std::nullopt;
  }
  
  return current_tuple;
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

auto CreateUndoLog(const TupleMeta &tuple_meta, const Schema *schema, const Tuple &old_tuple, const Tuple &new_tuple) -> UndoLog {
  // 创建一个新的UndoLog对象
  UndoLog undo_log;
  undo_log.is_deleted_ = tuple_meta.is_deleted_;
  undo_log.ts_ = tuple_meta.ts_;

  // 如果是删除操作，对于delete_executor，我们会传入相同的old_tuple和new_tuple
  // 通过IsTupleContentEqual可以快速判断是否是删除操作或者完全相同的元组
  if (IsTupleContentEqual(old_tuple, new_tuple)) {
    // 对于完全相同的元组或删除操作，如果已标记为删除，则创建完整的undo log
    if (undo_log.is_deleted_) {
      // 创建完整的修改字段列表
      std::vector<bool> modified_fields(schema->GetColumnCount(), true);
      undo_log.modified_fields_ = modified_fields;
      
      // 直接使用旧元组
      undo_log.tuple_ = old_tuple;
    } else {
      // 如果元组完全相同且未删除，则不需要记录任何修改
      std::vector<bool> modified_fields(schema->GetColumnCount(), false);
      undo_log.modified_fields_ = modified_fields;
      
      // 创建空的部分元组
      Schema partial_schema({});
      std::vector<Value> values;
      undo_log.tuple_ = Tuple(values, &partial_schema);
    }
    return undo_log;
  }
  
  // 记录修改的字段
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    // 使用CompareExactlyEquals而不是CompareNotEquals来更精确地比较两个值
    if (!old_tuple.GetValue(schema, i).CompareExactlyEquals(new_tuple.GetValue(schema, i))) {
      modified_fields[i] = true;
    }
  }
  undo_log.modified_fields_ = modified_fields;
  
  // 收集修改的列创建部分Schema
  std::vector<Column> modified_columns;
  std::vector<uint32_t> modified_indexes;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (modified_fields[i]) {
      modified_columns.push_back(schema->GetColumn(i));
      modified_indexes.push_back(i);
    }
  }
  
  // 创建部分元组
  Schema partial_schema(modified_columns);
  std::vector<Value> values;
  values.reserve(modified_columns.size());
  for (uint32_t idx : modified_indexes) {
    values.push_back(old_tuple.GetValue(schema, idx));
  }
  
  undo_log.tuple_ = Tuple(values, &partial_schema);
  
  return undo_log;
}

auto MergeUndoLog(const Schema *schema, const UndoLog &old_undo_log, const UndoLog &new_undo_log) -> UndoLog {
  UndoLog merged_undo_log = old_undo_log;
  
  // 创建完整的修改字段列表
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (new_undo_log.modified_fields_[i] && !old_undo_log.modified_fields_[i]) {
      merged_undo_log.modified_fields_[i] = true;
    }
  }
  
  // 收集所有修改的列
  std::vector<Column> merged_columns;
  std::vector<uint32_t> merged_indexes;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (merged_undo_log.modified_fields_[i]) {
      merged_columns.push_back(schema->GetColumn(i));
      merged_indexes.push_back(i);
    }
  }
  
  // 创建新的部分Schema
  Schema merged_schema(merged_columns);
  std::vector<Value> values;
  values.reserve(merged_columns.size());
  
  // 优先使用旧undo_log的值，如果旧undo_log中没有修改该字段，则使用新undo_log的值
  for (uint32_t i = 0; i < merged_indexes.size(); i++) {
    uint32_t idx = merged_indexes[i];
    if (old_undo_log.modified_fields_[idx]) {
      // 从旧undo_log中获取值
      std::vector<Column> old_columns;
      std::vector<uint32_t> old_indexes;
      for (uint32_t j = 0; j < schema->GetColumnCount(); j++) {
        if (old_undo_log.modified_fields_[j]) {
          old_columns.push_back(schema->GetColumn(j));
          old_indexes.push_back(j);
        }
      }
      Schema old_schema(old_columns);
      auto it = std::find(old_indexes.begin(), old_indexes.end(), idx);
      if (it != old_indexes.end()) {
        size_t old_idx = it - old_indexes.begin();
        values.push_back(old_undo_log.tuple_.GetValue(&old_schema, old_idx));
      }
    } else if (new_undo_log.modified_fields_[idx]) {
      // 从新undo_log中获取值
      std::vector<Column> new_columns;
      std::vector<uint32_t> new_indexes;
      for (uint32_t j = 0; j < schema->GetColumnCount(); j++) {
        if (new_undo_log.modified_fields_[j]) {
          new_columns.push_back(schema->GetColumn(j));
          new_indexes.push_back(j);
        }
      }
      Schema new_schema(new_columns);
      auto it = std::find(new_indexes.begin(), new_indexes.end(), idx);
      if (it != new_indexes.end()) {
        size_t new_idx = it - new_indexes.begin();
        values.push_back(new_undo_log.tuple_.GetValue(&new_schema, new_idx));
      }
    }
  }
  
  merged_undo_log.tuple_ = Tuple(values, &merged_schema);
  merged_undo_log.prev_version_ = old_undo_log.prev_version_;
  
  return merged_undo_log;
}

auto IsWriteWriteConflict(Transaction *txn, const TupleMeta &old_meta) -> bool {
  return old_meta.ts_ > txn->GetReadTs() && old_meta.ts_ != txn->GetTransactionId();
}

}  // namespace bustub
