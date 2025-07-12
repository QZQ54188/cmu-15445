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

namespace tuple_reconstruction_helper{

auto GetUndoLogSchema(const UndoLog &undo_log, const Schema *schema) -> Schema{
  std::vector<Column> undo_logs_column;
  for(size_t i = 0; i < schema->GetColumnCount(); i++){
    if(undo_log.modified_fields_[i]){
      undo_logs_column.push_back(schema->GetColumn(i));
    }
  }
  return Schema(undo_logs_column);
}

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
    auto undo_logs_schema = tuple_reconstruction_helper::GetUndoLogSchema(ul, schema);
    tuple_reconstruction_helper::Modify(reconstruct_values, ul, undo_logs_schema);
  }

  if(is_deleted){
    return std::nullopt;
  }
  return Tuple(reconstruct_values, schema);
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
