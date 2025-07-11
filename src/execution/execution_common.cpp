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

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
