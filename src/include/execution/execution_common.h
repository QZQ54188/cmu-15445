#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

/**
 * 创建UndoLog用于记录更新操作
 * @param tuple_meta 原始元组的元数据
 * @param schema 表结构
 * @param old_tuple 原始元组
 * @param new_tuple 更新后的元组
 * @return 创建的UndoLog
 */
auto CreateUndoLog(const TupleMeta &tuple_meta, const Schema *schema, const Tuple &old_tuple, const Tuple &new_tuple) 
    -> UndoLog;

/**
 * 合并新旧UndoLog
 * @param schema 表结构
 * @param old_undo_log 旧的UndoLog
 * @param new_undo_log 新的UndoLog
 * @return 合并后的UndoLog，旧的UndoLog有更高优先级
 */
auto MergeUndoLog(const Schema *schema, const UndoLog &old_undo_log, const UndoLog &new_undo_log) -> UndoLog;

/**
 * 判断是否存在写写冲突
 * @param txn 当前事务
 * @param old_meta 元组元数据
 * @return 是否存在写写冲突
 */
auto IsWriteWriteConflict(Transaction *txn, const TupleMeta &old_meta) -> bool;

}  // namespace bustub
