//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <functional>
#include <algorithm>
#include <vector>
#include <utility>
#include <optional>

#include "execution/executors/delete_executor.h"
#include "execution/execution_common.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  rids_.clear();
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  schema_ = &table_info_->schema_;
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  Tuple tuple;
  RID rid;
  // Get RIDs of tuples to be deleted
  while (child_executor_->Next(&tuple, &rid)) {
    rids_.push_back(rid);
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (rids_.empty()) {
    return false;
  }

  int count = 0;
  
  // Process all collected RIDs
  for (auto &tuple_rid : rids_) {
    // Get current tuple metadata and data
    auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(tuple_rid);
    
    // Check write-write conflict
    if (IsWriteWriteConflict(txn_, tuple_meta)) {
      txn_mgr_->SetTxnTainted(txn_);
      throw ExecutionException("Write-write conflict detected in DeleteExecutor.");
    }
    
    // Skip already deleted tuples
    if (tuple_meta.is_deleted_) {
      continue;
    }
    
    // Handle self-modification case (tuple_ts == txn_id)
    if (tuple_meta.ts_ == txn_->GetTransactionId()) {
      HandleSelfModification(tuple_rid, tuple_meta, old_tuple);
    } else {
      // Normal delete case: create new undo log and update version chain
      HandleNormalDelete(tuple_rid, tuple_meta, old_tuple);
    }
    
    // Update base tuple (mark as deleted)
    UpdateBaseTuple(tuple_rid);
    
    // Update write set and indexes
    UpdateWriteSetAndIndexes(tuple_rid, old_tuple);
    
    count++;
  }
  
  // Clear RIDs list to indicate processing is complete
  rids_.clear();
  
  // Return the number of deleted tuples
  std::vector<Value> values = {ValueFactory::GetIntegerValue(count)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

void DeleteExecutor::HandleSelfModification(RID tuple_rid, const TupleMeta &tuple_meta, const Tuple &old_tuple) {
  // Get version link
  auto old_version_link = txn_mgr_->GetVersionLink(tuple_rid);
  
  if (old_version_link.has_value() && 
      (size_t)old_version_link->prev_.prev_log_idx_ < txn_->GetUndoLogNum()) {
    // There exists an undo log from current transaction, meaning a previous UPDATE/DELETE operation
    // was performed, need to merge undo logs
    auto old_undo_log = txn_->GetUndoLog(old_version_link->prev_.prev_log_idx_);
    auto new_undo_log = CreateDeleteUndoLog(tuple_meta, schema_, old_tuple);
    auto merged_undo_log = MergeUndoLog(schema_, old_undo_log, new_undo_log);
    txn_->ModifyUndoLog(old_version_link->prev_.prev_log_idx_, merged_undo_log);
  }
  // If no undo log exists, it means this was just an INSERT operation, no need to maintain version chain
  // Just update the base tuple directly
}

void DeleteExecutor::HandleNormalDelete(RID tuple_rid, const TupleMeta &tuple_meta, const Tuple &old_tuple) {
  // Create new delete undo log
  auto new_undo_log = CreateDeleteUndoLog(tuple_meta, schema_, old_tuple);
  
  // Set previous version link if exists
  auto old_version_link = txn_mgr_->GetVersionLink(tuple_rid);
  if (old_version_link.has_value()) {
    new_undo_log.prev_version_ = old_version_link->prev_;
  }
  
  // Append undo log and update version link
  auto new_undo_link = txn_->AppendUndoLog(new_undo_log);
  txn_mgr_->UpdateVersionLink(tuple_rid, std::make_optional<VersionUndoLink>({new_undo_link, false}), nullptr);
}

void DeleteExecutor::UpdateBaseTuple(RID tuple_rid) {
  // Update base tuple, mark as deleted with current transaction ID as timestamp
  auto delete_meta = TupleMeta{txn_->GetTransactionId(), true};
  auto conflict_checker = [&](const TupleMeta &meta, const Tuple &table_tuple, RID rid) {
    return !IsWriteWriteConflict(txn_, meta);
  };
  
  // For delete, we don't need to pass the tuple data since we're just marking as deleted
  // We'll use the existing tuple data from the table
  auto [existing_meta, existing_tuple] = table_info_->table_->GetTuple(tuple_rid);
  if (!table_info_->table_->UpdateTupleInPlace(delete_meta, existing_tuple, tuple_rid, conflict_checker)) {
    txn_mgr_->SetTxnTainted(txn_);
    throw ExecutionException("Write-write conflict detected during tuple deletion.");
  }
}

void DeleteExecutor::UpdateWriteSetAndIndexes(RID tuple_rid, const Tuple &old_tuple) {
  // Update write set
  txn_->AppendWriteSet(plan_->GetTableOid(), tuple_rid);
  
  // Update indexes - delete entries for the old tuple
  for (auto &index_info : indexes_) {
    // Create a non-const copy of the tuple to call KeyFromTuple
    Tuple tuple_copy = old_tuple;
    auto old_key = tuple_copy.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(old_key, tuple_rid, txn_);
  }
}

}  // namespace bustub
