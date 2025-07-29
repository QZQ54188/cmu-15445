//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
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

#include "execution/executors/update_executor.h"
#include "execution/execution_common.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  tuples_.clear();
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  schema_ = &table_info_->schema_;
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  Tuple tuple;
  RID rid;
  // Get and update tuples, then insert into tuples_. 
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    tuples_.push_back({{new_values, &table_info_->schema_}, rid});
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  if (!tuples_.empty()) {
    auto &new_tuple = tuples_.back().first;
    auto rid = tuples_.back().second;

    // Create new undo_log.
    auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(rid);
    auto new_undo_log = CreateUndoLog(tuple_meta, schema_, old_tuple, new_tuple);

    // Update data.
    auto conflictChecker = [&](const TupleMeta &meta, const Tuple &table, RID rid) {
      return !IsWriteWriteConflict(txn_, meta);
    };
    if (!table_info_->table_->UpdateTupleInPlace({txn_->GetTransactionId(), false}, new_tuple, rid, conflictChecker)) {
      // Checker fail, set txn state to TAINTED, and throw an ExecutionException.
      txn_mgr_->SetTxnTainted(txn_);
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), rid);
    // Update indexes.
    for (auto &index_info : indexes_) {
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid, txn_);
      index_info->index_->InsertEntry(new_key, rid, txn_);
    }

    // Update undo log.
    auto old_version_link = txn_mgr_->GetVersionLink(rid);
    if (tuple_meta.ts_ == txn_->GetTransactionId()) {
      // Has been updated by current transaction, merge old and new undo_log, then update undo_log.
      // If no old_undo_log, skip merged and modify, because its the first tuple value.
      if (old_version_link.has_value() && (size_t) old_version_link->prev_.prev_log_idx_ < txn_->GetUndoLogNum()) {
        auto old_undo_log = txn_->GetUndoLog(old_version_link->prev_.prev_log_idx_);
        auto merged_undo_log = MergeUndoLog(schema_, old_undo_log, new_undo_log);
        txn_->ModifyUndoLog(old_version_link->prev_.prev_log_idx_, merged_undo_log);
      }
    } else {
      // Not updated by current transaction, append new undo log and update version link.
      if (old_version_link.has_value()) {
        new_undo_log.prev_version_ = old_version_link->prev_;
      }
      auto new_undo_link = txn_->AppendUndoLog(new_undo_log);
      txn_mgr_->UpdateVersionLink(rid, std::make_optional<VersionUndoLink>({new_undo_link, false}), nullptr);
      count += 1;
      tuples_.pop_back();
    }
  }

  std::vector<Value> v = {ValueFactory::GetIntegerValue(count)};
  *tuple = Tuple(v, &GetOutputSchema());
  return count >= 1;
}

}  // namespace bustub
