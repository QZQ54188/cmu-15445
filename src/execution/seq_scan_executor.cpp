//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  transaction_ = exec_ctx->GetTransaction();
  txn_mgr_ = exec_ctx->GetTransactionManager();
}

void SeqScanExecutor::Init() {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info->table_.get();
  schema_ = &table_info->schema_;

  iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());

  auto it = table_heap_->MakeEagerIterator();
  rids_.clear();
  while (!it.IsEnd()) {
    rids_.emplace_back(it.GetRID());
    ++it;
  }
  rid_it_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto &&[meta, t] = iter_->GetTuple();
    std::optional<Tuple> recon_tuple = std::nullopt;

    if (transaction_->GetReadTs() >= meta.ts_ || transaction_->GetTransactionId() == meta.ts_) {
      if (!meta.is_deleted_) {  // If the newest data is deleted, skip it.
        recon_tuple = std::make_optional(t);
      }
    } else {
      // Case 3: iterate the version chain to collect all undo logs after the read timestamp,
      // and recover the past version of the tuple.
      std::vector<UndoLog> undo_logs{};
      std::optional<UndoLink> undo_link = txn_mgr_->GetUndoLink(t.GetRid());
      // Collect undo logs.
      CollectUndoLogs(undo_logs, undo_link);
      // If no undo logs, means tuple is uncommitted or newer than the read timestamp, continue. 
      if(!undo_logs.empty()){
        // 只有当最后一个undo log的时间戳小于等于事务的读时间戳时，才重构元组
        if (transaction_->GetReadTs() >= undo_logs.back().ts_) {
          recon_tuple = ReconstructTuple(schema_, t, meta, undo_logs);
        }
      }
    }

    // If recon_tuple has value, output; otherwise, keep looping.
    if (recon_tuple.has_value()) {
      if (plan_->filter_predicate_ == nullptr || plan_->filter_predicate_->Evaluate(&recon_tuple.value(), *schema_).GetAs<bool>()) {
        *tuple = std::move(recon_tuple.value());
        *rid = iter_->GetRID();
        ++(*iter_);
        return true;
      }
    }

    ++(*iter_);
  }

  return false;
}

void SeqScanExecutor::CollectUndoLogs(std::vector<UndoLog> &undo_logs, std::optional<UndoLink> &undo_link) {
  std::vector<UndoLog> temp_undo_logs;
  
  // 首先收集所有符合条件的undo logs
  while (undo_link.has_value() && undo_link->prev_txn_ != INVALID_TXN_ID) {
    std::optional<UndoLog> undo_log = txn_mgr_->GetUndoLogOptional(undo_link.value());
    if (!undo_log.has_value()) {
      // Dangling pointer, made when garbage collection.
      break;
    }
    
    // 只收集已提交事务的undo logs或当前事务自己的undo logs
    // 1. 已提交事务: ts < TXN_START_ID 且 ts <= transaction_->GetReadTs()
    // 2. 当前事务: prev_txn_ == transaction_->GetTransactionId()
    if ((undo_log->ts_ < TXN_START_ID && undo_log->ts_ <= transaction_->GetReadTs()) || 
        undo_link->prev_txn_ == transaction_->GetTransactionId()) {
      temp_undo_logs.push_back(undo_log.value());
    }
    
    undo_link = undo_log->prev_version_;
  }
  
  // 将收集到的undo logs添加到结果中，确保顺序正确（从旧到新）
  undo_logs.insert(undo_logs.end(), temp_undo_logs.rbegin(), temp_undo_logs.rend());
}

}  // namespace bustub
