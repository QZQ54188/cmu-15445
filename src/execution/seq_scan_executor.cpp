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
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
  schema_ = &table_info->schema_;
  table_heap_ = table_info->table_.get();
  transaction_ = exec_ctx->GetTransaction();
  txn_mgr_ = exec_ctx->GetTransactionManager();
}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto [meta, t] = iter_->GetTuple();
    std::optional<Tuple> recon_tuple = std::nullopt;

    // 检查当前版本是否可见
    if (transaction_->GetReadTs() >= meta.ts_ || transaction_->GetTransactionId() == meta.ts_) {
      if (!meta.is_deleted_) {
        recon_tuple = std::make_optional(t);
      }
    } else {
      // 查找历史版本
      std::vector<UndoLog> undo_logs{};
      auto undo_link = txn_mgr_->GetUndoLink(t.GetRid());
      CollectUndoLogs(undo_logs, undo_link);
      
      if (!undo_logs.empty() && transaction_->GetReadTs() >= undo_logs.back().ts_) {
        recon_tuple = ReconstructTuple(schema_, t, meta, undo_logs);
      }
    }

    // 检查过滤条件并返回结果
    if (recon_tuple.has_value()) {
      if (plan_->filter_predicate_ == nullptr || 
          plan_->filter_predicate_->Evaluate(&recon_tuple.value(), *schema_).GetAs<bool>()) {
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

void SeqScanExecutor::CollectUndoLogs(std::vector<UndoLog> &undo_logs, std::optional<UndoLink> &undo_link){
  while(undo_link.has_value() && undo_link->prev_txn_ != INVALID_TXN_ID){
    std::optional<UndoLog> undo_log = txn_mgr_->GetUndoLogOptional(undo_link.value());
    if (!undo_log.has_value()) {
      // Dangling pointer, made when garbage collection.
      break;
    }
    undo_logs.push_back(undo_log.value());
    if (transaction_->GetReadTs() >= undo_log->ts_) {
      break;
    }
    undo_link = undo_log->prev_version_;
  }
}

}  // namespace bustub
