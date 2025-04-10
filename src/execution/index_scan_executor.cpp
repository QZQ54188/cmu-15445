//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/extendible_hash_table_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  is_scaned_ = false;
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());

  table_heap_ = table_info->table_.get();
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  Tuple index_key({plan_->pred_key_->val_}, &index_info->key_schema_);
  rids_.clear();
  htable_->ScanKey(index_key, &rids_, GetExecutorContext()->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_scaned_) {
    return false;
  }
  is_scaned_ = true;
  if (rids_.empty()) {
    return false;
  }
  auto cur_tuple = table_heap_->GetTuple(rids_.front());
  if (!cur_tuple.first.is_deleted_) {
    *tuple = cur_tuple.second;
    *rid = rids_.front();
  }
  return true;
}

}  // namespace bustub
