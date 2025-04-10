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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // 得到管理物理页面的页面堆
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto it = table_heap_->MakeEagerIterator();
  rids_.clear();
  // 首先将所有的元组添加进去，再根据filter进行筛选
  while (!it.IsEnd()) {
    rids_.emplace_back(it.GetRID());
    ++it;
  }
  rid_it_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 当前遍历到的tuple
  std::pair<TupleMeta, Tuple> cur_tuple;
  do {
    if (rid_it_ == rids_.end()) {
      return false;
    }
    cur_tuple = table_heap_->GetTuple(*rid_it_);
    if (!(cur_tuple.first.is_deleted_)) {
      // 如果当前元组没有被逻辑删除，那么就将其传参返回
      *tuple = cur_tuple.second;
      *rid = *rid_it_;
    }
    ++rid_it_;
  } while (cur_tuple.first.is_deleted_ ||
           (plan_->filter_predicate_ && !plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()));
  return true;
}

}  // namespace bustub
