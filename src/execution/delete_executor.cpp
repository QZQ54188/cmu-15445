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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  is_deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 删除计划只可以执行一次
  if (is_deleted_) {
    return false;
  }
  is_deleted_ = true;

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  // 表示当前删除元组的行数
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    cnt++;
    // 只需要进行逻辑删除
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    for (auto index_info : indexes) {
      auto key = tuple->KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, GetExecutorContext()->GetTransaction());
    }
  }
  *tuple = Tuple{{{TypeId::INTEGER, cnt}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
