//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();
  is_inserted_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // insert计划只可以执行一次
  if (is_inserted_) {
    return false;
  }
  is_inserted_ = true;

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

  int cnt = 0;  // 记录插入的行数
  while (child_executor_->Next(tuple, rid)) {
    cnt++;
    TupleMeta tuple_meta{0, false};
    auto result = table_info->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(),
                                                  exec_ctx_->GetTransaction(), table_info->oid_);
    if (!result.has_value()) {
      // 没有插入成功的情况下
      continue;
    }
    for (auto index_info : indexes) {
      // 更新所有索引表中的记录
      auto key = tuple->KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, result.value(), exec_ctx_->GetTransaction());
    }
  }
  *tuple = Tuple{{{TypeId::INTEGER, cnt}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
