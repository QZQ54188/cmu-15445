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

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  is_updated_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_updated_) {
    return false;
  }
  is_updated_ = true;

  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tuple{};
  RID child_rid{};
  int cnt = 0;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    cnt++;
    // 将元组标记为逻辑删除
    table_info_->table_->UpdateTupleMeta({0, true}, child_rid);
    std::vector<Value> new_values{};
    // 计算新元组对应更新后的值，仿照projection执行器的代码
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.emplace_back(expr->Evaluate(&child_tuple, table_info_->schema_));
    }
    auto new_tuple = Tuple{new_values, &table_info_->schema_};
    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple).value();
    for (auto &index_info : indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      index->DeleteEntry(old_key, child_rid, GetExecutorContext()->GetTransaction());
      index->InsertEntry(new_key, new_rid, GetExecutorContext()->GetTransaction());
    }
  }
  *tuple = Tuple{{{TypeId::INTEGER, cnt}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
