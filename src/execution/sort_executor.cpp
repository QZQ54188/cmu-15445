#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void SortExecutor::Init() {
  sort_res_.clear();
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    sort_res_.emplace_back(child_tuple);
  }
  std::sort(sort_res_.begin(), sort_res_.end(), [this](const Tuple &t1, const Tuple &t2) -> bool {
    auto schema = GetOutputSchema();
    for (const auto &order_by : plan_->GetOrderBy()) {
      auto v1 = order_by.second->Evaluate(&t1, schema);
      auto v2 = order_by.second->Evaluate(&t2, schema);
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      if (order_by.first == OrderByType::DESC) {
        return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
      }
    }
    return false;
  });
  sort_it_ = sort_res_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sort_it_ == sort_res_.end()) {
    return false;
  }
  *tuple = *sort_it_;
  *rid = tuple->GetRid();
  ++sort_it_;
  return true;
}

}  // namespace bustub
