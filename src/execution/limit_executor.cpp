//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void LimitExecutor::Init() {
  limit_res_.clear();
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  size_t i = 0;
  size_t limit = plan_->GetLimit();
  while (i < limit && child_executor_->Next(&child_tuple, &child_rid)) {
    i++;
    limit_res_.emplace_back(child_tuple);
  }
  limit_it_ = limit_res_.begin();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (limit_it_ == limit_res_.end()) {
    return false;
  }
  *tuple = *limit_it_;
  *rid = tuple->GetRid();
  ++limit_it_;
  return true;
}

}  // namespace bustub
