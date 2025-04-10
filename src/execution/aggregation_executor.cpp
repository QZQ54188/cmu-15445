//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();
  empty_table_processed_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->aht_.Begin() == this->aht_.End()) {
    // 哈希表为空的话查询计划的Groupsby一定为空
    if (!this->plan_->GetGroupBys().empty()) {
      return false;
    }
    // 如果已经处理过空表，直接返回 false
    if (empty_table_processed_) {
      return false;
    }
    std::vector<Value> res_values;
    for (auto &aggregate : aht_.GenerateInitialAggregateValue().aggregates_) {
      res_values.emplace_back(aggregate);
    }
    *tuple = Tuple{res_values, &GetOutputSchema()};
    *rid = tuple->GetRid();
    empty_table_processed_ = true;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> res_values{};
  res_values.reserve(aht_iterator_.Key().group_bys_.size() + aht_iterator_.Val().aggregates_.size());
  for (const auto &group_by : aht_iterator_.Key().group_bys_) {
    res_values.emplace_back(group_by);
  }
  for (const auto &aggregate : aht_iterator_.Val().aggregates_) {
    res_values.emplace_back(aggregate);
  }
  *tuple = Tuple{res_values, &GetOutputSchema()};
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
