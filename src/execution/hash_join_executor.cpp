//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  jht_.Clear();
  left_child_->Init();
  right_child_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (right_child_->Next(&child_tuple, &child_rid)) {
    jht_.InsertCombine(GetRightJoinKey(child_tuple), child_tuple);
  }
  if (plan_->join_type_ == JoinType::LEFT) {
    while (left_child_->Next(&child_tuple, &child_rid)) {
      auto left_hash_key = GetLeftJoinKey(child_tuple);
      std::vector<Tuple> right_tuples;
      if (jht_.GetValue(left_hash_key, &right_tuples)) {
        // 如果找到左表与右表哈希值一样的key，就合并
        std::vector<Value> cur_tuple_value;
        for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(child_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (const auto &tuple : right_tuples) {
          std::vector<Value> join_tuple_value = cur_tuple_value;
          for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
            join_tuple_value.emplace_back(tuple.GetValue(&right_child_->GetOutputSchema(), i));
          }
          left_join_res_.emplace(join_tuple_value, &GetOutputSchema());
        }
      } else {
        std::vector<Value> cur_tuple_value;
        for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(child_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(
              ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
        }
        left_join_res_.emplace(cur_tuple_value, &GetOutputSchema());
      }
    }
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    while (left_child_->Next(&child_tuple, &child_rid)) {
      auto left_hash_key = GetLeftJoinKey(child_tuple);
      std::vector<Tuple> right_tuples;
      if (jht_.GetValue(left_hash_key, &right_tuples)) {
        std::vector<Value> cur_tuple_value;
        for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(child_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (const auto &tuple : right_tuples) {
          std::vector<Value> join_tuple_value = cur_tuple_value;
          for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
            join_tuple_value.emplace_back(tuple.GetValue(&right_child_->GetOutputSchema(), i));
          }
          inner_join_res_.emplace(join_tuple_value, &GetOutputSchema());
        }
      }
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    if (left_join_res_.empty()) {
      return false;
    }
    *tuple = left_join_res_.front();
    left_join_res_.pop();
    return true;
  }
  if (plan_->GetJoinType() == JoinType::INNER) {
    if (inner_join_res_.empty()) {
      return false;
    }
    *tuple = inner_join_res_.front();
    inner_join_res_.pop();
    return true;
  }
  return false;
}

}  // namespace bustub
