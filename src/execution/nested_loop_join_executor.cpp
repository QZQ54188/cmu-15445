//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  std::vector<Tuple> left_tuples{};
  // 只读取左表的所有tuple
  while (left_executor_->Next(&child_tuple, &child_rid)) {
    left_tuples.emplace_back(child_tuple);
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (auto &left_tuple : left_tuples) {
      bool join_succeed = false;
      // 对每个左表tuple，初始化一次右执行器
      right_executor_->Init();
      while (right_executor_->Next(&child_tuple, &child_rid)) {
        auto join_value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &child_tuple,
                                                          right_executor_->GetOutputSchema());
        if (!join_value.IsNull() && join_value.GetAs<bool>()) {
          // 如果匹配上了，就合并
          join_succeed = true;
          std::vector<Value> cur_tuple_value;
          for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            cur_tuple_value.emplace_back(child_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
          }
          left_join_res_.emplace(cur_tuple_value, &GetOutputSchema());
        }
      }
      if (!join_succeed) {
        // 没匹配上的话就将右表缺少的部分按照NULL填充
        std::vector<Value> cur_tuple_value;
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          cur_tuple_value.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        left_join_res_.emplace(cur_tuple_value, &GetOutputSchema());
      }
    }
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    for (auto &left_tuple : left_tuples) {
      // 对每个左表tuple，初始化一次右执行器
      right_executor_->Init();
      while (right_executor_->Next(&child_tuple, &child_rid)) {
        auto join_value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &child_tuple,
                                                          right_executor_->GetOutputSchema());
        if (!join_value.IsNull() && join_value.GetAs<bool>()) {
          std::vector<Value> cur_tuple_value;
          for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            cur_tuple_value.emplace_back(child_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
          }
          inner_join_res_.emplace(cur_tuple_value, &GetOutputSchema());
        }
      }
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
