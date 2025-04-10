#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void TopNExecutor::Init() {
  topn_res_.clear();
  heap_size_ = 0;
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  std::priority_queue<Tuple, std::vector<Tuple>, std::function<bool(const Tuple &, const Tuple &)>> heap{
      [this](const Tuple &t1, const Tuple &t2) -> bool {
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
      }};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    if (heap.size() < plan_->GetN()) {
      heap.emplace(child_tuple);
      heap_size_++;
    } else {
      heap.emplace(child_tuple);
      heap.pop();
    }
  }
  while (!heap.empty()) {
    topn_res_.emplace(topn_res_.begin(), heap.top());
    heap.pop();
  }
  topn_it_ = topn_res_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (topn_it_ == topn_res_.end()) {
    return false;
  }
  *tuple = *topn_it_;
  *rid = tuple->GetRid();
  ++topn_it_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_size_; };

}  // namespace bustub
