#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 仿照merge_filter优化器即可
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto seq_plan = dynamic_cast<SeqScanPlanNode &>(*optimized_plan);
    // 顺序扫描优化为index扫描必须要有过滤条件
    if (auto pred = seq_plan.filter_predicate_; pred) {
      auto indexes = catalog_.GetTableIndexes(seq_plan.table_name_);
      // 必须要有索引
      if (!indexes.empty()) {
        auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(pred);
        auto equal_expr = std::dynamic_pointer_cast<ComparisonExpression>(pred);
        // 必须是v1 = 1类型的过滤条件
        if (!logic_expr && equal_expr && equal_expr->comp_type_ == ComparisonType::Equal) {
          auto target_index = dynamic_cast<ColumnValueExpression &>(*equal_expr->GetChildAt(0)).GetColIdx();
          for (auto *index_info : indexes) {
            auto key_attr = index_info->index_->GetKeyAttrs();
            if (key_attr.front() == target_index) {
              if (auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(equal_expr->GetChildAt(1));
                  pred_key != nullptr) {
                return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.table_oid_,
                                                           index_info->index_oid_, pred, pred_key.get());
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
