#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // 处理等值连接条件
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;

    if (ExtractJoinKeys(nlj_plan.Predicate(), &left_key_expressions, &right_key_expressions)) {
      // 如果成功提取出连接键，创建HashJoinPlanNode
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

// 递归提取连接键
auto Optimizer::ExtractJoinKeys(const AbstractExpressionRef &expr,
                                std::vector<AbstractExpressionRef> *left_key_expressions,
                                std::vector<AbstractExpressionRef> *right_key_expressions) -> bool {
  // 处理AND逻辑表达式，递归处理每个子表达式
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    // 处理AND表达式的左右子表达式
    bool left_success = ExtractJoinKeys(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions);
    bool right_success = ExtractJoinKeys(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions);
    return left_success && right_success;
  }

  // 处理等值比较表达式
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
    // 检查是否是两个列值表达式的比较
    const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[0].get());
    const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get());

    if (left_expr != nullptr && right_expr != nullptr) {
      // 检查表达式是否分别来自左右表
      if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
        left_key_expressions->emplace_back(comp_expr->children_[0]);
        right_key_expressions->emplace_back(comp_expr->children_[1]);
        return true;
      }
      if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
        left_key_expressions->emplace_back(comp_expr->children_[1]);
        right_key_expressions->emplace_back(comp_expr->children_[0]);
        return true;
      }
    }
  }

  // 无法处理的情况
  return false;
}

}  // namespace bustub
