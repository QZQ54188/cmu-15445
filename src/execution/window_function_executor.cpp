#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  // 清空之前的数据
  child_executor_->Init();
  result_set_.clear();
  sorted_tuples_.clear();
  partition_results_.clear();

  // 收集所有子执行器的tuple
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.emplace_back(tuple);
  }

  // 处理每个窗口函数
  for (const auto &[col_idx, window_func] : plan_->window_functions_) {
    std::unordered_map<AggregateKey, std::vector<std::pair<size_t, AggregateValue>>> partition_map;

    // 首先按ORDER BY子句排序（如果存在）
    if (!window_func.order_by_.empty()) {
      // 创建一个局部引用以便lambda可以捕获它
      const auto &order_by_list = window_func.order_by_;
      std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
                [this, &order_by_list](const Tuple &t1, const Tuple &t2) -> bool {
                  const auto &schema = child_executor_->GetOutputSchema();
                  for (const auto &order_by : order_by_list) {
                    const auto &expr = order_by.second;
                    auto v1 = expr->Evaluate(&t1, schema);
                    auto v2 = expr->Evaluate(&t2, schema);
                    if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
                      continue;
                    }
                    if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
                      return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
                    }
                    return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
                  }
                  return false;
                });
    }

    // 对于每个元组，计算其分区键
    for (size_t i = 0; i < sorted_tuples_.size(); i++) {
      const auto &tuple = sorted_tuples_[i];

      // 创建分区键
      std::vector<Value> partition_values;
      for (const auto &expr : window_func.partition_by_) {
        partition_values.emplace_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      }
      AggregateKey partition_key{partition_values};

      // 创建聚合值
      std::vector<Value> agg_values;
      if (window_func.function_ != nullptr) {
        agg_values.emplace_back(window_func.function_->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      } else {
        // 对于COUNT(*)，我们不需要表达式
        agg_values.emplace_back(ValueFactory::GetIntegerValue(1));
      }
      AggregateValue agg_value{agg_values};

      // 添加到分区映射
      partition_map[partition_key].emplace_back(i, agg_value);
    }

    // 进行窗口函数计算
    std::vector<Value> result_values(sorted_tuples_.size());

    for (auto &[partition_key, tuples_with_agg] : partition_map) {
      // 对每个分区进行计算

      if (window_func.type_ == WindowFunctionType::Rank) {
        // 为RANK窗口函数的特殊处理
        int32_t current_rank = 1;
        int32_t duplicated = 0;

        // 前一个元组的ORDER BY值
        std::vector<Value> prev_order_by_values;
        bool first_tuple = true;

        // 保存order_by_list的引用
        const auto &order_by_list = window_func.order_by_;

        for (const auto &[tuple_idx, _] : tuples_with_agg) {
          const auto &tuple = sorted_tuples_[tuple_idx];

          // 获取当前元组的ORDER BY值
          std::vector<Value> current_order_by_values;
          current_order_by_values.reserve(order_by_list.size());
          for (const auto &order_by : order_by_list) {
            current_order_by_values.emplace_back(order_by.second->Evaluate(&tuple, child_executor_->GetOutputSchema()));
          }

          if (first_tuple) {
            // 第一个元组的排名是1
            result_values[tuple_idx] = ValueFactory::GetIntegerValue(current_rank);
            prev_order_by_values = current_order_by_values;
            first_tuple = false;
          } else {
            // 检查是否与前一个元组的ORDER BY值相同
            bool is_equal = true;
            for (size_t j = 0; j < current_order_by_values.size(); j++) {
              if (current_order_by_values[j].CompareEquals(prev_order_by_values[j]) != CmpBool::CmpTrue) {
                is_equal = false;
                break;
              }
            }

            if (is_equal) {
              // 如果相同，排名与前一个元组相同
              result_values[tuple_idx] = ValueFactory::GetIntegerValue(current_rank);
              duplicated++;
            } else {
              // 如果不同，排名增加
              current_rank += duplicated + 1;
              duplicated = 0;
              result_values[tuple_idx] = ValueFactory::GetIntegerValue(current_rank);
              prev_order_by_values = current_order_by_values;
            }
          }
        }
      } else {
        // 处理聚合函数
        // 保存order_by_list的引用
        const auto &order_by_list = window_func.order_by_;

        // 对每个元组计算窗口聚合结果
        size_t i = 0;
        for (auto &[tuple_idx, _] : tuples_with_agg) {
          // 根据窗口函数类型进行初始化
          Value result_value;
          switch (window_func.type_) {
            case WindowFunctionType::CountStarAggregate:
              result_value = ValueFactory::GetIntegerValue(0);
              break;
            case WindowFunctionType::CountAggregate:
            case WindowFunctionType::SumAggregate:
            case WindowFunctionType::MinAggregate:
            case WindowFunctionType::MaxAggregate:
              result_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
              break;
            default:
              break;
          }

          // 根据窗口框架计算聚合值
          // 如果没有ORDER BY子句，窗口框架是整个分区
          // 如果有ORDER BY子句，窗口框架是从分区开始到当前行
          size_t frame_end = order_by_list.empty() ? tuples_with_agg.size() : i + 1;

          for (size_t j = 0; j < frame_end; j++) {
            const auto &[_, agg_value] = tuples_with_agg[j];
            const auto &value = agg_value.aggregates_[0];

            switch (window_func.type_) {
              case WindowFunctionType::CountStarAggregate:
                result_value = result_value.Add(ValueFactory::GetIntegerValue(1));
                break;
              case WindowFunctionType::CountAggregate:
                if (!value.IsNull()) {
                  result_value = result_value.IsNull() ? ValueFactory::GetIntegerValue(1)
                                                       : result_value.Add(ValueFactory::GetIntegerValue(1));
                }
                break;
              case WindowFunctionType::SumAggregate:
                if (!value.IsNull()) {
                  result_value = result_value.IsNull() ? value : result_value.Add(value);
                }
                break;
              case WindowFunctionType::MinAggregate:
                if (!value.IsNull()) {
                  result_value = result_value.IsNull()
                                     ? value
                                     : (result_value.CompareLessThan(value) == CmpBool::CmpTrue ? result_value : value);
                }
                break;
              case WindowFunctionType::MaxAggregate:
                if (!value.IsNull()) {
                  result_value =
                      result_value.IsNull()
                          ? value
                          : (result_value.CompareGreaterThan(value) == CmpBool::CmpTrue ? result_value : value);
                }
                break;
              default:
                break;
            }
          }

          result_values[tuple_idx] = result_value;
          i++;
        }
      }
    }

    // 存储此窗口函数的结果
    partition_results_[col_idx] = std::move(result_values);
  }

  // 准备结果集
  for (const auto &tuple : sorted_tuples_) {
    std::vector<Value> values;

    // 添加普通列
    for (size_t i = 0; i < plan_->columns_.size(); i++) {
      if (plan_->window_functions_.find(i) != plan_->window_functions_.end()) {
        // 这是一个窗口函数列，使用预计算的值
        values.emplace_back(partition_results_[i][result_set_.size()]);
      } else {
        // 这是一个普通列
        values.emplace_back(plan_->columns_[i]->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      }
    }

    // 创建输出元组
    result_set_.emplace_back(Tuple(values, &GetOutputSchema()));
  }

  // 重置结果集迭代器
  result_idx_ = 0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_idx_ >= result_set_.size()) {
    return false;
  }

  *tuple = result_set_[result_idx_];
  *rid = tuple->GetRid();
  result_idx_++;

  return true;
}

}  // namespace bustub
