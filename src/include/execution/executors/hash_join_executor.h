//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> keys_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

// 在std命名空间中为HashJoinKey添加hash特化
namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleHashJoinHashTable {
 public:
  /**
   * Default constructor
   */
  SimpleHashJoinHashTable() = default;

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param join_key the key to be inserted
   * @param tuple the value to be inserted
   */
  void InsertCombine(const HashJoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> join_val;
      join_val.emplace_back(tuple);
      ht_.insert({join_key, join_val});
    } else {
      ht_.at(join_key).emplace_back(tuple);
    }
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  auto GetValue(const HashJoinKey &key, std::vector<Tuple> *res_values) -> bool {
    if (auto find_it = ht_.find(key); find_it != ht_.end()) {
      *res_values = find_it->second;
      return true;
    }
    return false;
  }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetLeftJoinKey(const Tuple &tuple) -> HashJoinKey {
    std::vector<Value> res_values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      res_values.emplace_back(expr->Evaluate(&tuple, left_child_->GetOutputSchema()));
    }
    return HashJoinKey{res_values};
  }

  auto GetRightJoinKey(const Tuple &tuple) -> HashJoinKey {
    std::vector<Value> res_values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      res_values.emplace_back(expr->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    return HashJoinKey{res_values};
  }
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::queue<Tuple> left_join_res_;
  std::queue<Tuple> inner_join_res_;
  SimpleHashJoinHashTable jht_;
};

}  // namespace bustub
