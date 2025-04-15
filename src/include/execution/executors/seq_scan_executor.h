//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"
#include "storage/table/table_iterator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** 收集与当前事务相关的撤销日志 */
  void CollectUndoLogs(std::vector<UndoLog> &undo_logs, std::optional<UndoLink> &undo_link);
  
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  TableHeap *table_heap_;
  
  /** MVCC 相关成员 */
  Transaction *transaction_;
  TransactionManager *txn_mgr_;
  std::unique_ptr<TableIterator> iter_;
  const Schema *schema_;
  
  /** 原始成员，保留以便兼容 */
  std::vector<RID> rids_;              // 由于RID唯一标识一个tuple，rids_存放符合条件的元组
  std::vector<RID>::iterator rid_it_;  // 用于顺序遍历tuple的迭代器
};
}  // namespace bustub
