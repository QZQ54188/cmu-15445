//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  tuples_.clear();
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  schema_ = &table_info_->schema_;
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  Tuple tuple;
  RID rid;
  // 获取所有需要更新的元组，并计算更新后的值
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    tuples_.push_back({{new_values, schema_}, rid});
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  
  // 处理所有待更新的元组
  while (!tuples_.empty()) {
    // 取出待更新的元组和RID
    auto &new_tuple = tuples_.back().first;
    auto update_rid = tuples_.back().second;

    // 获取原始元组和元数据
    auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(update_rid);
    
    // 创建新的Undo日志
    auto new_undo_log = CreateUndoLog(tuple_meta, schema_, old_tuple, new_tuple);

    // 更新数据，检查是否有写写冲突
    auto conflict_checker = [&](const TupleMeta &meta, const Tuple &tuple, RID rid) {
      return !IsWriteWriteConflict(txn_, meta);
    };
    
    // 尝试更新元组，如果存在冲突则抛出异常
    if (!table_info_->table_->UpdateTupleInPlace({txn_->GetTransactionId(), false}, new_tuple, update_rid, conflict_checker)) {
      // 设置事务状态为TAINTED，并抛出异常
      txn_mgr_->SetTxnTainted(txn_);
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }
    
    // 更新成功，将更新操作加入到事务的写集合中
    txn_->AppendWriteSet(plan_->GetTableOid(), update_rid);
    
    // 更新相关索引
    for (auto &index_info : indexes_) {
      auto old_key = old_tuple.KeyFromTuple(*schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(*schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, update_rid, txn_);
      index_info->index_->InsertEntry(new_key, update_rid, txn_);
    }

    // 更新Undo日志
    auto old_version_link = txn_mgr_->GetVersionLink(update_rid);
    if (tuple_meta.ts_ == txn_->GetTransactionId()) {
      // 如果元组已经被当前事务修改过，需要合并新旧Undo日志
      if (old_version_link.has_value() && old_version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
        auto old_undo_log = txn_->GetUndoLog(old_version_link->prev_.prev_log_idx_);
        auto merged_undo_log = MergeUndoLog(schema_, old_undo_log, new_undo_log);
        txn_->ModifyUndoLog(old_version_link->prev_.prev_log_idx_, merged_undo_log);
      }
    } else {
      // 如果元组未被当前事务修改过，则添加新的Undo日志并更新版本链
      if (old_version_link.has_value()) {
        new_undo_log.prev_version_ = old_version_link->prev_;
      }
      auto new_undo_link = txn_->AppendUndoLog(new_undo_log);
      txn_mgr_->UpdateVersionLink(update_rid, std::make_optional<VersionUndoLink>({new_undo_link, false}), nullptr);
    }
    
    count++;
    tuples_.pop_back();
  }

  // 返回更新的元组数量
  std::vector<Value> v = {{TypeId::INTEGER, count}};
  *tuple = Tuple(v, &GetOutputSchema());
  return count > 0;
}

}  // namespace bustub
