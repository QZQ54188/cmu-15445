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
#include <functional>
#include <algorithm>
#include <vector>
#include <utility>
#include <optional>

#include "execution/executors/update_executor.h"
#include "execution/execution_common.h"
#include "common/exception.h"
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
  // Get and update tuples, then insert into tuples_. 
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    tuples_.push_back({{new_values, &table_info_->schema_}, rid});
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }

  int count = 0;
  
  // 处理所有收集的元组
  for (auto &[new_tuple, tuple_rid] : tuples_) {
    // 获取当前元组的元数据和数据
    auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(tuple_rid);
    
    // 检查写写冲突
    if (IsWriteWriteConflict(txn_, tuple_meta)) {
      txn_mgr_->SetTxnTainted(txn_);
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }
    
    // 跳过已删除的元组
    if (tuple_meta.is_deleted_) {
      continue;
    }
    
    // 处理自我修改情况（tuple_ts == txn_id）
    if (tuple_meta.ts_ == txn_->GetTransactionId()) {
      HandleSelfModification(tuple_rid, tuple_meta, old_tuple, new_tuple);
    } else {
      // 正常更新情况：创建新的undo log并更新版本链
      HandleNormalUpdate(tuple_rid, tuple_meta, old_tuple, new_tuple);
    }
    
    // 更新基元组
    UpdateBaseTuple(tuple_rid, new_tuple);
    
    // 更新写集和索引
    UpdateWriteSetAndIndexes(tuple_rid, old_tuple, new_tuple);
    
    count++;
  }
  
  // 清空元组列表，表示已处理完毕
  tuples_.clear();
  
  // 返回更新的元组数量
  std::vector<Value> values = {ValueFactory::GetIntegerValue(count)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

// 私有辅助方法实现

void UpdateExecutor::HandleSelfModification(RID tuple_rid, const TupleMeta &tuple_meta, const Tuple &old_tuple, const Tuple &new_tuple) {
  // 获取版本链接
  auto old_version_link = txn_mgr_->GetVersionLink(tuple_rid);
  
  if (old_version_link.has_value() && 
      (size_t)old_version_link->prev_.prev_log_idx_ < txn_->GetUndoLogNum()) {
    // 存在当前事务的undo log，说明之前进行过UPDATE操作，需要合并undo log
    auto old_undo_log = txn_->GetUndoLog(old_version_link->prev_.prev_log_idx_);
    auto new_undo_log = CreateUndoLog(tuple_meta, schema_, old_tuple, new_tuple, txn_->GetReadTs());
    auto merged_undo_log = MergeUndoLog(schema_, old_undo_log, new_undo_log);
    txn_->ModifyUndoLog(old_version_link->prev_.prev_log_idx_, merged_undo_log);
  }
  // 如果不存在undo log，说明之前只是INSERT操作，无需维护版本链，直接更新基元组即可
}

void UpdateExecutor::HandleNormalUpdate(RID tuple_rid, const TupleMeta &tuple_meta, const Tuple &old_tuple, const Tuple &new_tuple) {
  // 创建新的undo log，使用事务的read timestamp
  auto new_undo_log = CreateUndoLog(tuple_meta, schema_, old_tuple, new_tuple, txn_->GetReadTs());
  
  // 设置前一个版本链接（如果存在）
  auto old_version_link = txn_mgr_->GetVersionLink(tuple_rid);
  if (old_version_link.has_value()) {
    new_undo_log.prev_version_ = old_version_link->prev_;
  }
  
  // 追加undo log并更新版本链接
  auto new_undo_link = txn_->AppendUndoLog(new_undo_log);
  txn_mgr_->UpdateVersionLink(tuple_rid, std::make_optional<VersionUndoLink>({new_undo_link, false}), nullptr);
}

void UpdateExecutor::UpdateBaseTuple(RID tuple_rid, const Tuple &new_tuple) {
  // 更新基元组，使用当前事务ID作为时间戳
  auto update_meta = TupleMeta{txn_->GetTransactionId(), false};
  auto conflict_checker = [&](const TupleMeta &meta, const Tuple &table_tuple, RID rid) {
    return !IsWriteWriteConflict(txn_, meta);
  };
  
  if (!table_info_->table_->UpdateTupleInPlace(update_meta, new_tuple, tuple_rid, conflict_checker)) {
    txn_mgr_->SetTxnTainted(txn_);
    throw ExecutionException("Write-write conflict detected during tuple update.");
  }
}

void UpdateExecutor::UpdateWriteSetAndIndexes(RID tuple_rid, const Tuple &old_tuple, const Tuple &new_tuple) {
  // 更新写集
  txn_->AppendWriteSet(plan_->GetTableOid(), tuple_rid);
  
  // 更新索引
  for (auto &index_info : indexes_) {
    // 创建非const的Tuple副本以调用KeyFromTuple方法
    Tuple old_tuple_copy = old_tuple;
    Tuple new_tuple_copy = new_tuple;
    
    auto old_key = old_tuple_copy.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
    auto new_key = new_tuple_copy.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(old_key, tuple_rid, txn_);
    index_info->index_->InsertEntry(new_key, tuple_rid, txn_);
  }
}

}  // namespace bustub
