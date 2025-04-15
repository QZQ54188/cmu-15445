//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  is_deleted_ = false;
  to_delete_rids_.clear();
  
  // 初始化获取事务和事务管理器
  txn_ = GetExecutorContext()->GetTransaction();
  txn_mgr_ = GetExecutorContext()->GetTransactionManager();
  
  // 获取表信息和索引
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  
  // 先收集所有要删除的元组
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    to_delete_rids_.push_back({tuple, rid});
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  is_deleted_ = true;

  int cnt = 0;
  
  while (!to_delete_rids_.empty()) {
    auto &current = to_delete_rids_.back();
    auto &delete_tuple = current.first;
    auto delete_rid = current.second;
    
    auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(delete_rid);
    
    // 创建新的Undo日志
    auto new_undo_log = CreateUndoLog(tuple_meta, &table_info_->schema_, old_tuple, old_tuple);
    new_undo_log.is_deleted_ = true;  // 标记为删除操作
    new_undo_log.tuple_ = old_tuple;  // 确保完整的元组信息被保存
    
    // 设置正确的时间戳
    // 使用TXN_START_ID + txn_id作为时间戳，表示该tuple被当前事务修改
    new_undo_log.ts_ = TXN_START_ID + txn_->GetTransactionId();

    // 更新元组元数据
    table_info_->table_->UpdateTupleMeta({txn_->GetTransactionId(), true}, delete_rid);

    // 手动检查写写冲突
    if (IsWriteWriteConflict(txn_, tuple_meta)) {
      // 设置事务状态为TAINTED，并抛出异常
      txn_mgr_->SetTxnTainted(txn_);
      throw ExecutionException("Write-write conflict detected in DeleteExecutor.");
    }
    
    // 更新成功，将删除操作加入到事务的写集合中
    txn_->AppendWriteSet(plan_->GetTableOid(), delete_rid);
    
    // 删除相关索引
    for (auto &index_info : indexes_) {
      auto key = delete_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, delete_rid, txn_);
    }

    // 更新Undo日志
    auto old_version_link = txn_mgr_->GetVersionLink(delete_rid);
    if (tuple_meta.ts_ == txn_->GetTransactionId()) {
      // 如果元组已经被当前事务修改过，需要合并新旧Undo日志
      if (old_version_link.has_value() && (size_t) old_version_link->prev_.prev_log_idx_ < txn_->GetUndoLogNum()) {
        auto old_undo_log = txn_->GetUndoLog(old_version_link->prev_.prev_log_idx_);
        auto merged_undo_log = MergeUndoLog(&table_info_->schema_, old_undo_log, new_undo_log);
        txn_->ModifyUndoLog(old_version_link->prev_.prev_log_idx_, merged_undo_log);
      }
    } else {
      // 如果元组未被当前事务修改过，则添加新的Undo日志并更新版本链
      if (old_version_link.has_value()) {
        new_undo_log.prev_version_ = old_version_link->prev_;
      }
      auto new_undo_link = txn_->AppendUndoLog(new_undo_log);
      txn_mgr_->UpdateVersionLink(delete_rid, std::make_optional<VersionUndoLink>({new_undo_link, false}), nullptr);
    }
    
    cnt++;
    to_delete_rids_.pop_back();
  }
  
  // 返回删除的元组数量
  *tuple = Tuple{{{TypeId::INTEGER, cnt}}, &GetOutputSchema()};
  return cnt > 0;
}

}  // namespace bustub
