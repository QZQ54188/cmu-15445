//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  txn->commit_ts_ = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // 遍历写集，更新所有修改的元组时间戳
  for (const auto &write_set_entry : txn->GetWriteSets()) {
    table_oid_t table_oid = write_set_entry.first;
    const auto &rid_set = write_set_entry.second;
    
    auto table_info = catalog_->GetTable(table_oid);
    
    for (const auto &rid : rid_set) {
      // 获取当前元组的元数据
      auto current_meta = table_info->table_->GetTupleMeta(rid);
      
      // 更新时间戳为提交时间戳，保持删除状态
      TupleMeta new_meta{
        .ts_ = txn->commit_ts_,
        .is_deleted_ = current_meta.is_deleted_
      };
      
      table_info->table_->UpdateTupleMeta(new_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  last_commit_ts_.fetch_add(1);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // Get the watermark (minimum read timestamp)
  auto watermark = GetWatermark();
  
  // Iterate through all tables
  auto table_names = catalog_->GetTableNames();
  for (const auto& table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) continue;
    
    auto table_heap = table_info->table_.get();
    if (table_heap == nullptr) continue;
    
    // Iterate through all tuples in the table
    auto iter = table_heap->MakeIterator();
    while (!iter.IsEnd()) {
      auto rid = iter.GetRID();
      
      // Get the version link for this tuple
      auto version_link = GetVersionLink(rid);
      if (!version_link.has_value()) {
        ++iter;
        continue;
      }
      
      // Traverse the undo log chain
      auto current_link = version_link->prev_;
      while (current_link.IsValid()) {
        // Check if the undo log exists (not a dangling pointer)
        auto undo_log_opt = GetUndoLogOptional(current_link);
        if (!undo_log_opt.has_value()) {
          // This is a dangling pointer, we can't traverse further
          break;
        }
        
        auto undo_log = undo_log_opt.value();
        
        // If the undo log timestamp is less than watermark, mark it as deleted
        if (undo_log.ts_ < watermark) {
          undo_log.is_deleted_ = true;
          
          // Update the undo log in the transaction
          std::shared_lock<std::shared_mutex> txn_lck(txn_map_mutex_);
          auto txn_iter = txn_map_.find(current_link.prev_txn_);
          if (txn_iter != txn_map_.end()) {
            auto txn = txn_iter->second;
            txn_lck.unlock();
            txn->ModifyUndoLog(current_link.prev_log_idx_, undo_log);
          }
        }
        
        // Move to the next undo log in the chain
        current_link = undo_log.prev_version_;
      }
      
      ++iter;
    }
  }
  
  // Now remove transactions whose all undo logs are deleted
  std::vector<txn_id_t> txns_to_remove;
  {
    std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (const auto& [txn_id, txn] : txn_map_) {
      // Skip uncommitted transactions
      if (txn->GetTransactionState() != TransactionState::COMMITTED) {
        continue;
      }
      
      // Check if all undo logs are deleted
      bool all_deleted = true;
      for (size_t i = 0; i < txn->GetUndoLogNum(); i++) {
        auto undo_log = txn->GetUndoLog(i);
        if (!undo_log.is_deleted_) {
          all_deleted = false;
          break;
        }
      }
      
      if (all_deleted) {
        txns_to_remove.push_back(txn_id);
      }
    }
  }
  
  // Remove the transactions
  if (!txns_to_remove.empty()) {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (auto txn_id : txns_to_remove) {
      txn_map_.erase(txn_id);
    }
  }
}

}  // namespace bustub
