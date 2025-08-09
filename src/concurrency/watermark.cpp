#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // 增加计数器
  current_reads_[read_ts]++;
  
  // 如果是第一次添加这个时间戳，更新水位线
  if (current_reads_[read_ts] == 1) {
    read_ts_set.insert(read_ts);
    UpdateWatermark();
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    throw Exception("txn not found in current_reads");
  }
  
  // 减少计数器
  current_reads_[read_ts]--;
  
  // 如果没有事务使用这个时间戳了，从集合中移除
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
    read_ts_set.erase(read_ts);
    UpdateWatermark();
  }
}

auto Watermark::UpdateWatermark() -> void {
  if (read_ts_set.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = *read_ts_set.cbegin();
  }
}

}  // namespace bustub

