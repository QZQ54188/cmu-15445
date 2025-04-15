#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  current_reads_[read_ts]++;
  
  if (current_reads_[read_ts] == 1) {
    active_read_ts_.insert(read_ts);
  }
  
  watermark_ = *active_read_ts_.begin();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    return;
  }
  
  iter->second--;
  
  if (iter->second == 0) {
    current_reads_.erase(iter);
    active_read_ts_.erase(read_ts);
    // 更新水位线
    if (active_read_ts_.empty()) {
      watermark_ = commit_ts_;
    } else {
      watermark_ = *active_read_ts_.begin();
    }
  }
}

}  // namespace bustub
