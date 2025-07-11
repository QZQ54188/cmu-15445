#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if(current_reads_.empty()){
    current_reads_[read_ts] = 1;
    RecordReadTs(read_ts);
    return;
  }
  if(current_reads_.find(read_ts) == current_reads_.end()){
    current_reads_[read_ts] = 1;
    RecordReadTs(read_ts);
  }else{
    current_reads_[read_ts] += 1;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if(current_reads_.find(read_ts) == current_reads_.end()){
    throw Exception("txn not found in current_reads");
  }
  if(current_reads_[read_ts] == 1){
    current_reads_.erase(read_ts);
    RemoveReadRs(read_ts);
  }else{
    current_reads_[read_ts] -= 1;
  }
}

auto Watermark::RecordReadTs(timestamp_t read_ts) -> void{
  read_ts_set.insert(read_ts);
  watermark_ = *read_ts_set.cbegin();
}

auto Watermark::RemoveReadRs(timestamp_t read_ts) -> void{
  read_ts_set.erase(read_ts);
  if(!read_ts_set.empty()){
    watermark_ = *read_ts_set.cbegin();
  }else{
    watermark_ = commit_ts_;
  }
}

}  // namespace bustub
