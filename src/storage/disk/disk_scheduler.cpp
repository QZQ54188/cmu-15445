//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // 将请求放入请求队列
  // 使用 std::optional 包装，表示这是一个有效的请求（非终止信号）
  request_queue_.Put(std::optional<DiskRequest>(std::move(r)));

  // 注意：这里不需要等待请求完成
  // 请求会由后台线程处理，完成后会通过 r.callback_ 通知调用者
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request_opt = request_queue_.Get();

    // 如果当前request是std::nullopt就退出
    if (!request_opt.has_value()) {
      break;
    }

    DiskRequest request = std::move(request_opt.value());

    // 根据请求类型调用相应的磁盘操作
    bool success = false;
    try {
      if (request.is_write_) {
        // 写操作
        disk_manager_->WritePage(request.page_id_, request.data_);
        success = true;
      } else {
        // 读操作
        disk_manager_->ReadPage(request.page_id_, request.data_);
        success = true;
      }
    } catch (const std::exception &e) {
      // 处理可能的异常
      success = false;
    }

    request.callback_.set_value(success);
  }
}

}  // namespace bustub
