//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool found_frame = false;

  // 首先尝试从空闲列表中获取帧
  if (!free_list_.empty()) {
    found_frame = true;
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 没有空闲帧，尝试从替换器驱逐一个帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    found_frame = true;
    // 如果没有空闲帧，尝试从replacer中获取可驱逐的帧
    page_id_t old_page_id = pages_[frame_id].GetPageId();

    if (pages_[frame_id].IsDirty()) {
      DiskRequest write_request;
      write_request.is_write_ = true;
      write_request.page_id_ = old_page_id;
      write_request.data_ = pages_[frame_id].GetData();

      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      write_request.callback_ = std::move(promise);

      disk_scheduler_->Schedule(std::move(write_request));
      future.get();
    }
    page_table_.erase(old_page_id);
  }

  if (!found_frame) {
    return nullptr;
  }

  *page_id = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;     // 新页面的引用计数为1
  pages_[frame_id].is_dirty_ = false;  // 设置新页面为非脏页

  page_table_[*page_id] = frame_id;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查页面ID是否有效
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  if (page_table_.find(page_id) != page_table_.end()) {
    // 首先检查页面是否已经在缓冲区中
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];

    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);  // 确保页面不可被驱逐

    return page;
  }

  // 页面不再缓冲池中，需要到磁盘上加载
  frame_id_t frame_id;
  bool found_frame = false;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    found_frame = true;
  } else {
    // 没有空闲帧，尝试从替换器驱逐一个帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    page_id_t old_page_id = pages_[frame_id].GetPageId();
    if (old_page_id != INVALID_PAGE_ID) {
      // 如果被驱逐的页面是脏页，需要写回磁盘
      if (pages_[frame_id].IsDirty()) {
        DiskRequest write_request;
        write_request.is_write_ = true;
        write_request.page_id_ = old_page_id;
        write_request.data_ = pages_[frame_id].GetData();

        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        write_request.callback_ = std::move(promise);

        disk_scheduler_->Schedule(std::move(write_request));
        future.get();
      }
      page_table_.erase(old_page_id);
    }
    found_frame = true;
  }

  if (!found_frame) {
    return nullptr;
  }

  Page *page = &pages_[frame_id];

  page->ResetMemory();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  page_table_[page_id] = frame_id;

  DiskRequest read_request;
  read_request.is_write_ = false;
  read_request.page_id_ = page_id;
  read_request.data_ = page->GetData();

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  read_request.callback_ = std::move(promise);

  disk_scheduler_->Schedule(std::move(read_request));
  future.get();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // 获取页面所在的帧
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  // 检查引用计数是否已经为0
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;

  // 如果is_dirty为true，则设置页面为脏页
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  // 如果引用计数降为0，则将页面标记为可驱逐
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  // replacer_->RecordAccess(frame_id, access_type);
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  DiskRequest write_request;
  write_request.is_write_ = true;
  write_request.page_id_ = page_id;
  write_request.data_ = page->GetData();

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  write_request.callback_ = std::move(promise);

  disk_scheduler_->Schedule(std::move(write_request));
  future.get();

  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // 获取所有页面ID的副本，避免在遍历过程中修改页表
  std::vector<page_id_t> page_ids;
  {
    std::lock_guard<std::mutex> lock(latch_);
    for (const auto &[page_id, _] : page_table_) {
      page_ids.push_back(page_id);
    }
  }

  // 对每个页面调用FlushPage函数
  for (const auto &page_id : page_ids) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);

  replacer_->Remove(frame_id);

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;

  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }

  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);

  if (page != nullptr) {
    page->WLatch();
  }

  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
