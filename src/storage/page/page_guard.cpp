#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  // 如果当前对象持有资源的话
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 避免自我赋值
  if (this == &that) {
    return *this;
  }

  // 如果当前对象持有资源的话就先释放
  this->Drop();

  // 同上移动构造函数
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  // 如果我们当前持有一个页面，需要先释放它
  this->Drop();

  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();  // 这会减少引用计数并清空page_
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  this->Drop();

  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();  // 这会减少引用计数并清空page_
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // 在创建新的ReadPageGuard之前先保存当前的状态
  BufferPoolManager *temp_bpm = bpm_;
  Page *temp_page = page_;

  // 先获取读锁
  temp_page->RLatch();

  // 创建一个临时的ReadPageGuard，然后使用移动构造函数
  ReadPageGuard temp_guard(temp_bpm, temp_page);

  // 清空当前BasicPageGuard的状态
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return temp_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // 在创建新的WritePageGuard之前先保存当前的状态
  BufferPoolManager *temp_bpm = bpm_;
  Page *temp_page = page_;

  // 先获取写锁
  temp_page->WLatch();

  // 创建一个临时的WritePageGuard，然后使用移动构造函数
  WritePageGuard temp_guard(temp_bpm, temp_page);

  // 清空当前BasicPageGuard的状态
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return temp_guard;
}
}  // namespace bustub
