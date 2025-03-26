//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // 初始化最大深度
  max_depth_ = max_depth <= HTABLE_DIRECTORY_MAX_DEPTH ? max_depth : HTABLE_DIRECTORY_MAX_DEPTH;
  // 当前第二级逻辑块的数量为0
  global_depth_ = 0;

  for (size_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
    local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // 使用哈希值的低位来确定桶索引，确保不超过当前的全局深度
  uint32_t mask = (1U << global_depth_) - 1;
  uint32_t index = hash & mask;
  return index < Size() ? index : index % Size();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  // 如果索引没有越界的话
  if (bucket_idx > Size()) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // 检查索引释放越界
  if (bucket_idx > Size()) {
    throw Exception("Bucket index out of range");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // 使用loacl_depth进行分裂
  uint32_t local_depth = local_depths_[bucket_idx];
  if (local_depth == 0) {
    return 0;
  }

  return bucket_idx ^ (1U << (local_depth - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ >= max_depth_) {
    throw Exception("Cannot increase global depth beyond max_depth");
  }
  global_depth_++;

  uint32_t old_size = Size() / 2;

  for (size_t i = 0; i < old_size; i++) {
    bucket_page_ids_[i + old_size] = bucket_page_ids_[i];
    local_depths_[i + old_size] = local_depths_[i];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (!CanShrink()) {
    throw Exception("Cannot decrease global depth");
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }

  uint32_t size = Size();
  for (uint32_t i = 0; i < size; i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1U << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    return 0;
  }
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index out of range");
  }

  // 允许local_depth > global_depth的情况，但不能超过max_depth
  if (local_depth > max_depth_) {
    throw Exception("Invalid local depth");
  }

  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index out of range");
  }
  if (local_depths_[bucket_idx] >= global_depth_) {
    throw Exception("Cannot increase local depth beyond global depth");
  }

  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index out of range");
  }
  if (local_depths_[bucket_idx] == 0) {
    throw Exception("Cannot decrease local depth below 0");
  }

  local_depths_[bucket_idx]--;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1U << max_depth_; }

}  // namespace bustub
