//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // 初始化第一级逻辑块最大数量，并且确保不超过HTABLE_HEADER_MAX_DEPTH
  max_depth_ = max_depth <= HTABLE_HEADER_MAX_DEPTH ? max_depth : HTABLE_HEADER_MAX_DEPTH;

  // 一开始没有映射，所以没有逻辑块
  for (size_t i = 0; i < MaxSize(); i++) {
    directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  // 使用哈希值的高位确定目录索引
  if (max_depth_ == 0) {
    return 0;
  }
  uint32_t shift_bits = 32 - max_depth_;
  uint32_t index = (hash >> shift_bits);
  return index < MaxSize() ? index : index % MaxSize();
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  if (directory_idx > MaxSize()) {
    return INVALID_PAGE_ID;
  }
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  if (directory_idx > MaxSize()) {
    throw Exception("Directory index out of range");
  }
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  // 表示可拓展哈希中第一级逻辑块的大小
  return 1U << max_depth_;
}

}  // namespace bustub
