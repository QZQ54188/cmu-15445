//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // 创建头页面，仿照测试代码创建
  page_id_t header_page_id = INVALID_PAGE_ID;
  BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
  header_page_id_ = header_page_id;

  // 创建第二级逻辑页表，即目录页表，仿照测试代码创建
  page_id_t directory_page_id = INVALID_PAGE_ID;
  BasicPageGuard directory_guard = bpm->NewPageGuarded(&directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);

  header_page->SetDirectoryPageId(0, directory_page_id);

  // 创建第一个桶页面
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard bucket_guard = bpm->NewPageGuarded(&bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);

  // 将桶页面ID存储在目录页面中
  directory_page->SetBucketPageId(0, bucket_page_id);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = Hash(key);

  // 获取头页面，在头页面中寻找对应的第二级目录页面
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  // 获取页面目录ID
  uint32_t dir_index = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_index);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto dir_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_guard.As<ExtendibleHTableDirectoryPage>();

  // 获取桶页面的ID
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // 获取桶页面
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // 在桶页面中查找key
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // 首先获取头页面的信息
  uint32_t dir_index;
  page_id_t dir_page_id;
  {
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    dir_index = header_page->HashToDirectoryIndex(hash);
    dir_page_id = header_page->GetDirectoryPageId(dir_index);

    if (dir_page_id == INVALID_PAGE_ID) {
      return InsertToNewDirectory(header_page, dir_index, hash, key, value);
    }
    // header_guard 在这里作用域结束后自动释放
  }

  // 获取目录页面的信息
  uint32_t bucket_index;
  page_id_t bucket_page_id;
  uint32_t local_depth;
  uint32_t global_depth;
  {
    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    bucket_index = dir_page->HashToBucketIndex(hash);
    bucket_page_id = dir_page->GetBucketPageId(bucket_index);
    local_depth = dir_page->GetLocalDepth(bucket_index);
    global_depth = dir_page->GetGlobalDepth();

    if (bucket_page_id == INVALID_PAGE_ID) {
      return InsertToNewBucket(dir_page, bucket_index, key, value);
    }
    // dir_guard 在这里作用域结束后自动释放
  }

  // 尝试插入到桶中
  {
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }

    // 如果键已存在，直接返回false
    V temp_value;
    if (bucket_page->Lookup(key, temp_value, cmp_)) {
      return false;
    }

    // 检查是否达到最大深度
    if (local_depth >= directory_max_depth_) {
      return false;
    }
    // // bucket_guard 在这里作用域结束后自动释放，释放桶页面
  }

  // 在必要的时候增加全局深度
  if (local_depth + 1 > global_depth) {
    if (global_depth >= directory_max_depth_) {
      return false;  // 全局深度已达最大，无法增加
    }

    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    dir_page->IncrGlobalDepth();
    // dir_guard 在这里作用域结束后自动释放
  }

  // 创建新桶并且修改目录页映射
  page_id_t new_bucket_page_id;
  uint32_t diff_bit = 1U << local_depth;
  uint32_t new_bucket_index = bucket_index ^ diff_bit;
  uint32_t mask = (1U << (local_depth + 1)) - 1;

  {
    auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    // new_bucket_guard 在这里作用域结束后自动释放
  }

  // 更新目录映射
  {
    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

    // 更新目录映射
    UpdateDirectoryMapping(dir_page, new_bucket_index, new_bucket_page_id, local_depth + 1, mask);
    // dir_guard 在这里作用域结束后自动释放
  }

  // 迁移键值对并重新插入新键
  {
    auto old_bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto old_bucket_page = old_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    auto new_bucket_guard = bpm_->FetchPageWrite(new_bucket_page_id);
    auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // 迁移键值对
    MigrateEntries(old_bucket_page, new_bucket_page, diff_bit);

    // 重新插入键值对
    if ((hash & diff_bit) == 0) {
      return old_bucket_page->Insert(key, value, cmp_);
    } else {
      return new_bucket_page->Insert(key, value, cmp_);
    }
    // 两个桶的guard在这里作用域结束后自动释放
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // 创建新的目录页
  page_id_t dir_page_id;
  auto dir_guard = bpm_->NewPageGuarded(&dir_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);

  header->SetDirectoryPageId(directory_idx, dir_page_id);

  // 创建新的桶页面
  page_id_t bucket_page_id;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  bucket_page->Init(bucket_max_size_);

  // 在目录页中设置桶页面id
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  dir_page->SetBucketPageId(bucket_index, bucket_page_id);

  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // 创建新的桶页面
  page_id_t bucket_page_id;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);

  // 在目录页面中设置桶页面ID
  directory->SetBucketPageId(bucket_idx, bucket_page_id);

  // 将键值对插入到桶中
  return bucket->Insert(key, value, cmp_);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // 获取头页面信息
  uint32_t dir_index;
  page_id_t dir_page_id;
  {
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

    dir_index = header_page->HashToDirectoryIndex(hash);
    dir_page_id = header_page->GetDirectoryPageId(dir_index);
    if (dir_page_id == INVALID_PAGE_ID) {
      return false;
    }
    // header_guard 在这里作用域结束后自动释放
  }

  // 获取目录页面信息
  uint32_t bucket_index;
  page_id_t bucket_page_id;
  uint32_t local_depth;
  {
    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

    bucket_index = dir_page->HashToBucketIndex(hash);
    bucket_page_id = dir_page->GetBucketPageId(bucket_index);
    local_depth = dir_page->GetLocalDepth(bucket_index);
    if (bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    // dir_guard 在这里作用域结束后自动释放
  }

  // 检查键是否存在并删除
  bool bucket_empty = false;
  {
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // 检查键是否存在
    V temp_value;
    if (!bucket_page->Lookup(key, temp_value, cmp_)) {
      return false;  // 键不存在
    }

    // 从桶中删除键
    if (!bucket_page->Remove(key, cmp_)) {
      return false;
    }

    // 检查桶是否为空
    bucket_empty = bucket_page->IsEmpty();
    // bucket_guard 在这里作用域结束后自动释放
  }

  // 如果桶为空，处理可能的合并
  if (bucket_empty && local_depth > 0) {
    // 获取分裂镜像桶信息
    uint32_t split_image_idx;
    page_id_t split_image_page_id;
    uint32_t split_image_local_depth;
    {
      auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
      auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

      split_image_idx = dir_page->GetSplitImageIndex(bucket_index);
      split_image_page_id = dir_page->GetBucketPageId(split_image_idx);

      if (split_image_page_id == INVALID_PAGE_ID) {
        return true;  // 没有分裂镜像桶，直接返回
      }

      split_image_local_depth = dir_page->GetLocalDepth(split_image_idx);
      if (split_image_local_depth != local_depth) {
        return true;  // 局部深度不同，不能合并
      }
      // dir_guard 在这里作用域结束后自动释放
    }

    // 更新目录映射并删除空桶
    {
      auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
      auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

      // 减少局部深度
      dir_page->DecrLocalDepth(bucket_index);
      dir_page->DecrLocalDepth(split_image_idx);

      uint32_t new_local_depth = local_depth - 1;
      uint32_t mask = (1U << new_local_depth) - 1;
      uint32_t prefix = bucket_index & mask;

      // 更新所有相关目录项
      for (uint32_t i = 0; i < (1U << dir_page->GetGlobalDepth()); i++) {
        if ((i & mask) == prefix) {
          dir_page->SetLocalDepth(i, new_local_depth);
          dir_page->SetBucketPageId(i, split_image_page_id);
        }
      }

      // 检查是否可以减少全局深度
      if (dir_page->CanShrink()) {
        dir_page->DecrGlobalDepth();
      }
      // dir_guard 在这里作用域结束后自动释放
    }

    // 删除不再需要的桶页面
    bpm_->DeletePage(bucket_page_id);
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t diff_bit) {
  // 创建临时数组来保存所有键值对
  std::vector<std::pair<K, V>> all_entries;
  uint32_t old_size = old_bucket->Size();

  // 先保存所有键值对
  for (uint32_t i = 0; i < old_size; i++) {
    all_entries.push_back({old_bucket->KeyAt(i), old_bucket->ValueAt(i)});
  }

  // 清空旧桶
  for (const auto &entry : all_entries) {
    old_bucket->Remove(entry.first, cmp_);
  }

  // 根据哈希值重新分配键值对
  for (const auto &entry : all_entries) {
    uint32_t hash = Hash(entry.first);
    if ((hash & diff_bit) == 0) {
      // 应该留在旧桶
      old_bucket->Insert(entry.first, entry.second, cmp_);
    } else {
      // 应该移动到新桶
      new_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // 计算旧桶的索引
  uint32_t old_bucket_idx = new_bucket_idx ^ (1U << (new_local_depth - 1));

  // 更新所有相关目录项的local_depth
  for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); i++) {
    if ((i & local_depth_mask) == (old_bucket_idx & local_depth_mask)) {
      directory->SetLocalDepth(i, new_local_depth);
    } else if ((i & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      directory->SetLocalDepth(i, new_local_depth);
      directory->SetBucketPageId(i, new_bucket_page_id);
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
