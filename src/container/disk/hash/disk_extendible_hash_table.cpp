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

  // 获取头页面，基本操作同上GetValue函数
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t dir_index = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_index);

  if (dir_page_id == INVALID_PAGE_ID) {
    // 如果对应页面目录不存在，就创建一个页面目录
    return InsertToNewDirectory(header_page, dir_index, hash, key, value);
  }

  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);

  if (bucket_page_id == INVALID_PAGE_ID) {
    // 如果桶不存在，就创建一个新桶
    return InsertToNewBucket(dir_page, bucket_index, key, value);
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (bucket_page->Insert(key, value, cmp_)) {
    return true;
  }

  // 桶已经满了的情况下，需要分裂目录页
  if (dir_page->GetLocalDepth(bucket_index) >= directory_max_depth_) {
    // 已经到达最大可以分裂的程度，无法再分裂，直接返回false
    return false;
  }

  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);

  // 检测是否需要增加全局深度
  if (local_depth + 1 > dir_page->GetGlobalDepth()) {
    if (dir_page->GetGlobalDepth() >= directory_max_depth_) {
      // 无法再增加全局深度
      return false;
    }
    dir_page->IncrGlobalDepth();
  }

  // 创建新桶
  page_id_t new_bucket_page_id;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);

  // 计算用于区分新旧桶的位掩码
  uint32_t diff_bit = 1U << local_depth;
  uint32_t new_bucket_index = bucket_index ^ diff_bit;

  // 计算掩码，用于找到所有指向同一个桶的目录项
  uint32_t mask = (1U << (local_depth + 1)) - 1;

  // 更新目录映射
  UpdateDirectoryMapping(dir_page, new_bucket_index, new_bucket_page_id, local_depth + 1, mask);

  // 迁移键值对
  MigrateEntries(bucket_page, new_bucket_page, diff_bit);

  // 重新插入键值对
  if ((hash & diff_bit) == 0) {
    return bucket_page->Insert(key, value, cmp_);
  } else {
    return new_bucket_page->Insert(key, value, cmp_);
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

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBuckets(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                     uint32_t split_image_idx) {
  // 获取桶和分裂镜像桶的局部深度
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  page_id_t split_image_page_id = directory->GetBucketPageId(split_image_idx);

  // 新的局部深度
  uint32_t new_local_depth = local_depth - 1;

  // 掩码用于找到与当前桶共享相同前缀的所有桶
  uint32_t mask = (1U << new_local_depth) - 1;
  uint32_t prefix = bucket_idx & mask;

  // 更新所有相关目录项，指向分裂镜像桶
  for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); i++) {
    if ((i & mask) == prefix) {
      directory->SetLocalDepth(i, new_local_depth);
      directory->SetBucketPageId(i, split_image_page_id);
    }
  }

  // 删除不再需要的页面
  bpm_->DeletePage(bucket_page_id);

  // 检查是否可以减少全局深度
  if (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // 获取头页面
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // 获取目录页面
  uint32_t dir_index = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_index);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 获取桶页面
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 从桶中删除键
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }

  // 如果桶变为空，尝试合并
  if (bucket_page->IsEmpty()) {
    TryMergeEmptyBucket(dir_page, bucket_index);
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::TryMergeEmptyBucket(ExtendibleHTableDirectoryPage *directory,
                                                            uint32_t bucket_idx) {
  // 只有局部深度大于0的桶才能合并
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    return;
  }

  // 获取分裂镜像桶
  uint32_t split_image_idx = directory->GetSplitImageIndex(bucket_idx);
  page_id_t split_image_page_id = directory->GetBucketPageId(split_image_idx);

  // 检查分裂镜像桶是否存在
  if (split_image_page_id == INVALID_PAGE_ID) {
    return;
  }

  // 检查分裂镜像桶的局部深度是否与当前桶相同
  if (directory->GetLocalDepth(split_image_idx) != local_depth) {
    return;
  }

  // 合并桶
  MergeBuckets(directory, bucket_idx, split_image_idx);

  // 检查合并后的桶是否也为空，如果为空则递归合并
  auto new_bucket_guard = bpm_->FetchPageRead(split_image_page_id);
  auto new_bucket_page = new_bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  if (new_bucket_page->IsEmpty()) {
    // 计算合并后的新桶索引
    uint32_t new_bucket_idx = bucket_idx & ((1U << (local_depth - 1)) - 1);
    TryMergeEmptyBucket(directory, new_bucket_idx);
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t diff_bit) {
  // 修改迁移逻辑，创建临时数组来保存要移动的键值对
  std::vector<std::pair<K, V>> items_to_move;

  uint32_t old_size = old_bucket->Size();
  for (uint32_t i = 0; i < old_size; i++) {
    // 获取键值对
    K key = old_bucket->KeyAt(i);
    V value = old_bucket->ValueAt(i);

    // 计算键的哈希值
    uint32_t hash = Hash(key);

    // 根据哈希值确定条目应该放在哪个桶中
    if ((hash & diff_bit) != 0) {  // 如果diff_bit位为1，应该移动到新桶
      items_to_move.push_back({key, value});
    }
  }

  // 将收集的项从旧桶中删除，并插入到新桶中
  for (const auto &item : items_to_move) {
    old_bucket->Remove(item.first, cmp_);
    new_bucket->Insert(item.first, item.second, cmp_);
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
