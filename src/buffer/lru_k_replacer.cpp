//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // 不需要初始化latch_，因为他会自动初始化，node_store_也会自动初始化为空
  current_timestamp_ = 0;
  curr_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 获取锁，保护并发访问
  std::scoped_lock<std::mutex> lock(latch_);

  if (replacer_size_ == 0 || curr_size_ == 0) {
    return false;
  }
  // target表示将要被驱逐的frame
  frame_id_t target = -1;
  bool found = false;  // 表示有没有找到被驱逐的frame

  for (const auto &[fid, node] : node_store_) {
    // 跳过不可驱逐的frame
    if (!node.IsEvictable()) {
      continue;
    }

    // 找到当前node的存储历史，遍历找到最大的LRU-k差值
    const auto &history = node.GetHistory();
    if (history.size() < k_) {
      // 如果之前从来没有找到过驱逐的frame，选择当前frame驱逐
      if (!found) {
        found = true;
        target = fid;
        continue;
      } else if (node_store_[target].GetHistory().size() < k_) {
        // 目标页访问次数也小于k_,比较最早访问时间
        if (history.front() < node_store_[target].GetHistory().front()) {
          target = fid;
        }
      } else {
        // 如果目标页面访问次数>=k，当前页面访问次数<k，优先选择当前页面
        target = fid;
      }
    } else {
      if (!found) {
        found = true;
        target = fid;
        continue;
      } else if (node_store_[target].GetHistory().size() >= k_) {
        auto it_cur = history.begin();
        std::advance(it_cur, history.size() - k_);
        size_t cur_timestamp = *it_cur;

        auto &target_history = node_store_[target].GetHistory();
        auto it_target = target_history.begin();
        std::advance(it_target, target_history.size() - k_);
        size_t target_timestamp = *it_target;

        auto cur_dis = current_timestamp_ - cur_timestamp;
        auto target_dis = current_timestamp_ - target_timestamp;

        if (cur_dis > target_dis) {
          target = fid;
        }
      }
    }
  }

  if (!found) {
    return false;
  }

  *frame_id = target;
  curr_size_--;
  node_store_.erase(target);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode());
  }

  auto &node = node_store_[frame_id];
  auto &history = node.GetMutableHistory();
  history.push_back(current_timestamp_);

  // 更新时间戳
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");

  // 如果frame_id不在当前存储frame中，直接返回
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  auto &node = it->second;
  if (node.IsEvictable() == set_evictable) {
    // 当前状态与要设置的状态相同的话，直接返回
    return;
  }
  node.SetEvictable(set_evictable);

  // 更新replacer的大小
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");

  // frame_id不在当前存储frame中，直接返回
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  auto &node = it->second;
  BUSTUB_ASSERT(node.IsEvictable(), "Cannot remove a non-evictable frame");

  curr_size_--;
  node_store_.erase(it);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
