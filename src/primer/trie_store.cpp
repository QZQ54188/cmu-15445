#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // 在锁的保护下获取根节点的副本，作用域结束之后lock_guard会自动释放锁（RAII）
  Trie new_root;
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    new_root = root_;
  }

  // 在当前副本查找值，Trie::Get会正确处理空树的情况
  const T *ptr = new_root.Get<T>(key);

  if (ptr == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(new_root, *ptr);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // 在整个put过程中，都要持有写锁，因为只允许有一个写端
  std::lock_guard<std::mutex> write(write_lock_);

  // 同上，在锁的保护下获取根节点的副本
  Trie new_root;
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    new_root = root_;
  }

  // 使用Tire::put创建包含新值的Trie
  new_root = new_root.Put(key, std::move(value));

  // 更新根节点
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    root_ = new_root;
  }
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // 其实这三个函数的基本思路都差不多，在原本的Trie基本实现上加锁控制
  std::lock_guard<std::mutex> write(write_lock_);

  Trie new_root;
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    new_root = root_;
  }

  new_root = new_root.Remove(key);

  {
    std::lock_guard<std::mutex> lock(root_lock_);
    root_ = new_root;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
