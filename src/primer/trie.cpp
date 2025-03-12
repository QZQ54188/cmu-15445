#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // 表示当前遍历到的节点
  auto current = root_;
  // 如果当前是空树，直接返回
  if (root_ == nullptr) {
    return nullptr;
  }
  for (char ch : key) {
    // 遍历key的每个字符，在孩子中找到对应的子树
    if (current->children_.find(ch) == current->children_.end()) {
      return nullptr;
    }
    current = current->children_.at(ch);
  }

  if (!current->is_value_node_) {
    return nullptr;
  }

  const auto *node = dynamic_cast<const TrieNodeWithValue<T> *>(current.get());
  if (node == nullptr) {
    // 在get和put提供的模板T不一样时，转化不成功的情况
    return nullptr;
  }
  return node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 创建新的根节点，使用 shared_ptr 来管理内存
  std::shared_ptr<TrieNode> new_root;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = std::shared_ptr<TrieNode>(root_->Clone());
  }

  // 处理空key的情况
  if (key.empty()) {
    return Trie(std::make_shared<const TrieNodeWithValue<T>>(std::move(new_root->children_),
                                                             std::make_shared<T>(std::move(value))));
  }

  // 在循环外创建 value 的智能指针，确保只移动一次
  auto value_ptr = std::make_shared<T>(std::move(value));

  // 得到智能指针对应的裸指针进行操作
  auto current = new_root.get();

  // 遍历key的字符，构建路径
  for (size_t i = 0; i < key.length(); i++) {
    char ch = key[i];
    auto it = current->children_.find(ch);

    // 判断是否是最后一个字符
    if (i == key.length() - 1) {
      if (it != current->children_.end()) {
        // 如果最后一个字符对应的节点已经存在
        current->children_[ch] = std::make_shared<const TrieNodeWithValue<T>>(it->second->children_, value_ptr);
      } else {
        // 如果最后一个字符对应的节点不存在
        current->children_[ch] = std::make_shared<const TrieNodeWithValue<T>>(value_ptr);
      }
    } else {
      std::shared_ptr<TrieNode> new_node;
      if (it != current->children_.end()) {
        // 如果节点存在，创建新的节点
        new_node = std::shared_ptr<TrieNode>(it->second->Clone());
      } else {
        // 如果节点不存在，创建新的空节点
        new_node = std::make_shared<TrieNode>();
      }
      current->children_[ch] = new_node;
      current = new_node.get();
    }
  }

  // 使用 std::const_pointer_cast 将 shared_ptr<TrieNode> 转换为 shared_ptr<const TrieNode>
  return Trie(std::const_pointer_cast<const TrieNode>(new_root));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return *this;
  }

  // 创建新的根节点，使用 shared_ptr 管理内存
  std::shared_ptr<TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());

  if (key.empty()) {
    if (root_->is_value_node_) {
      return Trie(std::make_shared<const TrieNode>(root_->children_));
    }
    return *this;
  }

  std::stack<std::pair<TrieNode *, char>> path;
  auto current = new_root.get();

  for (char ch : key) {
    auto it = current->children_.find(ch);
    if (it == current->children_.end()) {
      return *this;
    }

    path.push({current, ch});
    auto next_node = std::shared_ptr<TrieNode>(it->second->Clone());
    current->children_[ch] = next_node;
    current = next_node.get();
  }

  if (!current->is_value_node_) {
    return *this;
  }

  if (!current->children_.empty()) {
    auto new_node = std::make_shared<const TrieNode>(current->children_);
    auto &[parent, ch] = path.top();
    parent->children_[ch] = new_node;
  } else {
    while (!path.empty()) {
      auto &[parent, ch] = path.top();
      path.pop();

      parent->children_.erase(ch);

      if (parent->is_value_node_ || !parent->children_.empty()) {
        break;
      }
    }
  }

  if (new_root->children_.empty() && !new_root->is_value_node_) {
    return Trie(nullptr);
  }

  return Trie(std::const_pointer_cast<const TrieNode>(new_root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
