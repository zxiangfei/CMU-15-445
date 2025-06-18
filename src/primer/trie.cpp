/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-17 14:39:11
 * @FilePath: /CMU-15-445/src/primer/trie.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  //————————————————————————————————start——————————————————————————————————————————————
  /**
   * 1.从根结点开始，逐个字符遍历key。
   * 2.如果当前字符对应的子节点不存在，说明key不在trie中，返回nullptr。
   * 3.如果找到对应的key，使用dynamic_cast类型转换来判断类型是否符合
   * 4.如果不符合就返回nullptr
   * 5.符合范围value
   */
  auto current_node = this->root_;
  for (char c : key) {
    if (current_node == nullptr) {
      return nullptr;  // 没有找到key
    }
    auto it = current_node->children_.find(c);
    if (it == current_node->children_.end()) {
      return nullptr;  // 没有找到key
    }
    current_node = it->second;
  }
  if (current_node == nullptr || !current_node->is_value_node_) {
    return nullptr;  // key不存在或不是值节点
  }
  const auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current_node.get());

  if (value_node == nullptr) {
    return nullptr;  // 类型不匹配
  }
  return value_node->value_.get();  // 返回value
  //————————————————————————————————end——————————————————————————————————————————————
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  //————————————————————————————————start——————————————————————————————————————————————
  //您应该遍历树并在必要时创建新节点。如果节点对应的键已经存在创建一个新的TrieNodeWithValue
  auto current_node = this->root_;
  std::vector<std::pair<char, std::shared_ptr<const TrieNode>>>
      path;  //保存路径，路径上的结点需要clone，非路径结点直接复用
  for (char c : key) {
    path.push_back({c, current_node});
    if (current_node == nullptr) {
      current_node = nullptr;  // 如果当前结点为空，说明路径不存在，即路径上下一个结点也为nullptr
      continue;                // 继续遍历下一个字符
    }

    auto it = current_node->children_.find(c);
    if (it == current_node->children_.end()) {
      current_node = nullptr;  // 如果当前字符对应的子节点不存在，说明路径不存在，即路径上下一个结点也为nullptr
    } else {
      current_node = it->second;  // 如果当前字符对应的子节点存在，更新当前结点为该子节点
    }
  }

  //如果插入的结点已存在，保留原有结点的子结点
  std::map<char, std::shared_ptr<const TrieNode>> inherited_children;
  if (current_node != nullptr) {
    // 如果当前结点不为nullptr，说明路径上存在该结点，复用其所有子结点
    inherited_children = current_node->children_;
  }

  //插入结点一定是新结点
  std::shared_ptr<const TrieNode> new_node =
      std::make_shared<TrieNodeWithValue<T>>(std::move(inherited_children), std::make_shared<T>(std::move(value)));

  //开始回溯路径，clone路径上的结点
  for (auto it = path.rbegin(); it != path.rend(); ++it) {
    char c = it->first;
    auto parent_node = it->second;

    std::map<char, std::shared_ptr<const TrieNode>> new_children;

    // 如果当前父结点不为nullptr，说明原树中存在该父结点，需要先复用其所有子结点
    if (parent_node != nullptr) {
      // 如果当前结点存在，直接复用当前结点的子节点
      new_children = parent_node->children_;
    }

    //无论父是否为已存在结点，都需要更新字符c对应的子结点为新结点
    new_children[c] = new_node;  // 将新结点添加到新的子节点中

    // 新建父结点，如果父节点已存在就clone，不存在就新建
    if (parent_node) {
      new_node = parent_node->Clone();  // 克隆父结点
      const_cast<std::map<char, std::shared_ptr<const TrieNode>> &>(new_node->children_) =
          std::move(new_children);  // 更新克隆的父结点的子节点
    } else {
      new_node = std::make_shared<const TrieNode>(std::move(new_children));  // 新建一个TrieNode
    }
  }

  return Trie{new_node};  // 返回新的Trie，根结点为新建的TrieNode
  //————————————————————————————————end——————————————————————————————————————————————
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  //————————————————————————————————start——————————————————————————————————————————————
  //遍历这个树，并在必要时删除节点。如果节点不再包含值，应该将其转换为TrieNode。如果一个节点不再有子节点，就应该删除它。
  //和put一样，先查找删除结点所在的路径，再回溯更新路径
  auto current_node = this->root_;
  std::vector<std::pair<char, std::shared_ptr<const TrieNode>>>
      path;  //保存路径，路径上的结点需要clone，非路径结点直接复用

  for (char c : key) {
    path.push_back({c, current_node});
    if (current_node == nullptr) {
      break;  // 如果当前结点为空，说明路径不存在，直接break
    }

    auto it = current_node->children_.find(c);
    if (it == current_node->children_.end()) {
      current_node = nullptr;  // 如果当前字符对应的子节点不存在，更新当前结点为nullptr
    } else {
      current_node = it->second;  // 如果当前字符对应的子节点存在，更新当前结点为该子节点
    }
  }

  if (current_node == nullptr || !current_node->is_value_node_) {
    return *this;  // 如果key不存在或不是值节点，直接返回原Trie
  }

  std::shared_ptr<const TrieNode> new_node;  //用于保存遍历的结点
  if (!current_node->children_.empty()) {  // 如果当前要删除的结点的子结点不为空，把子结点保存下来
    new_node = std::make_shared<TrieNode>(current_node->children_);
  } else {
    new_node = nullptr;  // 如果当前要删除的结点的子结点为空，new_node设置为nullptr
  }

  for (auto it = path.rbegin(); it != path.rend(); ++it) {
    //获取当前字符和父结点
    char c = it->first;
    auto parent_node = it->second;

    // 如果父节点为nullptr，说明原树中不存在要删除节点，直接返回新的Trie
    if (!parent_node) {
      continue;  // 如果父节点为nullptr，说明原树中不存在要删除节点，直接continue
    }

    std::map<char, std::shared_ptr<const TrieNode>> new_children = parent_node->children_;  // 用来保存父结点的子结点

    if (!new_node) {  //如果new_node为空，说明此时要删除的结点还没删除完，执行删除操作
      new_children.erase(c);  // 删除当前字符对应的子节点
    } else {
      // 如果new_node不为空，说明当前结点已经被删除过了，需要将new_node添加到父结点的子节点中
      new_children[c] = new_node;  // 将new_node添加到新的子节点中
    }

    //删除过后，如果父结点的子节点为空，且父结点不存在值，就还需要继续删除，即把new_node设置为nullptr
    if (new_children.empty() && !parent_node->is_value_node_) {
      new_node = nullptr;  // 如果父结点的子节点为空，且父结点不存在值，就把new_node设置为nullptr
    } else {
      // 否则就克隆父结点
      new_node = parent_node->Clone();  // 克隆父结点
      const_cast<std::map<char, std::shared_ptr<const TrieNode>> &>(new_node->children_) =
          std::move(new_children);  // 更新克隆的父结点的子节点
    }
  }
  return Trie{new_node};
  //————————————————————————————————end——————————————————————————————————————————————
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
