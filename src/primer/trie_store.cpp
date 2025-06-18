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
  // throw NotImplementedException("TrieStore::Get is not implemented.");

  //—————————————————————————————start——————————————————————————————
  // (1) 获取root锁，获取根节点，释放root锁。不要在持有root锁的情况下查找trie中的值。
  // (2) 在trie中查找值。
  // (3) 如果找到值，返回一个ValueGuard对象，该对象持有对值和根的引用。否则，返回std::nullopt。

  std::lock_guard<std::mutex> root_lock(root_lock_);  //获取root锁
  auto new_root = this->root_;
  const T *value = new_root.Get<T>(key);  //调用Trie的Get方法查找值
  if (value == nullptr) {
    return std::nullopt;  // 未找到或者类型不匹配，返回nullptr
  }
  return ValueGuard<T>(std::move(new_root), *value);  // 找到后返回ValueGuard对象，持有对值和根的引用
  //—————————————————————————————end——————————————————————————————
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");

  //—————————————————————————————start——————————————————————————————
  //同时只有一个写操作
  std::lock_guard<std::mutex> write_lock(write_lock_);     //获取写锁,只允许一个写操作进行
  auto new_root = this->root_.Put(key, std::move(value));  //调用Trie的Put方法插入值
  std::lock_guard<std::mutex> root_lock(root_lock_);       //获取root锁
  this->root_ = std::move(new_root);                       //更新根节点
  //—————————————————————————————end——————————————————————————————
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");

  //—————————————————————————————start——————————————————————————————
  //同时只有一个删除操作
  std::lock_guard<std::mutex> write_lock(write_lock_);  //获取写锁,只允许一个写操作进行
  auto new_root = this->root_.Remove(key);              //调用Trie的Remove方法删除值
  std::lock_guard<std::mutex> root_lock(root_lock_);    //获取root锁
  this->root_ = std::move(new_root);                    //更新根节点
  //—————————————————————————————end——————————————————————————————
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
