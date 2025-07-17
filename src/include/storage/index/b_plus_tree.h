/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;  // 提前声明，打印树结构体

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
/**
 * 在递归或迭代操作中，集中跟踪已加锁的页，便于统一解锁与判断是否已到根节点
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};  //可选地保存对 header page 的写锁，确保 root 更新时独占访问

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};  //保存根页面 ID，便于判断当前操作是否在根页面上

  /**
   * 分别保存已加写锁或读锁的页，调用 pop 及重置 header_page_ 可批量解锁
   */
  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;  //
  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }  //判断给定页号是不是根节点

  //自定义队列，用于存放悲观锁模式下从根页到叶子页的路径
  std::deque<int> indexs_;
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  /**
   * 构造函数
   * @param name 索引名称
   * @param header_page_id 头页面 ID
   * @param buffer_pool_manager 缓冲池管理器，用于分配/获取/回写页面
   * @param comparator 键比较器
   * @param leaf_max_size 叶子页面最大槽位数，默认为 LEAF_PAGE_SLOT_CNT
   * @param internal_max_size 内部页面最大槽位数，默认为 INTERNAL_PAGE_SLOT_CNT
   */
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;  //判断树是否为空  通常检查根 page_id_ 是否为 INVALID_PAGE_ID 或 leaf 页键数为 0

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value)
      -> bool;  //插入操作：插入唯一键，返回是否成功（重复键失败并返回 false）

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);  //删除操作：删除键并保持树平衡（可能合并或重分配页）

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result)
      -> bool;  //点查询：查找单个 key，将相应值 push 到 result 中，成功返回 true

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;  //获取根页面 ID，通常从 header page 中读取 root_page_id_ 字段

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;  // 开始迭代器，从最左叶节点的第一个元素开始

  auto End() -> INDEXITERATOR_TYPE;  // 结束迭代器

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;  //将迭代器定位到≥key 的第一个记录

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);  //命令行打印树结构

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);  // 绘输出 Graphviz 文件并生成（或保存）图片

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;  //以上述文本形式打印B+树信息

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);  //从文件读取数据并逐个插入到 B+ 树中

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);  //从文件读取数据并逐个删除 B+ 树中的键

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  void BatchOpsFromFile(const std::filesystem::path &file_name);  //从文件读取批量操作并执行

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);  // 递归生成 Graphviz 格式节点和边

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);  // 递归打印树结构

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;  //将 B+ 树转换为可打印的结构体，便于输出或可视化

  //自定义函数
  auto KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) const -> int;  //二分查找函数，返回键在页中的位置

  auto IndexBinarySearchLeaf(const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page, const KeyType &key) const
      -> int;  //二分查找函数，在叶子中查找key插入的位置

  void BorrowFromLeftSibling(BPlusTreePage *now_page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                             int index);  //从左兄弟页借一个槽位，更新当前页和父页

  void BorrowFromRightSibling(BPlusTreePage *now_page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                              int index);  //从右兄弟页借一个槽位，更新当前页和父页

  void MergeWithLeftSibling(BPlusTreePage *now_page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                            int index);  //与左兄弟页合并，更新当前页和父页

  void MergeWithRightSibling(BPlusTreePage *now_page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                             int index);  //与右兄弟页合并，更新当前页和父页

  // member variable
  std::string index_name_;       // 索引名称，用于标识 B+ 树
  BufferPoolManager *bpm_;       // 缓冲池管理器，用于页面的分配、读取和写入
  KeyComparator comparator_;     // 键比较器，用于比较键的大小关系
  std::vector<std::string> log;  // NOLINT 用于记录操作日志，便于调试和回溯
  int leaf_max_size_;            // 叶子页面的最大槽位数，默认为 LEAF_PAGE_SLOT_CNT
  int internal_max_size_;        // 内部页面的最大槽位数，默认为 INTERNAL_PAGE_SLOT_CNT
  page_id_t header_page_id_;     // 头页面 ID，用于存储 B+ 树的元数据，如根页面 ID 等
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
//把 B+Tree 转为一个纯内存的、只含键和孩子节点的树结构，方便按层打印
struct PrintableBPlusTree {
  int size_;                                  // 节点宽度，用于对齐
  std::string keys_;                          // 该节点的所有键拼接成的字符串
  std::vector<PrintableBPlusTree> children_;  //下层可打印子节点列表

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
