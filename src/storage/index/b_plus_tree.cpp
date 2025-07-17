/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-17 11:10:03
 * @FilePath: /CMU-15-445/src/storage/index/b_plus_tree.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;  //上下文对象，用于存储操作状态

  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_page->root_page_id_;
  guard.Drop();

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return false;  //如果根页 ID 无效，表示树为空，返回 false
  }

  //如果树不为空，开始查找
  //从根页开始，逐层向下查找,是叶子页时停止
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);  //二分查找找到第一个大于等于 key 的子节点
    if (index == -1) {
      return false;  //如果没有找到，返回 false
    }
    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    page_id_t child_page_id = internal_page->ValueAt(index);  //获取子节点的页 ID
    ctx.read_set_.push_back(bpm_->ReadPage(child_page_id));   //读取子节点页面
    page = ctx.read_set_.back().As<BPlusTreePage>();

    ctx.read_set_.pop_front();  //拿到子页读锁后，释放父页的读锁，有利于并发
  }

  //到达叶子页后，开始查找键值对
  int index = KeyBinarySearch(page, key);  //在叶子页中二分查找键
  if (index == -1) {
    return false;  //如果没有找到，返回 false
  }
  auto leaf_page = static_cast<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
  result->push_back(leaf_page->ValueAt(index));  //将找到的值添加到结果中

  return true;  //返回 true，表示找到了对应的键值对
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;  //创建上下文实例

  WritePageGuard op_header_guard = bpm_->WritePage(header_page_id_);               //获取头页的写锁
  ctx.header_page_ = std::make_optional(std::move(op_header_guard));               //将头页写锁存入上下文
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;  //获取根页 ID

  /**
   * 1.如果树为空，即根页 ID 为 INVALID_PAGE_ID
   * 则创建一个新的叶子页作为根页，并将其初始化。然后直接插入结点，结束
   */
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    //创建新的叶子页
    page_id_t root_page_id = bpm_->NewPage();
    if (root_page_id == INVALID_PAGE_ID) {
      return false;  //如果无法分配新页，返回 false
    }
    WritePageGuard root_guard = bpm_->WritePage(root_page_id);  //获取新叶子页的写锁
    auto root_page = root_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    root_page->Init(leaf_max_size_);  //初始化叶子页

    //插入键值对
    root_page->SetKeyAt(0, key);      //设置第一个键
    root_page->SetValueAt(0, value);  //设置第一个值
    root_page->SetSize(1);            //设置叶子页的大小为 1

    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;  //更新头页的根页 ID
    ctx.root_page_id_ = root_page_id;           //更新上下文中的根页 ID
    return true;                                //返回 true，表示插入成功
  }

  /**
   * 2.如果树不为空
   * 先尝试乐观锁，假设叶子节点不满，中间节点使用读锁，叶子结点使用写锁
   * 乐观锁不成立(即叶子节点满了)，则需要使用悲观锁，所有结点使用读锁
   */
  /**2.1 乐观锁 */
  BPlusTreePage *op_write_page = nullptr;  //用于记录要写入的页
  //先找到要插入的叶子节点
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));  //读取根页
  auto op_now_page = ctx.read_set_.back().As<BPlusTreePage>();

  //如果根页就是叶子页
  if (op_now_page->IsLeafPage()) {
    ctx.read_set_.pop_back();                                      //释放根页的读锁
    ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));  //获取根页的写锁
    op_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  }

  ctx.header_page_ = std::nullopt;  //因为是乐观锁，所以可以清除头页的写锁，因为后续操作不需要修改头页,提高并发

  page_id_t op_now_page_id = ctx.root_page_id_;  //当前页的页 ID

  //如果根页不是叶子页，逐层向下查找，直到找到叶子页
  while (!op_now_page->IsLeafPage()) {
    int index = KeyBinarySearch(op_now_page, key);  //在当前页中二分查找键
    if (index == -1) {
      return false;  //如果没有找到，返回 false
    }
    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(op_now_page);
    op_now_page_id = internal_page->ValueAt(index);           //获取子节点的页 ID
    ctx.read_set_.push_back(bpm_->ReadPage(op_now_page_id));  //读取子节点页面
    op_now_page = ctx.read_set_.back().As<BPlusTreePage>();   //更新当前页为子节点页面

    if (op_now_page->IsLeafPage()) {                              //如果到达叶子页
      ctx.read_set_.pop_back();                                   //释放叶子页的读锁
      ctx.write_set_.push_back(bpm_->WritePage(op_now_page_id));  //获取叶子页的写锁
      op_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    }
    ctx.read_set_.pop_front();  //释放父节点的读锁
  }

  /**
   * 找到叶子页后，需要判断当前乐观锁状态下，叶子页是否已满
   * 如果未满，乐观锁成立，直接插入
   * 如果已满，则需要使用悲观锁
   */
  op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();

  if (op_write_page->GetSize() < op_write_page->GetMaxSize()) {  //如果叶子页未满
    auto op_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(op_write_page);
    int index = IndexBinarySearchLeaf(op_leaf_page, key);  //在叶子页中二分查找键的插入位置
    if (index == -1) {
      return false;  //返回-1，一般为左边界 = key
    }

    //找到key的位置后需要判断是否重复插入
    if (comparator_(op_leaf_page->KeyAt(index), key) == 0) {
      return false;  //如果键已存在，返回 false
    }

    int size = op_leaf_page->GetSize();  //获取当前叶子页的大小
    //如果键不存在，直接插入
    for (int i = size; i > index; --i) {
      op_leaf_page->SetKeyAt(i, op_leaf_page->KeyAt(i - 1));      //将后面的键向后移动
      op_leaf_page->SetValueAt(i, op_leaf_page->ValueAt(i - 1));  //将后面的值向后移动
    }
    op_leaf_page->SetKeyAt(index, key);      //设置新键
    op_leaf_page->SetValueAt(index, value);  //设置新值
    op_leaf_page->SetSize(size + 1);         //更新叶子页的大小
    return true;                             //返回 true，表示插入成功
  }

  //如果叶子页已满，乐观锁不成立，需要使用悲观锁，先清除所有的锁，/**因为读锁在搜索过程中已经被释放，例如155行，所以只需要清楚写锁*/
  // ctx.read_set_.clear();  //清除所有读锁
  ctx.write_set_.clear();  //清除所有写锁

  /** 2.2 悲观锁 */
  //写锁    读头页
  WritePageGuard pe_header_guard = bpm_->WritePage(header_page_id_);               //获取头页的写锁
  ctx.header_page_ = std::make_optional(std::move(pe_header_guard));               //将头页写锁存入上下文
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;  //获取根页 ID

  //从根节点开始一路写锁到叶节点
  WritePageGuard pe_root_guard = bpm_->WritePage(ctx.root_page_id_);  //获取根页的写锁
  auto pe_now_page = pe_root_guard.AsMut<BPlusTreePage>();            //获取根页
  ctx.write_set_.push_back(std::move(pe_root_guard));                 //将根页写锁存入上下文

  if (pe_now_page->GetSize() <
      pe_now_page->GetMaxSize()) {  //如果根页未满，说明后续的插入操作不会造成根的变化(分裂)，可以释放头页的写锁
    ctx.header_page_ = std::nullopt;  //清除头页的写锁
  }

  //一路找到叶子节点
  while (!pe_now_page->IsLeafPage()) {
    int index = KeyBinarySearch(pe_now_page, key);  //在当前页中二分查找键
    if (index == -1) {
      return false;  //如果没有找到，返回 false
    }
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pe_now_page);
    page_id_t pe_now_page_id = internal_page->ValueAt(index);   //获取子节点的页 ID
    ctx.write_set_.push_back(bpm_->WritePage(pe_now_page_id));  //将子节点写锁存入上下文

    ctx.indexs_.push_back(index);  //记录当前索引位置

    pe_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //更新当前页为子节点页面

    if (pe_now_page->GetSize() < pe_now_page->GetMaxSize()) {  //如果中间过程一但有节点不满，根节点就不会发生变化
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;  //清除头页的写锁
      }
      while (ctx.write_set_.size() > 1) {  //并且此时可以释放上层页写锁，因为不会发生分裂
        ctx.write_set_.pop_front();        //释放上层页的写锁
      }
    }
  }

  //到达叶子页后，找到要插入的位置
  auto pe_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(pe_now_page);
  int insert_index = IndexBinarySearchLeaf(pe_leaf_page, key);  //在叶子页中二分查找键的插入位置
  if (insert_index == -1) {
    return false;  //如果没有找到，返回 false
  }
  //如果键已存在，返回 false
  if (comparator_(pe_leaf_page->KeyAt(insert_index), key) == 0) {
    return false;
  }

  //键不存在，如果要插入的叶子未满，直接插入
  if (pe_leaf_page->GetSize() < pe_leaf_page->GetMaxSize()) {
    int size = pe_leaf_page->GetSize();  //获取当前叶子页的大小
    for (int i = size; i > insert_index; --i) {
      pe_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(i - 1));      //将后面的键向后移动
      pe_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(i - 1));  //将后面的值向后移动
    }
    pe_leaf_page->SetKeyAt(insert_index, key);           //设置新键
    pe_leaf_page->SetValueAt(insert_index, value);       //设置新值
    pe_leaf_page->SetSize(pe_leaf_page->GetSize() + 1);  //更新叶子页的大小

    ctx.write_set_.pop_back();  //释放叶子页的写锁
    return true;                //返回 true，表示插入成功
  }

  //如果叶子页已满，需要分裂叶子页
  //在分裂时，如果(maxsize + 1)是偶数，直接平分；如果(maxsize + 1)是奇数，让左边多一个而不是右边多一个(偏好问题)

  int left_size = (pe_leaf_page->GetSize() + 2) / 2;         //左边叶子页应该分的的数量
  int right_size = pe_leaf_page->GetSize() + 1 - left_size;  //右边叶子页应该分的的数量

  //创建新的叶子页
  page_id_t new_leaf_page_id = bpm_->NewPage();
  if (new_leaf_page_id == INVALID_PAGE_ID) {
    return false;  //如果无法分配新页，返回 false
  }
  WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_page_id);  //获取新叶子页的写锁
  auto new_leaf_page = new_leaf_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  ctx.write_set_.push_back(std::move(new_leaf_guard));  //将新叶子页写锁存入上下文
  new_leaf_page->Init(leaf_max_size_);                  //初始化新叶子页

  //修改两个页size
  pe_leaf_page->SetSize(left_size);    //设置左边叶子页的大小
  new_leaf_page->SetSize(right_size);  //设置右边叶子页的大小

  //更新两个页的next_page_id
  new_leaf_page->SetNextPageId(pe_leaf_page->GetNextPageId());
  pe_leaf_page->SetNextPageId(new_leaf_page_id);  //设置左边叶子页的下一个页 ID

  //此时分两种情况处理
  if (insert_index < left_size) {  //如果插入位置在左边叶子页
    //将左边叶子页的键值对复制到新叶子页
    for (int i = 0; i < right_size; ++i) {
      new_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(left_size + i - 1));      //设置新叶子页的键
      new_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(left_size + i - 1));  //设置新叶子页的值
    }
    //将要插入的键值对插入到左边叶子页
    for (int i = left_size - 1; i > insert_index; --i) {
      pe_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(i - 1));      //将后面的键向后移动
      pe_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(i - 1));  //将后面的值向后移动
    }
    pe_leaf_page->SetKeyAt(insert_index, key);      //设置新键
    pe_leaf_page->SetValueAt(insert_index, value);  //设置新值
  } else {                                          //如果插入位置在右边叶子页
    for (int i = 0; i < insert_index - left_size; ++i) {
      new_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(i + left_size));  //将右边叶子插入位置之前的键填充上
      new_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(i + left_size));  //将右边叶子插入位置之前的值填充上
    }
    //将要插入的键值对插入到新叶子页
    new_leaf_page->SetKeyAt(insert_index - left_size, key);      //设置新键
    new_leaf_page->SetValueAt(insert_index - left_size, value);  //设置新值
    for (int i = insert_index - left_size + 1; i < right_size; ++i) {
      new_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(left_size + i - 1));  //将右边叶子插入位置之后的键填充上
      new_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(left_size + i - 1));  //将右边叶子插入位置之后的值填充上
    }
  }

  //叶子页处理之后，需要向上更新父页
  //获取新叶子页的第一个key，用于更新父页
  KeyType new_key = new_leaf_page->KeyAt(0);  //获取新叶子页的第一个键
  //叶子页不需要再更新，释放写锁
  ctx.write_set_.pop_back();  //释放新叶子页的写锁
  ctx.write_set_.pop_back();  //释放当前叶子页的写锁

  page_id_t first_split_page_id = ctx.root_page_id_;  //根节点分裂时使用
  page_id_t second_split_page_id = new_leaf_page_id;  //新叶子页的页 ID

  bool new_root_flag = true;  //标记是否需要创建新的根页

  while (!ctx.indexs_.empty()) {            //逐层向上更新父页
    insert_index = ctx.indexs_.back() + 1;  //获取要插入的位置，兄弟页要插入到该页右邻位置
    auto internal_page =
        ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();  //获取当前页
    int size = internal_page->GetSize();  //获取当前页的大小

    if (size < internal_page->GetMaxSize()) {  //如果当前页未满，直接插入
      for (int i = size; i > insert_index; --i) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));      //将后面的键向后移动
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));  //将后面的值向后移动
      }
      internal_page->SetKeyAt(insert_index, new_key);                 //设置新键
      internal_page->SetValueAt(insert_index, second_split_page_id);  //设置新叶子页的页 ID
      internal_page->SetSize(size + 1);                               //更新当前页的大小

      new_root_flag = false;   //不需要创建新的根页
      ctx.write_set_.clear();  //清除写锁
      ctx.indexs_.clear();     //清除索引
      break;
    }

    //如果当前页已满，需要分裂当前页
    //但是需要注意的是，内部页的key比value少一个,并且是按value的数量分配
    int left_size = (internal_page->GetSize() + 2) / 2;         //左边内部页应该分的数量
    int right_size = internal_page->GetSize() + 1 - left_size;  //右边内部页应该分的数量

    //创建新的内部页
    page_id_t new_internal_page_id = bpm_->NewPage();
    if (new_internal_page_id == INVALID_PAGE_ID) {
      return false;  //如果无法分配新页，返回 false
    }

    WritePageGuard new_internal_guard = bpm_->WritePage(new_internal_page_id);  //获取新内部页的写锁
    auto new_internal_page = new_internal_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    ctx.write_set_.push_back(std::move(new_internal_guard));  //将新内部页写锁存入上下文

    new_internal_page->Init(internal_max_size_);  //初始化新内部页

    //修改两个页size
    internal_page->SetSize(left_size);       //设置左边内部页的大小
    new_internal_page->SetSize(right_size);  //设置右边内部页的大小

    //在中间页分裂过程中还需要注意，“中间页的key比value少一个”这种实现方式下，中间页的父页中的key是不需要在子页中存在的
    /**
     * 下面是个例子
     *                                10       中间页
     *                        /               \
     *                    4     8    |   14           18    中间页
     *                  /  \   / \    /   \       /    \
     *                2 | 4 5|9 | 8 12 | 14 15|  16 | 18 19    叶子页
     */
    //此时分两种情况，如果插入结点在左页和插入结点在右页
    if (insert_index < left_size) {  //如果插入位置在左边内部页
      KeyType mid_key = internal_page->KeyAt(
          left_size - 1);  //获取分裂之前的中间页的mid_key，作为向上传递的key，这个key不需要在内部子页存在

      //复制键值到新的内部页
      for (int i = 0; i < right_size; ++i) {
        if (i > 0) {  //内部页key[0]不存数据，所以要单独处理
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(left_size + i - 1));  //设置新内部页的键
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(left_size + i - 1));  //设置新内部页的值
      }
      //将要插入的键值对插入到左边内部页
      for (int i = left_size - 1; i > insert_index; --i) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));      //将后面的键向后移动
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));  //将后面的值向后移动
      }
      internal_page->SetKeyAt(insert_index, new_key);                 //设置新键
      internal_page->SetValueAt(insert_index, second_split_page_id);  //设置新叶子页的页 ID

      //更新insert_index，用于后续上层继续插入
      new_key = mid_key;
    } else {  //如果插入位置在右边内部页
      //左侧内部页不需要变，右侧内部页插入结点
      //插入节点之前的key和value不变，直接移动到新页
      for (int i = 0; i < insert_index - left_size; ++i) {
        if (i > 0) {  //内部页key[0]不存数据，所以要单独处理
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(i + left_size));  //将右边内部页插入位置之前的键填充上
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(i + left_size));  //将右边内部页插入位置之前的值填充上
      }

      KeyType mid_key;
      //此时不能直接把insert_index插入到右侧，因为有一种可能是要插入的这个insert_index就是mid_key
      //所以需要判断insert_indexs是否是 mid_key
      if (insert_index > left_size) {                                    //此时说明insert_index不是mid_key
        new_internal_page->SetKeyAt(insert_index - left_size, new_key);  //设置新键
        mid_key = internal_page->KeyAt(
            left_size);  //获取分裂之前的中间页的mid_key，作为向上传递的key，这个key不需要在内部子页存在
      } else {              //如果insert_index是mid_key
        mid_key = new_key;  //此时mid_key就是要插入的key
      }
      new_internal_page->SetValueAt(insert_index - left_size, second_split_page_id);  //设置新叶子页的页 ID

      for (int i = insert_index - left_size + 1; i < right_size; ++i) {
        new_internal_page->SetKeyAt(i, internal_page->KeyAt(left_size + i - 1));  //将右边内部页插入位置之后的键填充上
        new_internal_page->SetValueAt(i,
                                      internal_page->ValueAt(left_size + i - 1));  //将右边内部页插入位置之后的值填充上
      }

      //更新insert_index，用于后续上层继续插入
      new_key = mid_key;  //更新新键为中间页的键
    }

    //更新要向上传递的新页id，用于上层页设置新子页指针
    second_split_page_id = new_internal_page_id;  //更新新内部页的页 ID

    //释放新页的写锁
    ctx.write_set_.pop_back();  //释放新内部页的写锁
    ctx.write_set_.pop_back();  //释放当前内部页的写锁

    ctx.indexs_.pop_back();  //移除当前索引位置
  }

  //所有路径分裂完毕需要判断是否需要创建新的根页
  if (new_root_flag) {
    //创建新的根页
    page_id_t new_root_page_id = bpm_->NewPage();
    if (new_root_page_id == INVALID_PAGE_ID) {
      return false;  //如果无法分配新页，返回 false
    }
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_page_id);  //获取新根页的写锁
    auto new_root_page = new_root_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    ctx.write_set_.push_back(std::move(new_root_guard));  //将新根页写锁存入上下文
    new_root_page->Init(internal_max_size_);              //初始化新根页

    //设置新根页的第一个键和子节点
    new_root_page->SetSize(2);                           //更新新根页的大小
    new_root_page->SetKeyAt(1, new_key);                 //设置新键
    new_root_page->SetValueAt(0, first_split_page_id);   //设置旧根页的页 ID
    new_root_page->SetValueAt(1, second_split_page_id);  //设置新叶子页的页 ID

    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;  //更新头页的根页 ID

    ctx.write_set_.clear();  //清除写锁
  }

  return true;  //返回 true，表示插入成功
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  Context ctx;

  //读头页
  ReadPageGuard op_header_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = op_header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;  //获取根页 ID

  //如果树为空，直接返回
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;  //树为空，直接返回
  }

  //不为空
  /**
   * 1.乐观锁，假设删除不需要合并
   */
  //先找到要删除的叶子节点
  BPlusTreePage *op_write_page = nullptr;                      //用于记录要写入的页
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));  //读取根页
  auto op_now_page = ctx.read_set_.back().As<BPlusTreePage>();

  //如果根页就是叶子页，升级其读锁为写锁
  if (op_now_page->IsLeafPage()) {
    ctx.read_set_.pop_back();                                      //释放根页的读锁
    ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));  //获取根页的写锁
    op_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  }
  op_header_guard.Drop();  //释放头页的读锁，因为后续操作不需要修改头页,提高并发

  page_id_t op_now_page_id = ctx.root_page_id_;  //当前页的页 ID

  //如果根页不是叶子页，逐层向下查找，直到找到叶子页
  while (!op_now_page->IsLeafPage()) {
    int index = KeyBinarySearch(op_now_page, key);  //在当前页中二分查找键
    if (index == -1) {
      return;  //如果没有找到，直接返回
    }
    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(op_now_page);
    op_now_page_id = internal_page->ValueAt(index);           //获取子节点的页 ID
    ctx.read_set_.push_back(bpm_->ReadPage(op_now_page_id));  //读取子节点页面
    op_now_page = ctx.read_set_.back().As<BPlusTreePage>();   //更新当前页为子节点页面

    if (op_now_page->IsLeafPage()) {                              //如果到达叶子页
      ctx.read_set_.pop_back();                                   //释放叶子页的读锁
      ctx.write_set_.push_back(bpm_->WritePage(op_now_page_id));  //获取叶子页的写锁
      op_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    }
    ctx.read_set_.pop_front();  //释放父节点的读锁
  }

  op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //获取当前页为叶子页

  //到达叶子页后，开始查找键值对
  //如果节点无需合并以及借用，直接获取写锁删除
  if (op_write_page->GetSize() > op_write_page->GetMinSize()) {  //如果叶子页的大小小于最小值
    int delete_index = KeyBinarySearch(op_write_page, key);      //在叶子页中二分查找键的索引位置
    if (delete_index == -1) {
      return;  //如果没有找到，直接返回
    }
    //如果键存在，直接删除
    auto size = op_write_page->GetSize();  //获取叶子页的大小
    auto op_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(op_write_page);
    for (int i = delete_index; i < size - 1; ++i) {
      op_leaf_page->SetKeyAt(i, op_leaf_page->KeyAt(i + 1));      //将后面的键向前移动
      op_leaf_page->SetValueAt(i, op_leaf_page->ValueAt(i + 1));  //将后面的值向前移动
    }
    op_leaf_page->SetSize(size - 1);  //更新叶子页的大小
    return;                           //删除成功，直接返回
  }

  //如果叶子页的大小等于最小值，乐观锁不成立，需要使用悲观锁
  // ctx.read_set_.clear();  //清除所有读锁
  ctx.write_set_.clear();  //清除所有写锁

  /** 2.悲观锁 */
  //重新获得头页的写锁，查找要删除的叶子页
  WritePageGuard pe_header_guard = bpm_->WritePage(header_page_id_);               //获取头页的写锁
  ctx.header_page_ = std::make_optional(std::move(pe_header_guard));               //将头页写锁存入上下文
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;  //获取根页 ID

  //从根节点开始一路写锁到叶节点
  WritePageGuard root_guard = bpm_->WritePage(ctx.root_page_id_);   //获取根页的写锁
  ctx.write_set_.push_back(std::move(root_guard));                  //将根页写锁存入上下文
  auto pe_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //获取根页

  //在delete过程中，只有在root页的size大于2时，才不会发生根的变化
  if (pe_now_page->GetSize() > 2) {
    ctx.header_page_ = std::nullopt;  //清除头页的写锁，因为后续操作不需要修改头页,提高并发
  }

  //一路找到叶子节点
  while (!pe_now_page->IsLeafPage()) {
    int index = KeyBinarySearch(pe_now_page, key);  //在当前页中二分查找键
    if (index == -1) {
      return;  //如果没有找到，直接返回
    }
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pe_now_page);
    page_id_t pe_now_page_id = internal_page->ValueAt(index);  //获取子节点的页 ID

    ctx.write_set_.push_back(bpm_->WritePage(pe_now_page_id));  //获取子节点的写锁
    ctx.indexs_.push_back(index);                               //记录当前索引位置

    pe_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //更新当前页为子节点页面

    if (pe_now_page->GetSize() > pe_now_page->GetMinSize()) {  //如果中间过程一但有节点不满，根节点就不会发生变化
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;  //清除头页的写锁
      }
      while (ctx.write_set_.size() > 1) {  //并且此时可以释放上层页写锁，因为不会发生分裂
        ctx.write_set_.pop_front();        //释放上层页的写锁
      }
    }
  }

  //到达叶子页后，找到要删除的位置
  int delete_index = KeyBinarySearch(pe_now_page, key);  //在叶子页中二分查找键的索引位置
  if (delete_index == -1) {
    return;  //如果没有找到，直接返回
  }

  //如果键存在，直接删除
  int size = pe_now_page->GetSize();  //获取叶子页的大小
  auto pe_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(pe_now_page);
  for (int i = delete_index; i < size - 1; ++i) {
    pe_leaf_page->SetKeyAt(i, pe_leaf_page->KeyAt(i + 1));      //将后面的键向前移动
    pe_leaf_page->SetValueAt(i, pe_leaf_page->ValueAt(i + 1));  //将后面的值向前移动
  }
  pe_leaf_page->SetSize(size - 1);  //更新叶子页的大小

  /**
   * 删除操作完成后，如果删除页的结点数大于等于最小值，则完成删除操作，
   * 否则需要进行合并或借用操作
   */
  // 当前操作结点的page id，在原root变为空要被删除时，也是新root结点的page id，
  page_id_t pe_now_page_id = INVALID_PAGE_ID;

  while (
      !ctx.write_set_
           .empty()) {  //上面在找叶子页时，如果路径页中节点数量大于最小值，就直接释放写锁了，如果没有释放，说明需要借或合并

    //如果是根页，特殊处理
    if (ctx.write_set_.back().GetPageId() == ctx.root_page_id_) {
      auto pe_root_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //获取根页
      //如果root页为叶子页，单独处理，如果root为空，更新page id，如果不为空直接返回
      if (pe_root_page->IsLeafPage()) {
        if (pe_root_page->GetSize() == 0) {  //如果根页的大小为0，说明树为空
          ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;  //更新头页的根页 ID
          // bpm_->DeletePage(ctx.root_page_id_);  //删除根页
        }
        return;
      }

      //此时root为内部页，若此时root页的size大于2，说明删除操作不会影响根页，<=1是都需要删除，因为即使=1，内部页页不存储key
      if (pe_root_page->GetSize() <= 1) {
        ctx.write_set_.pop_back();                                                       //释放根页的写锁
        bpm_->DeletePage(ctx.root_page_id_);                                             //删除根页
        ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = pe_now_page_id;  //更新头页的根页 ID
      }
      return;  // root页的大小大于2，说明删除操作不会影响根页，直接返回
    }

    //若删除后的结点大于等于半满，则完成删除操作。这里相当于循环的出口
    if (pe_now_page->GetSize() >= pe_now_page->GetMinSize()) {  //如果叶子页的大小大于等于最小值
      return;                                                   //删除成功，直接返回
    }

    //获取当前页的父节点
    auto it = ctx.write_set_.rbegin();
    it++;  //跳过当前页，获取父节点
    auto parent_page = it->AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();  //获取父节点页面
    int index = ctx.indexs_.back();  //获取当前页在父节点中的索引位置

    //先判断是否可以向左借用
    if (index > 0) {
      WritePageGuard left_guard = bpm_->WritePage(parent_page->ValueAt(index - 1));  //获取左兄弟页的写锁
      ctx.write_set_.push_back(std::move(left_guard));                //将左兄弟页写锁存入上下文
      auto left_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //获取左兄弟页

      //如果左兄弟页的大小大于最小值,可以向左借用
      if (left_page->GetSize() > left_page->GetMinSize()) {
        BorrowFromLeftSibling(pe_now_page, left_page, parent_page, index);  //从左兄弟页借用
        return;
      }
      //如果左兄弟页的大小小于等于最小值，不能向左借用,释放左页写锁
      ctx.write_set_.pop_back();  //释放左兄弟页的写锁
    }

    //左不可借用，看右是否可以借用
    if (index < parent_page->GetSize() - 1) {
      WritePageGuard right_guard = bpm_->WritePage(parent_page->ValueAt(index + 1));  //获取右兄弟页的写锁
      auto right_page = right_guard.AsMut<BPlusTreePage>();                           //获取右兄弟页
      ctx.write_set_.push_back(std::move(right_guard));  //将右兄弟页写锁存入上下文

      //如果右兄弟页的大小大于最小值,可以向右借用
      if (right_page->GetSize() > right_page->GetMinSize()) {
        BorrowFromRightSibling(pe_now_page, right_page, parent_page, index);
        return;
      }
      //如果右兄弟页的大小小于等于最小值，不能向右借用,释放右页写锁
      ctx.write_set_.pop_back();  //释放右兄弟页的写锁
    }

    //左右都不可借，需要合并，(不可借一定可合并)
    //先判断左兄弟是否可以合并
    if (index > 0) {
      WritePageGuard left_guard = bpm_->WritePage(parent_page->ValueAt(index - 1));  //获取左兄弟页的写锁
      auto left_page = left_guard.AsMut<BPlusTreePage>();                            //获取左兄弟页
      ctx.write_set_.push_back(std::move(left_guard));  //将左兄弟页写锁存入上下文
      //合并左兄弟页和当前页
      MergeWithLeftSibling(pe_now_page, left_page, parent_page, index);  //合并左兄弟页和当前页
      pe_now_page_id = ctx.write_set_.back().GetPageId();                //更新当前页的页 ID为左兄弟的页id
      ctx.write_set_.pop_back();                                         //释放左兄弟页的写锁
      //获取被合并的页ID并删除页(也就是合并前的当前页)
      page_id_t deleted_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();          //释放当前页的写锁
      bpm_->DeletePage(deleted_page_id);  //删除被合并的页
    } else {                              //如果没有左兄弟页，直接与右兄弟页合并
      WritePageGuard right_guard = bpm_->WritePage(parent_page->ValueAt(index + 1));  //获取右兄弟页的写锁
      auto right_page = right_guard.AsMut<BPlusTreePage>();                           //获取右兄弟页
      ctx.write_set_.push_back(std::move(right_guard));  //将右兄弟页写锁存入上下文
      //合并右兄弟页和当前页
      MergeWithRightSibling(pe_now_page, right_page, parent_page, index);  //合并右兄弟页和当前页
      //获取被合并的页ID并删除页(也就是合并前的当前页)
      page_id_t deleted_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();                           //释放右兄弟页的写锁
      pe_now_page_id = ctx.write_set_.back().GetPageId();  //更新当前页的页 ID为右兄弟的页id
      ctx.write_set_.pop_back();                           //释放当前页的写锁
      bpm_->DeletePage(deleted_page_id);                   //删除被合并的页
    }

    //当前页处理完成之后，消除ctx.indexes_.back()，并更新now_page
    ctx.indexs_.pop_back();                                      //移除当前索引位置
    pe_now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();  //获取下一个当前页
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;

  //读头页
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();  //转为 BPlusTreeHeaderPage 类型
  ctx.root_page_id_ = header_page->root_page_id_;             //获取根页 ID
  header_guard.Drop();                                        //释放头页的读锁

  //如果树为空，直接返回空迭代器
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(), -1);  //树为空，返回index = -1
  }

  //不为空,找到最左侧的叶子页
  ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);  //读取根页
  auto now_page = page_guard.As<BPlusTreePage>();                //转为 BPlusTreePage 类型
  while (!now_page->IsLeafPage()) {                              //如果不是叶子页，继续向下查找
    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);
    page_id_t leftmost_child_id = internal_page->ValueAt(0);  //获取最左侧子节点的页 ID
    page_guard = bpm_->ReadPage(leftmost_child_id);           //读取最左侧子节点
    now_page = page_guard.As<BPlusTreePage>();                //获取最左侧子节点
  }

  //到达叶子页后，返回一个新的索引迭代器
  return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), 0);  //返回索引迭代器，初始位置为叶子页的第一个键
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;

  //读头页
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();  //转为 BPlusTreeHeaderPage 类型
  ctx.root_page_id_ = header_page->root_page_id_;             //获取根页 ID
  header_guard.Drop();                                        //释放头页的读锁

  //如果树为空，直接返回空迭代器
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(), -1);  //树为空，返回index = -1
  }

  //不为空,找到包含输入键的叶子页
  ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);  //读取根页
  auto now_page = page_guard.As<BPlusTreePage>();                //转为 BPlusTreePage 类型
  while (!now_page->IsLeafPage()) {                              //如果不是叶子页，继续向下查找

    int index = KeyBinarySearch(now_page, key);  //在当前内部页中二分查找

    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);

    if (index == -1) {
      return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(), -1);  //如果没有找到，返回空迭代器
    }

    page_id_t child_page_id = internal_page->ValueAt(index);  //获取子节点的页 ID
    page_guard = bpm_->ReadPage(child_page_id);               //读取子节点
    now_page = page_guard.As<BPlusTreePage>();                //获取子节点页面
  }

  //到达叶子页后，查找键的位置
  int index = KeyBinarySearch(now_page, key);  //在叶子页中二分查找键的索引位置
  if (index == -1) {
    return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(), -1);  //如果没有找到，返回空迭代器
  }

  return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), index);  //返回索引迭代器，初始位置为叶子页中键的索引位置
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(), -1);  //返回一个空的索引迭代器，表示结束
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);        //读头页
  auto header_page = guard.As<BPlusTreeHeaderPage>();  //转为 BPlusTreeHeaderPage 类型
  return header_page->root_page_id_;                   //返回根页 ID
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) const -> int {
  //二分查找键在页中的位置
  int left;
  int right;

  //叶子页和内部页的key存储不一样，因为内部页的keys[0]不存数据
  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
    left = 0;
    right = leaf_page->GetSize() - 1;  //叶子页的键从 0 开始到 size - 1
    while (left <= right) {
      int mid = left + (right - left) / 2;
      if (comparator_(key, leaf_page->KeyAt(mid)) == 0) {
        return mid;  //找到相等的键，返回索引
      }
      if (comparator_(key, leaf_page->KeyAt(mid)) < 0) {
        right = mid - 1;  //如果中间键大于 key，向左查找
      } else {
        left = mid + 1;  //如果中间键小于 key，向右查找
      }
    }
  } else {
    auto internal_page = static_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    left = 1;                              //内部页的键从 1 开始到 size
    right = internal_page->GetSize() - 1;  //内部页的键从 1 开始到 size - 1
    int size = internal_page->GetSize();

    if (comparator_(key, internal_page->KeyAt(left)) < 0) {
      return 0;
    }

    while (left <= right) {
      int mid = left + (right - left) / 2;
      if (comparator_(internal_page->KeyAt(mid), key) <= 0) {  //要查找的key在mid的右边或等于mid
        if (mid + 1 >= size || comparator_(internal_page->KeyAt(mid + 1), key) > 0) {
          return mid;  //如果下一个键大于 key，返回当前索引
        }
        left = mid + 1;   //否则继续向右查找
      } else {            //要查找的key在mid的左边
        right = mid - 1;  //向左查找
      }
    }
  }
  return -1;  //如果没有找到，返回 -1
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IndexBinarySearchLeaf(const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page,
                                           const KeyType &key) const -> int {
  //二分查找函数，在叶子中查找key插入的位置
  int left = 0;
  int right = page->GetSize() - 1;
  int size = page->GetSize();

  if (comparator_(key, page->KeyAt(left)) < 0) {
    return 0;  //如果第一个键大于 key，返回 0
  }

  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(page->KeyAt(mid), key) < 0) {
      if (mid + 1 >= size || comparator_(page->KeyAt(mid + 1), key) >= 0) {
        return mid + 1;  //如果下一个键大于 key，返回 mid + 1
      }
      left = mid + 1;  //如果中间键小于 key，向右查找
    } else {
      right = mid - 1;  //如果中间键大于 key，向左查找
    }
  }
  return -1;  //返回插入位置
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeftSibling(BPlusTreePage *now_page, BPlusTreePage *left_page,
                                           BPlusTreePage *parent_page, int index) {
  auto parent_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);

  int left_size = left_page->GetSize();  //获取左兄弟页的大小
  int now_size = now_page->GetSize();    //获取当前页的大小

  //当前页是叶子页还是内部页，处理不一样
  if (now_page->IsLeafPage()) {
    auto leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(now_page);
    auto left_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(left_page);

    //将当前页的所有key，value向后移动一位，并把左兄弟页的最后一个key，value放到当前页的第一个位置
    for (int i = now_size - 1; i >= 0; --i) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));      //将后面的键向后移动
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));  //将后面的值向后移动
    }
    leaf_page->SetKeyAt(0, left_leaf_page->KeyAt(left_size - 1));
    leaf_page->SetValueAt(0, left_leaf_page->ValueAt(left_size - 1));

    //更新当前两个页的大小
    leaf_page->SetSize(now_size + 1);  //更新当前页的大小
    left_leaf_page->SetSize(left_size - 1);

    //更新父节点的key
    parent_internal_page->SetKeyAt(index, leaf_page->KeyAt(0));
  } else {
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);
    auto left_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(left_page);

    //与叶子页不同，将当前页的所有key，value向后移动一位，并把父页的对应key放到当前页的第一个位置，把左兄弟页的最后一个key放到当前的key上，把左兄弟页的最后一个value放到当前页的第一个value上
    for (int i = now_size - 1; i >= 0; --i) {
      if (i > 0) {                                                //内部页key[0]不存数据，所以要单独处理
        internal_page->SetKeyAt(i + 1, internal_page->KeyAt(i));  //将后面的键向后移动
      }
      internal_page->SetValueAt(i + 1, internal_page->ValueAt(i));  //将后面的值向后移动
    }
    internal_page->SetKeyAt(1, parent_internal_page->KeyAt(index));  //将父节点的key放到当前页的第一个位置
    internal_page->SetValueAt(
        0, left_internal_page->ValueAt(left_size - 1));  //将左兄弟页的最后一个value放到当前页的第一个value上
    parent_internal_page->SetKeyAt(
        index, left_internal_page->KeyAt(left_size - 1));  //将左兄弟页的最后一个key放到当前页的第一个key上

    //更新当前两个页的大小
    internal_page->SetSize(now_size + 1);  //更新当前页的大小
    left_internal_page->SetSize(left_size - 1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRightSibling(BPlusTreePage *now_page, BPlusTreePage *right_page,
                                            BPlusTreePage *parent_page, int index) {
  auto parent_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);

  int right_size = right_page->GetSize();  //获取右兄弟页的大小
  int now_size = now_page->GetSize();      //获取当前页的大小

  //当前页是叶子页还是内部页，处理不一样
  if (now_page->IsLeafPage()) {
    auto leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(now_page);
    auto right_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(right_page);

    //将右兄弟的第一个节点的key，value放到当前页的最后一个位置
    leaf_page->SetKeyAt(now_size, right_leaf_page->KeyAt(0));
    leaf_page->SetValueAt(now_size, right_leaf_page->ValueAt(0));

    //将右兄弟页的所有key，value向前移动一位
    for (int i = 0; i < right_size - 1; ++i) {
      right_leaf_page->SetKeyAt(i, right_leaf_page->KeyAt(i + 1));      //将前面的键向前移动
      right_leaf_page->SetValueAt(i, right_leaf_page->ValueAt(i + 1));  //将前面的值向前移动
    }

    //更新当前两个页的大小
    leaf_page->SetSize(now_size + 1);  //更新当前页的大小
    right_leaf_page->SetSize(right_size - 1);

    //更新父节点的key
    parent_internal_page->SetKeyAt(index + 1, right_leaf_page->KeyAt(0));
  } else {  //当前页是内部页
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);
    auto right_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(right_page);

    //将父结点的index+1位置的key放到当前页的最后一个位置,并将右兄弟页的第一个value放到当前页的最后一个value上
    internal_page->SetKeyAt(now_size, parent_internal_page->KeyAt(index + 1));  //将父节点的key放到当前页的最后一个位置
    internal_page->SetValueAt(now_size,
                              right_internal_page->ValueAt(0));  //将右兄弟页的第一个value放到当前页的最后一个value上

    //将右兄弟页的第一个key,放到父结点上
    parent_internal_page->SetKeyAt(index + 1, right_internal_page->KeyAt(1));

    //将右兄弟页的所有key，value向前移动一位
    for (int i = 0; i < right_size; ++i) {
      if (i > 0) {  //内部页key[0]不存数据，所以要单独处理
        right_internal_page->SetKeyAt(i, right_internal_page->KeyAt(i + 1));  //将前面的键向前移动
      }
      right_internal_page->SetValueAt(i, right_internal_page->ValueAt(i + 1));  //将前面的值向前移动
    }

    //更新当前两个页的大小
    internal_page->SetSize(now_size + 1);  //更新当前页的大小
    right_internal_page->SetSize(right_size - 1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithLeftSibling(BPlusTreePage *now_page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                                          int index) {
  int left_size = left_page->GetSize();
  int now_size = now_page->GetSize();
  int parent_size = parent_page->GetSize();

  auto parent_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);
  //当前页是叶子页还是内部页，处理不一样
  if (now_page->IsLeafPage()) {
    auto leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(now_page);
    auto left_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(left_page);

    //将当前页的所有内容复制到左兄弟页
    for (int i = 0; i < now_size; ++i) {
      left_leaf_page->SetKeyAt(left_size + i, leaf_page->KeyAt(i));      //将当前页的键复制到左兄弟页
      left_leaf_page->SetValueAt(left_size + i, leaf_page->ValueAt(i));  //将当前页的值复制到左兄弟页
    }

    //更新左兄弟页的大小
    left_leaf_page->SetSize(left_size + now_size);

    //更新左兄弟的next_page_id
    left_leaf_page->SetNextPageId(leaf_page->GetNextPageId());

  } else {  //如果是内部页
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);
    auto left_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(left_page);

    KeyType mid_key = parent_internal_page->KeyAt(index);  //获取父结点上当前页与左兄弟页的中间key

    //先将父结点的index位置的key放到左兄弟页的最后一个位置
    left_internal_page->SetKeyAt(left_size, mid_key);  //将父节点的key放到左兄弟页的最后一个位置
    left_internal_page->SetValueAt(left_size,
                                   internal_page->ValueAt(0));  //将当前页的第一个value放到左兄弟页的最后一个value上

    //将当前页的所有内容复制到左兄弟页
    for (int i = 1; i < now_size; ++i) {
      left_internal_page->SetKeyAt(left_size + i, internal_page->KeyAt(i));  //将当前页的键复制到左兄弟页
      left_internal_page->SetValueAt(left_size + i, internal_page->ValueAt(i));  //将当前页的值复制到左兄弟页
    }

    //更新左兄弟页的大小
    left_internal_page->SetSize(left_size + now_size);  //更新左兄弟页的大小
  }

  //处理父页，父页少一个key,将index之后的key和value向前移动一位
  for (int i = index; i < parent_size - 1; ++i) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));      //将后面的键向前移动
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));  //将后面的值向前移动
  }
  //更新父页的大小
  parent_internal_page->SetSize(parent_size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithRightSibling(BPlusTreePage *now_page, BPlusTreePage *right_page,
                                           BPlusTreePage *parent_page, int index) {
  int right_size = right_page->GetSize();
  int now_size = now_page->GetSize();
  int parent_size = parent_page->GetSize();

  auto parent_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);

  //当前页是叶子页还是内部页，处理不一样
  if (now_page->IsLeafPage()) {
    auto leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(now_page);
    auto right_leaf_page = static_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(right_page);
    //将右兄弟页的所有内容复制到当前页
    for (int i = 0; i < right_size; ++i) {
      leaf_page->SetKeyAt(now_size + i, right_leaf_page->KeyAt(i));      //将右兄弟页的键复制到当前页
      leaf_page->SetValueAt(now_size + i, right_leaf_page->ValueAt(i));  //将右兄弟页的值复制到当前页
    }
    //更新当前页的大小
    leaf_page->SetSize(now_size + right_size);
    //更新当前页的next_page_id
    leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());  //更新当前页的next_page_id
  } else {                                                       //内部页
    auto internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(now_page);
    auto right_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(right_page);

    KeyType mid_key = parent_internal_page->KeyAt(index + 1);  //获取父结点上当前页与有兄弟页的中间key

    //将父结点的index+1位置的key放到当前页的最后一个位置
    internal_page->SetKeyAt(now_size, mid_key);  //
    internal_page->SetValueAt(now_size,
                              right_internal_page->ValueAt(0));  //将右兄弟页的第一个value放到当前页的最后一个value上

    //将右兄弟页的所有内容复制到当前页
    for (int i = 1; i < right_internal_page->GetSize(); ++i) {
      internal_page->SetKeyAt(now_size + i, right_internal_page->KeyAt(i));  //将右兄弟页的键 复制到当前页
      internal_page->SetValueAt(now_size + i, right_internal_page->ValueAt(i));  //将右兄弟页的值复制到当前页
    }

    //更新当前页的大小
    internal_page->SetSize(now_size + right_size);  //更新当前页的大小
  }

  //处理父页，父页少一个key,将index之后的key和value向前移动一位
  for (int i = index + 1; i < parent_size - 1; ++i) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));      //将后面的键向前移动
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));  //将后面的值向前移动
  }
  //更新父页的大小
  parent_internal_page->SetSize(parent_size - 1);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
