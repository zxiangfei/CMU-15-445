//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

/**
 * p1
 * 在缓冲池中创建新页面。将page_id设置为新页面的id，或者如果所有frame当前都在使用中并且不可驱逐，则设置为nullptr
 * 您应该从空闲列表或替换器中选择替换frame（始终先从空闲列表中查找），然后调用AssignPage方法获取新的页面id。
 * 如果替换frame有脏页面，您应该先将其写回磁盘。您还需要为新页面重置内存和元数据
 *
 * 请记住通过调用replacer.SetEvictable(frame_id,
 * false)来“Pin”frame，以便替换器在BufferPoolManager“Unpin”之前不会驱逐frame。
 * 此外，请记住在替换器中记录frame的访问历史，以使lru-k算法能够正常工作
 */
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  frame_id_t frame_id;  //用于存储替换的frame id
  if (!this->free_list_.empty()) {  //首先先看空闲链表是否为空，不为空，就先从空闲链表中取缓冲池中的frame存储page data
    frame_id = this->free_list_.front();  // 获取空闲链表的第一个frame id
    this->free_list_.pop_front();         // 从空闲链表中移除该frame id
  } else {                                //如果空闲链表为空，则从替换器中选择一个frame进行替换
    if (!this->replacer_->Evict(&frame_id)) {
      return nullptr;  // 如果替换器也没有可驱逐的frame，则返回nullptr
    }
    //找到可替换的frame后，执行后续操作
    if (pages_[frame_id].is_dirty_) {  //如果当前frame中的页面是脏的，则需要将其写回磁盘
      auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
      auto futher = promise.get_future();               // 获取future对象，用于等待写入完成

      disk_scheduler_->Schedule({.is_write_ = true,                         // 设置请求为写入请求
                                 .data_ = pages_[frame_id].GetData(),       // 设置要写入的数据
                                 .page_id_ = pages_[frame_id].GetPageId(),  // 设置要写入的页面id
                                 .callback_ = std::move(promise)});         // 调度写入请求

      futher.get();                        // 等待写入完成
      pages_[frame_id].is_dirty_ = false;  // 重置脏标记
    }
    page_table_.erase(pages_[frame_id].GetPageId());  // 从页表中删除旧页面的id
  }

  //完成frame中旧page的处理后，将新page data写入frame中
  *page_id = AllocatePage();                 // 分配一个新的页面id
  page_table_.emplace(*page_id, frame_id);   // 将新页面的id和frame id添加到页表中
  pages_[frame_id].page_id_ = *page_id;      // 设置新页面的id
  pages_[frame_id].ResetMemory();            // 重置frame中的内存
  pages_[frame_id].pin_count_ = 1;           // 设置pin计数
  pages_[frame_id].is_dirty_ = false;        // 设置脏标记
  replacer_->RecordAccess(frame_id);         // 记录访问历史
  replacer_->SetEvictable(frame_id, false);  // 设置frame为不可驱逐状态

  return &pages_[frame_id];  // 返回新创建的页面
  //————————————————————————————————end——————————————————————————————
}

/**
 * p2
 * NewPage的包装器，返回的不是新页面的裸指针，而是BasicPageGuard结构
 * 功能与NewPage相同
 */
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

/**
 * p1
 * 从缓冲池中获取请求的页面。如果需要从磁盘中获取page_id，但所有帧当前都在使用中且不可驱逐，则返回nullptr
 *
 * 首先在缓冲池中搜索page_id
 * 如果未找到，请从空闲列表或替换器中选择一个替换帧，
 * 通过使用disk_scheduler_->Schedule调度读取DiskRequest从磁盘读取页面，并替换帧中的旧页面
 * 与NewPage（）类似，如果旧页面是脏的，则需要将其写回磁盘并更新新页面的元数据
 *
 * 需要禁用驱逐并记录frame的访问历史，就像NewPage()一样
 *
 */
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  frame_id_t frame_id;  // 用于存储访问的frame id

  // 在缓冲池中查找页面
  if (page_table_.find(page_id) != page_table_.end()) {  // 如果页面已存在于缓冲池中，返回对应的页面指针
    frame_id = page_table_[page_id];                     // 获取页面对应的frame id
    pages_[frame_id].pin_count_++;                       // 增加页面的pin计数
    replacer_->RecordAccess(frame_id);                   // 记录访问历史
    replacer_->SetEvictable(frame_id, false);            // 设置页面为不可驱逐状态
    return &pages_[frame_id];                            // 返回页面指针
  }

  // 如果页面不在缓冲池中，则需要从磁盘加载页面
  if (!this->free_list_.empty()) {  //首先先看空闲链表是否为空，不为空，就先从空闲链表中取缓冲池中的frame存储page data
    frame_id = this->free_list_.front();  // 获取空闲链表的第一个frame id
    this->free_list_.pop_front();         // 从空闲链表中移除该frame id
  } else {                                //如果空闲链表为空，则从替换器中选择一个frame进行替换
    if (!this->replacer_->Evict(&frame_id)) {
      return nullptr;  // 如果替换器也没有可驱逐的frame，则返回nullptr
    }
    //找到可替换的frame后，执行后续操作
    if (pages_[frame_id].is_dirty_) {  //如果当前frame中的页面是脏的，则需要将其写回磁盘
      auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
      auto futher = promise.get_future();               // 获取future对象，用于等待写入完成

      disk_scheduler_->Schedule({.is_write_ = true,                         // 设置请求为写入请求
                                 .data_ = pages_[frame_id].GetData(),       // 设置要写入的数据
                                 .page_id_ = pages_[frame_id].GetPageId(),  // 设置要写入的页面id
                                 .callback_ = std::move(promise)});         // 调度写入请求

      futher.get();                        // 等待写入完成
      pages_[frame_id].is_dirty_ = false;  // 重置脏标记
    }
    page_table_.erase(pages_[frame_id].GetPageId());  // 从页表中删除旧页面的id
  }

  //完成frame中旧page的处理后，将新page data写入frame中
  page_table_.emplace(page_id, frame_id);    // 将新页面的id和frame id添加到页表中
  page_table_[page_id] = frame_id;           // 将新页面的id与frame id关联
  pages_[frame_id].ResetMemory();            // 重置frame中的内存
  pages_[frame_id].pin_count_ = 1;           // 设置pin计数
  pages_[frame_id].is_dirty_ = false;        // 设置脏标记
  pages_[frame_id].page_id_ = page_id;       // 设置新页面的id
  replacer_->RecordAccess(frame_id);         // 记录访问历史
  replacer_->SetEvictable(frame_id, false);  // 设置frame为不可驱逐状态

  //从磁盘读取页面数据
  auto promise = disk_scheduler_->CreatePromise();                 // 创建一个promise对象，用于异步读取
  auto futher = promise.get_future();                              // 获取future对象，用于等待读取完成
  disk_scheduler_->Schedule({.is_write_ = false,                   // 设置请求为读取请求
                             .data_ = pages_[frame_id].GetData(),  // 设置要读取的数据存储位置
                             .page_id_ = page_id,                  // 设置要读取的页面id
                             .callback_ = std::move(promise)});    // 调度读取请求
  futher.get();                                                    // 等待读取完成

  return &pages_[frame_id];  // 返回新创建的页面
  //————————————————————————————————end——————————————————————————————
}

/**
 * p2
 * FetchPage的包装器，返回的不是裸指针，而是BasicPageGuard结构
 * 功能与FetchPage相同
 */
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};  //返回构造的BasicPageGuard
}
/**
 * p2
 * FetchPageRead的包装器，返回的不是裸指针，而是ReadPageGuard结构
 * 功能与FetchPage相同
 */
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {};  // 如果页面不存在，返回一个空的ReadPageGuard
  }
  page->RLatch();       // 获取读锁
  return {this, page};  //返回构造的ReadPageGuard
}
/**
 * p2
 * FetchPageWrite的包装器，返回的不是裸指针，而是WritePageGuard结构
 * 功能与FetchPage相同
 *
 * 如果调用FetchPageRead或FetchPageWrite，则预计返回的页面已经分别持有读或写锁
 */
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {};  // 如果页面不存在，返回一个空的WritePageGuard
  }
  page->WLatch();       // 获取写锁
  return {this, page};  //返回构造的WritePageGuard
}

/**
 * p1
 * 从缓冲池中取消绑定目标页面。如果page_id不在缓冲池中或其引脚计数已为0，则返回false
 * 减少页面的引脚数。如果引脚数达到0，则替换器应将帧逐出
 * 此外，在页面上设置脏标记，以指示页面是否被修改。
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  auto it = page_table_.find(page_id);  // 在页表中查找页面id
  if (it == page_table_.end()) {        // 如果页面id不在页表中，返回false
    return false;
  }
  if (pages_[it->second].pin_count_ <= 0) {  // 如果页面的引脚计数小于等于0，返回false
    return false;
  }
  pages_[it->second].pin_count_--;              // 减少页面的引脚计数
  if (pages_[it->second].pin_count_ == 0) {     // 如果引脚计数达到0
    replacer_->SetEvictable(it->second, true);  // 设置页面为可驱逐状态
  }
  pages_[it->second].is_dirty_ |= is_dirty;  // 设置脏标记
  return true;
  //————————————————————————————————end——————————————————————————————
}

/**
 * p1
 * 将指定页面无条件写回磁盘并清除 Dirty 标志。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  auto it = page_table_.find(page_id);  // 在页表中查找页面id
  if (it == page_table_.end()) {        // 如果页面id不在页表中，返回false
    return false;
  }

  auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
  auto futher = promise.get_future();               // 获取future对象，用于等待写入
  disk_scheduler_->Schedule({.is_write_ = true,
                             .data_ = pages_[it->second].data_,
                             .page_id_ = page_id,
                             .callback_ = std::move(promise)});  // 调度写入请求

  futher.get();                          // 等待写入完成
  pages_[it->second].is_dirty_ = false;  // 清除脏标记
  return true;                           // 返回true表示写入成功
  //————————————————————————————————end——————————————————————————————
}

/**
 * P1
 * 将缓冲池所有页写回磁盘
 */
void BufferPoolManager::FlushAllPages() {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  for (size_t i = 0; i < pool_size_; ++i) {  // 遍历缓冲池中的所有页面
    if (pages_[i].is_dirty_) {               // 如果页面是脏的
      lock.unlock();                         // 释放锁，避免在IO操作时阻塞其他线程
      FlushPage(pages_[i].GetPageId());      // 写回磁盘并清除脏标记
      lock.lock();                           // 重新获取锁
    }
  }
  //————————————————————————————————end——————————————————————————————
}

/**
 * p1
 * 从缓冲池中删除页面。如果page_id不在缓冲池中，则不执行任何操作并返回true
 * 如果页面被固定，无法删除，立即返回false
 *
 * 从页表中删除页后，停止在替换器中跟踪帧，并将帧添加回空闲列表
 * 此外，重置页面的内存和元数据。最后，您应该调用DeallocatePage来模拟释放磁盘上的页面
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  //——————————————————————————————start——————————————————————————————
  std::unique_lock<std::mutex> lock(latch_);  // 上锁，防止多线程访问冲突

  auto it = page_table_.find(page_id);  // 在页表中查找页面id
  if (it == page_table_.end()) {        // 如果页面id不在页表中，返回true
    return true;
  }
  frame_id_t fid = it->second;       // 先缓存
  if (pages_[fid].pin_count_ > 0) {  // 如果页面被固定，无法删除，返回false
    return false;
  }

  // 删除页面前先将其写回磁盘（如果是脏的）
  if (pages_[fid].is_dirty_) {
    lock.unlock();       // 释放锁，避免在IO操作时阻塞其他线程
    FlushPage(page_id);  // 写回磁盘并清除脏标记
    lock.lock();         // 重新获取锁
  }

  replacer_->Remove(fid);        // 从替换器中移除该frame
  free_list_.emplace_back(fid);  // 将frame添加回空闲列表
  page_table_.erase(it);         // 从页表中删除页面id
  pages_[fid].ResetMemory();     // 重置页面的内存和元数据

  DeallocatePage(page_id);  // 模拟释放磁盘上的页面
  return true;              // 返回true表示删除成功
  //————————————————————————————————end——————————————————————————————
}

/**
 * 分配一个新的页面id
 * 使用前应该上锁
 */
auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
