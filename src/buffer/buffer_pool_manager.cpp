//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
/**
 * 提供只读指针给读护卫
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
/**
 * 提供可写指针给写护卫
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
/**
 * 用于在驱逐帧时清空状态，准备装载新页面
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
/**
 * 返回缓冲池中帧的数量，即 num_frames_
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
/**
 * 分配一个新的页面 ID；
 * 找一个空闲或可驱逐帧；
 * 如果该帧含旧页，写回并从 page_table_ 移除；
 * 在该帧 Reset()，设置新的 page_id；
 * 更新 page_table_；
 * 返回 WritePageGuard 以让调用者立即写该页。
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::unique_lock<std::mutex> latch(*bpm_latch_);

  //找到BufferPoolManager中的一个帧用于存储新页面
  frame_id_t frame_id;
  if (!free_frames_.empty()) {  //如果有空闲帧，直接使用
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {  //否则需要驱逐一个帧
    auto frame_id_opt = replacer_->Evict();
    if (!frame_id_opt.has_value()) {  //如果没有可驱逐的帧，返回无效页号
      return INVALID_PAGE_ID;
    }
    frame_id = frame_id_opt.value();

    //找到page_table_中对应的旧页，删除并写回
    page_id_t old_page_id = INVALID_PAGE_ID;
    // 找到 frame_id 对应的 page_id
    for (auto &kv : page_table_) {
      if (kv.second == frame_id) {
        old_page_id = kv.first;
        break;
      }
    }
    //删除
    if (old_page_id != INVALID_PAGE_ID) {
      page_table_.erase(old_page_id);
    }
    //如果旧页是脏的，需要写回磁盘
    if (frames_[frame_id]->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
      auto future = promise.get_future();               // 获取future对象，用于等待写入完成
      disk_scheduler_->Schedule(DiskRequest{.is_write_ = true,
                                            .data_ = frames_[frame_id]->GetDataMut(),
                                            .page_id_ = old_page_id,
                                            .callback_ = std::move(promise)}  //调度写入请求
      );
      future.get();  // 等待写入完成
    }
    frames_[frame_id]->Reset();  //重置帧头部状态
  }

  //完成frame中旧page的处理后，将新page data写入frame中
  page_id_t new_page_id = next_page_id_.fetch_add(1);   //获取新的页号
  disk_scheduler_->IncreaseDiskSpace(new_page_id + 1);  //确保磁盘空间足够

  //将新页号和帧号映射关系添加到page_table_中
  page_table_.emplace(new_page_id, frame_id);

  //读新页数据
  auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
  auto future = promise.get_future();               // 获取future对象，用于等待写入完成
  disk_scheduler_->Schedule(DiskRequest{.is_write_ = true,
                                        .data_ = frames_[frame_id]->GetDataMut(),
                                        .page_id_ = new_page_id,
                                        .callback_ = std::move(promise)}  //调度写入请求
  );
  future.get();  // 等待写入完成

  //更新LRU-K替换器状态
  replacer_->RecordAccess(frame_id);        //记录访问
  replacer_->SetEvictable(frame_id, true);  //设置为不可驱逐

  return new_page_id;  //返回新页号
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
/**
 * 从缓冲池中删除页面。如果page_id不在缓冲池中，则不执行任何操作并返回true
 * 如果页面被固定，无法删除，立即返回false
 *
 * 从页表中删除页后，停止在替换器中跟踪帧，并将帧添加回空闲列表
 * 此外，重置页面的内存和元数据。最后，您应该调用DeallocatePage来模拟释放磁盘上的页面
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> latch(*bpm_latch_);

  // 检查页是否存在于缓冲池中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果页不在缓冲池中，直接返回 true
    return true;
  }

  // 获取帧 ID
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];
  // 检查页面是否被固定（pin）
  if (frame->pin_count_.load() > 0) {
    // 如果页面被固定，无法删除，返回 false
    return false;
  }

  // 从页表中删除页面,如果是脏页先写回
  if (frame->is_dirty_) {                             // 如果页面是脏的，需要写回磁盘
    auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
    auto future = promise.get_future();               // 获取future对象，用于等待写入完成
    disk_scheduler_->Schedule(DiskRequest{
        .is_write_ = true, .data_ = frame->GetDataMut(), .page_id_ = page_id, .callback_ = std::move(promise)}
                              //调度写入请求
    );
    future.get();  // 等待写入完成
  }
  // 从页表中删除页面
  page_table_.erase(page_id);
  // 将帧添加回空闲列表
  free_frames_.push_back(frame_id);
  // 重置页面的内存和元数据
  frame->Reset();
  // 模拟释放磁盘上的页面
  replacer_->Remove(frame_id);               // 停止在替换器中跟踪帧
  disk_scheduler_->DeallocatePage(page_id);  // 从磁盘上删除页面  ,可以不调用，p1默认不回收磁盘空间
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
/**
 * 类似于 WritePage  若参数页 ID 不合法或未分配，返回 std::nullopt
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::shared_ptr<FrameHeader> frame_header;

  {
    std::lock_guard<std::mutex> bpm_lock(*bpm_latch_);  // 锁住缓冲池管理器的互斥锁，确保替换器状态更新是线程安全的

    frame_header = CheckedPage(page_id, access_type);
    if (!frame_header) {
      return std::nullopt;  // 如果无法获取页面，则返回std::nullopt
    }

    frame_header->pin_count_++;                               // 增加 pin count，表示这个帧正在被使用
    frame_header->is_dirty_ = true;                           // 设置该帧为脏页，因为有写护卫在使用它
    replacer_->SetEvictable(frame_header->frame_id_, false);  // 设置该帧为不可驱逐状态，因为有写护卫在使用
  }

  // 创建一个WritePageGuard来保护页面数据
  return WritePageGuard(page_id, frame_header, replacer_, bpm_latch_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
/**
 * 对页面获取读锁保护
 *
 * 如果无法将页面数据带入内存，则此函数将返回std::nullopt。
 *
 * 页面数据只能通过页面保护访问
 * BufferPoolManager的用户应该根据需要获取ReadPageGuard或WritePageGuard，以确保对数据的访问是线程安全的。
 *
 * ReadPageGuard可以在不同线程中读取同一页面的数据，但所有数据访问必须是不可变的
 * 如果用户想要修改页面数据，则必须使用CheckedWritePage获取WritePageGuard
 */
/**
 * 类似于 ReadPage  若参数页 ID 不合法或未分配，返回 std::nullopt
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::shared_ptr<FrameHeader> frame_header;

  {
    std::unique_lock<std::mutex> latch(*bpm_latch_);

    frame_header = CheckedPage(page_id, access_type);
    if (!frame_header) {
      return std::nullopt;  // 如果无法获取页面，则返回std::nullopt
    }

    frame_header->pin_count_++;                               // 增加 pin count，表示这个帧正在被使用
    replacer_->SetEvictable(frame_header->frame_id_, false);  // 设置该帧为不可驱逐状态，因为有读护卫在使用
  }

  // 创建一个ReadPageGuard来保护页面数据
  return ReadPageGuard(page_id, frame_header, replacer_, bpm_latch_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
/**
 * 展开CheckedWritePage的内部值，如果存在的话。
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
/**
 * 展开CheckedReadPage的内部值，如果存在的话。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
/**
 * 将帧中 is_dirty_ 的页面写回磁盘并重置脏标志
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> latch(*bpm_latch_);

  // 检查页是否存在于缓冲池中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果页不在缓冲池中，直接返回 false
    return false;
  }

  // 获取帧 ID,检查是否是脏数据，如果是写回
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];
  if (frame->is_dirty_) {
    // 如果页面是脏的，需要写回磁盘
    auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
    auto future = promise.get_future();               // 获取future对象，用于等待写入完成
    disk_scheduler_->Schedule(DiskRequest{
        .is_write_ = true, .data_ = frame->GetDataMut(), .page_id_ = page_id, .callback_ = std::move(promise)}
                              //调度写入请求
    );
    future.get();              // 等待写入完成
    frame->is_dirty_ = false;  // 写回后重置脏标志
  }
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
/**
 * 将所有的页面数据从内存刷新到磁盘。
 */
void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> latch(*bpm_latch_);
  for (auto &kv : page_table_) {
    FlushPage(kv.first);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
/**
 * 返回该页当前 pin_count_（如果在缓冲池中）
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::unique_lock<std::mutex> latch(*bpm_latch_);

  // 检查页是否存在于缓冲池中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果页不在缓冲池中，返回 std::nullopt
    return std::nullopt;
  }

  // 获取帧 ID
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 返回 pin_count_
  return frame->pin_count_.load();
}

/**
 * 在 page_table_ 中查找；
 * 若不在，则：
 * * 看 free_frames_ 未空则取一帧；
 * * 否则用 replacer_->Evict() 获取可驱逐的帧 ID；
 * 刷新旧页（写回并 page_table_.erase()）；
 * 读磁盘新页到此帧；
 * 将该 frame->pin_count_++、replacer_->RecordAccess()、replacer_->SetEvictable(..., false)；
 * 返回此 FrameHeader 的 shared_ptr
 */
auto BufferPoolManager::CheckedPage(page_id_t page_id, AccessType access_type) -> std::shared_ptr<FrameHeader> {
  frame_id_t frame_id;

  //在缓冲池中查找页面
  if (page_table_.find(page_id) != page_table_.end()) {
    //如果页面已存在，返回对应的帧头
    frame_id = page_table_[page_id];
    auto frame = frames_[frame_id];
    // frame->pin_count_++;  //增加pin计数
    // replacer_->RecordAccess(frame_id);  //记录访问
    // replacer_->SetEvictable(frame_id, false);  //设置为不可驱逐
    return frame;
  }

  //如果页面不存在，尝试从磁盘读取
  if (!free_frames_.empty()) {
    frame_id = this->free_frames_.front();  // 获取空闲链表的第一个frame id
    this->free_frames_.pop_front();         // 从空闲链表中移除该frame id
  } else {                                  //如果没有空闲帧，需要驱逐一个帧
    auto frame_id_opt = replacer_->Evict();
    if (!frame_id_opt.has_value()) {  //如果没有可驱逐的帧，返回无效页号
      return nullptr;
    }
    frame_id = frame_id_opt.value();

    //找到page_table_中对应的旧页，删除并写回
    page_id_t old_page_id = INVALID_PAGE_ID;
    // 找到 frame_id 对应的 page_id
    for (auto &kv : page_table_) {
      if (kv.second == frame_id) {
        old_page_id = kv.first;
        break;
      }
    }
    //删除
    if (old_page_id != INVALID_PAGE_ID) {
      page_table_.erase(old_page_id);
    }
    //如果旧页是脏的，需要写回磁盘
    if (frames_[frame_id]->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步写入
      auto future = promise.get_future();               // 获取future对象，用于等待写入完成
      disk_scheduler_->Schedule(DiskRequest{.is_write_ = true,
                                            .data_ = frames_[frame_id]->GetDataMut(),
                                            .page_id_ = old_page_id,
                                            .callback_ = std::move(promise)}  //调度写入请求
      );
      future.get();  // 等待写入完成
    }
    frames_[frame_id]->Reset();  //重置帧头部状态
  }

  //完成frame中旧page的处理后，将新page data写入frame中
  disk_scheduler_->IncreaseDiskSpace(page_id + 1);  //确保磁盘空间足够
  //将新页号和帧号映射关系添加到page_table_中
  page_table_.emplace(page_id, frame_id);
  // frames_[frame_id]->is_dirty_ = false;  //新页默认不脏
  // frames_[frame_id]->pin_count_.store(1);  //pin计数置为1，表示新页被pin住

  //更新LRU-K替换器状态
  replacer_->RecordAccess(frame_id);        //记录访问
  replacer_->SetEvictable(frame_id, true);  //设置为不可驱

  //从磁盘中读数据
  auto promise = disk_scheduler_->CreatePromise();  // 创建一个promise对象，用于异步读取
  auto future = promise.get_future();               // 获取future对象，用于等待读取完成
  disk_scheduler_->Schedule(DiskRequest{.is_write_ = false,
                                        .data_ = frames_[frame_id]->GetDataMut(),
                                        .page_id_ = page_id,
                                        .callback_ = std::move(promise)}  //调度读取请求
  );
  future.get();              // 等待读取完成
  return frames_[frame_id];  //返回帧头
}

}  // namespace bustub
