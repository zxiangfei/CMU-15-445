//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 */
/**
 * 用于管理内存帧和相关元数据的“BufferPoolManager”的辅助类
 * 此类表示“BufferPoolManager”将数据页存储到其中的内存帧的标头
 * 实际的内存帧不是直接存储在“FrameHeader”中，而是“FrameHead”指向帧的存储指针，并且是单独存储的
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;  //返回只读指针，指向当前帧中存储的页面数据
  auto GetDataMut() -> char *;           //返回可写指针，指向当前帧中存储的页面数据
  void Reset();  //清空数据（memset 0）、重置 pin_count_、is_dirty_，为一个空闲帧做准备

  /** @brief The frame ID / index of the frame this header represents. */
  const frame_id_t frame_id_;  //唯一标识这个帧在缓冲池数组中的索引

  /** @brief The readers / writer latch for this frame. */
  /**
   * ReadPageGuard 在构造时以共享（读）锁方式锁住它，析构时解锁
   * WritePageGuard 在构造时以独占（写）锁方式锁住它，并在析构时解锁
   */
  std::shared_mutex rwlatch_;  //用于控制多线程环境下对同一帧数据的并发访问

  /** @brief The number of pins on this frame keeping the page in memory. */
  std::atomic<size_t> pin_count_;  //记录当前有多少个 Guard（或其他逻辑）正在“pin”住（使用）这个帧。只有当 pin_count_ ==
                                   // 0 时，缓冲池才允许将这个帧驱逐出去

  /** @brief The dirty flag. */
  bool is_dirty_;  //标识这个帧中的页面数据自加载以来是否被修改过。驱逐或写回时，需要根据它决定是否要把页面写回磁盘

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  std::vector<char> data_;  //采用 std::vector<char> 动态分配一段固定大小（通常是 4KB），存放页面内容

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the `FrameHeader` is
   * currently storing. This might allow you to skip searching for the corresponding (page ID, frame ID) pair somewhere
   * else in the buffer pool manager...
   */
};

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 */
/**
 * 负责在内存中维护一个固定大小的缓冲池，用于存储从磁盘读取的页面，使用 LRU-K 算法管理页面置换
 *
 *
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;  //返回缓冲池中帧的数量，即 num_frames_
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

 private:
  /** @brief The number of frames in the buffer pool. */
  const size_t num_frames_;  //缓冲池帧总数

  /** @brief The next page ID to be allocated.  */
  std::atomic<page_id_t> next_page_id_;  //下一个要分配的页号

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually protects.
   */
  std::shared_ptr<std::mutex> bpm_latch_;  //保护整个 BufferPoolManager 内部状态的互斥锁

  /** @brief The frame headers of the frames that this buffer pool manages. */
  std::vector<std::shared_ptr<FrameHeader>> frames_;  //缓冲池中所有帧的头部信息，包含帧 ID、读写锁、pin 计数、脏标志等

  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;  //哈希表 page_id → frame_id，记录当前哪些页已经被加载到哪个帧

  /** @brief A list of free frames that do not hold any page's data. */
  std::list<frame_id_t> free_frames_;  //空闲帧列表，优先从这里分配新帧，不用驱逐

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  std::shared_ptr<LRUKReplacer> replacer_;  //指向 LRUKReplacer 的智能指针，用于挑选驱逐候选页

  /** @brief A pointer to the disk scheduler. */
  std::unique_ptr<DiskScheduler> disk_scheduler_;  //指向 LRUKReplacer 的智能指针，用于挑选驱逐候选页

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  LogManager *log_manager_ __attribute__((__unused__));  //（本任务 P1 暂不使用）日志管理器，用于实现事务 WAL 等

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free and has nothing
   * stored inside of it. Additionally, you may also want to implement a helper function that returns either a shared
   * pointer to a `FrameHeader` that already has a page's data stored inside of it, or an index to said `FrameHeader`.
   */
  auto CheckedPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::shared_ptr<FrameHeader>;
};
}  // namespace bustub
