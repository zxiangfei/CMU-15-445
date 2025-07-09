//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class FrameHeader;

/**
 * @brief An RAII object that grants thread-safe read access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `ReadPageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With `ReadPageGuard`s, there can be multiple threads that share read access to a page's data. However, the existence
 * of any `ReadPageGuard` on a page implies that no thread can be mutating the page's data.
 */
/**
 * 对页面获取读锁保护
 * 允许多个线程共享读访问权限，但不能有线程修改页面数据
 * 只允许 BufferPoolManager 构造有效的 ReadPageGuard
 */
class ReadPageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `ReadPageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `ReadPageGuard` is through the buffer pool manager.
   */
  ReadPageGuard() = default;  //默认构造：创建一个 无效 的 guard（is_valid_==false），只是为了支持后续的移动赋值。

  //禁用拷贝，不允许两个 guard 同时管理同一把锁
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  //支持 移动语义，将锁或引用从一个 guard 转移到另一个
  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;

  //返回所管理页面的 ID
  auto GetPageId() const -> page_id_t;
  //返回所管理页面的数据指针（只读）
  auto GetData() const -> const char *;

  //模板方法，直接将数据指针转换为用户定义的结构 T*（方便按结构体访问页面）
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  //查询该页在内存中是否已被修改（dirty flag，由 FrameHeader 维护）
  auto IsDirty() const -> bool;
  //手动释放 guard：相当于提前调用析构逻辑，解锁并更新替换器状态
  void Drop();
  //析构时自动执行 Drop()（如果 is_valid_ == true），解锁并通知 LRUKReplacer 该帧可替换
  ~ReadPageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  // 只允许 BufferPoolManager 构造有效的 ReadPageGuard
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch);

  /** @brief The page ID of the page we are guarding. */
  page_id_t page_id_;  //当前页 ID

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  std::shared_ptr<FrameHeader> frame_;  //指向帧头，里面拥有实际的数据指针和 pin count/dirty flag

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  std::shared_ptr<LRUKReplacer> replacer_;  // LRUK 替换器，用于管理帧的可替换状态

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  std::shared_ptr<std::mutex> bpm_latch_;  //缓冲池的互斥锁，用于保护替换器状态更新

  /**
   * @brief The validity flag for this `ReadPageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  bool is_valid_{false};  //标志此 guard 是否处于“已初始化且未释放”状态，防止对无效 guard 误操作

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::shared_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */

  std::shared_lock<std::shared_mutex> lock_;
};

/**
 * @brief An RAII object that grants thread-safe write access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `WritePageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With a `WritePageGuard`, there can be only be 1 thread that has exclusive ownership over the page's data. This means
 * that the owner of the `WritePageGuard` can mutate the page's data as much as they want. However, the existence of a
 * `WritePageGuard` implies that no other `WritePageGuard` or any `ReadPageGuard`s for the same page can exist at the
 * same time.
 */
class WritePageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `WritePageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `WritePageGuard` is through the buffer pool manager.
   */
  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;
  auto GetPageId() const -> page_id_t;

  auto GetData() const -> const char *;  //只读指针
  auto GetDataMut() -> char *;           //可写指针

  /**
   * 提供读数据的类型转换
   */
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  /**
   *提供写数据的类型转换
   */
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }
  auto IsDirty() const -> bool;
  void Drop();
  ~WritePageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch);

  /** @brief The page ID of the page we are guarding. */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief The validity flag for this `WritePageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::unique_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */

  std::unique_lock<std::shared_mutex> lock_;
};

}  // namespace bustub
