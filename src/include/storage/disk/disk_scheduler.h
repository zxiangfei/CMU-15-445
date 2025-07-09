/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-07-02 22:24:46
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-03 11:03:10
 * @FilePath: /CMU-15-445/src/include/storage/disk/disk_scheduler.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;  //标识是否是写请求

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;  //数据缓冲区，如果是 read → 读到 data_ 里；如果是 write → 从 data_ 写到磁盘

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;  //读/写磁盘的页号

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;  //回调，用于在请求完成时通知请求发起者
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
/**
 * 通过使用适当的DiskRequest对象,调用DiskScheduler::Schedule()来调度请求。
 * 调度程序维护一个后台工作线程，该线程使用磁盘管理器处理计划的请求。
 * 后台线程在DiskScheduler构造函数中创建，并在其析构函数中join()
 */
class DiskScheduler {  //负责调度磁盘请求
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);  //调度请求r

  /**
   * TODO(P1): Add implementation
   *
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread();  //开始后台工作线程

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };  //创建promise

  /**
   * @brief Increases the size of the database file to fit the specified number of pages.
   *
   * This function works like a dynamic array, where the capacity is doubled until all pages can fit.
   *
   * @param pages The number of pages the caller wants the file used for storage to support.
   */
  /**
   * 增加数据库文件的大小以适应指定的页数
   * 这个函数的工作方式类似于一个动态数组，它的容量会翻倍，直到所有页面都能容纳为止
   * @param pages 调用者希望文件用于存储的页数
   */
  void IncreaseDiskSpace(size_t pages) { disk_manager_->IncreaseDiskSpace(pages); }

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   * Also note: This is a no-op without a more complex data structure to track deallocated pages.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  /**
   * 释放磁盘上的页
   *
   * 目前 DiskManager::DeletePage(page_id) 在没有额外空闲页管理结构的情况下实际上是个
   * no-op（什么也不做），但接口预留了将来真正回收页、维护空闲页链表的能力
   *
   * 当 BufferPoolManager 要删除一个页（DeletePage()）时，会调用它，理论上是把对应磁盘页标记为可重用
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }
  /**
   * 上面这两个函数不会自己做调度队列之类的操作，它们只是把 “扩容”/“删页” 这两类 I/O 请求直接转给 DiskManager 去执行；
   * 在更完整的实现里，你也可以把它们包裹成 DiskRequest 丢给 DiskScheduler 异步处理
   */

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));  //  指向磁盘管理器的指针
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;  //磁盘操作请求队列
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;  //后台工作线程
};
}  // namespace bustub
