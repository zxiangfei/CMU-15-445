/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-24 11:22:52
 * @FilePath: /CMU-15-445/src/storage/disk/disk_scheduler.cpp
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * // 构造：保存 disk_manager 指针，启动后台线程
 */
DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

/**
 * 析构函数：先发一个 nullopt 终止后台循环，再 join 线程
 */
DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * 调度一个请求：将其封装为 optional 并添加到channel队列
 */
void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional(std::move(r))); }

/**
 * 在request_queue_中没有nullptr时，一直循环运行(析构函数会插入nullptr)
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request_optional = request_queue_.Get();  //获取队列请求
    if (!request_optional.has_value()) {
      break;
    }                                                    //如果为nullptr，退出
    auto request = std::move(request_optional.value());  //不为nullptr，取出optional中的请求

    if (request.is_write_) {  //写请求    从 data_ 写到磁盘
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {  //读请求     读到 data_ 里
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }

    request.callback_.set_value(true);
  }
}

}  // namespace bustub
