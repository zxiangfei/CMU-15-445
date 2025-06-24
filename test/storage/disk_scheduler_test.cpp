/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-20 18:53:58
 * @FilePath: /CMU-15-445/test/storage/disk_scheduler_test.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager_test.cpp
//
// Identification: test/storage/disk/disk_scheduler_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <future>  // NOLINT
#include <memory>

#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, data, /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, buf, /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

}  // namespace bustub
