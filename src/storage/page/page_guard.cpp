/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-25 14:30:20
 * @FilePath: /CMU-15-445/src/storage/page/page_guard.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * 移动构造:把 that 的资源（bpm_、page_、is_dirty_）转移到新对象，that 置为空，防止重复 unpin 或标记
 * 当你调用BasicPageGuard(std::move(other_guard))时
 * 你期望新的guard将完全像其他guard一样工作。此外，旧的页面guard不应该可用
 * 例如，不应该可能在两个页面guard上调用.Drop()，并且pin计数减少2
 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

/**
 * 如果想在 guard 生命周期内提前释放页面，应调用此方法完成 unpin，并将内部状态重置为空
 * 释放page guard 时，应该清除所有内容（使页面guard不再有用），并告诉bpm_我们已经完成使用此页面
 */
void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);  // 调用 BufferPoolManager 的 UnpinPage 方法，解除页面的 pin
    page_ = nullptr;                       // 清除page指针，防止重复unpin
    bpm_ = nullptr;                        // 清除bpm_指针，防止重复 unpin
    is_dirty_ = false;                     // 重置脏标记
  }
}

/**
 * 移动赋值:先对自己当前持有的页面做 Drop，再将 that 的资源移入自己，并将 that 置空
 * 与移动构造类似，但这里需要先释放当前 guard 的资源。
 */
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();  // 先释放当前 guard 的资源
    // 转移 that 的资源到当前对象
    bpm_ = that.bpm_;            // 转移 bpm_ 指针
    page_ = that.page_;          // 转移 page_ 指针
    is_dirty_ = that.is_dirty_;  // 转移脏标记

    // 将 that 的指针置空，防止重复 unpin 或标记
    that.bpm_ = nullptr;     // 清空 that 的 bpm_ 指针
    that.page_ = nullptr;    // 清空 that 的 page_ 指针
    that.is_dirty_ = false;  // 重置 that 的脏标记
  }
  return *this;  // 返回当前对象的引用
}

/**
 * 析构函数:当 BasicPageGuard 对象生命周期结束时，自动调用 Drop() 方法
 */
BasicPageGuard::~BasicPageGuard() {
  Drop();  // 调用 Drop 方法释放资源
};         // NOLINT

/**
 * 升级 BasicPageGuard 为 ReadPageGuard
 * 在升级过程中，保持页面在缓冲池中不被驱逐，并使当前 guard 失效
 * 返回一个新的 ReadPageGuard 对象
 */
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();  // 获取页面的读锁，确保在升级过程中页面不会被修改
  // 升级为 ReadPageGuard
  ReadPageGuard read_guard(bpm_, page_);

  //不应该使用Drop()方法，因为它会导致页面被 unpin
  bpm_ = nullptr;     // 清空 bpm_ 指针，防止重复 unpin
  page_ = nullptr;    // 清空 page_ 指针，防止重复 unpin
  is_dirty_ = false;  // 重置脏标记，确保升级

  return read_guard;  // 返回新的 ReadPageGuard
}

/**
 * 升级 BasicPageGuard 为 WritePageGuard
 * 在升级过程中，保持页面在缓冲池中不被驱逐，并使当前 guard 失效
 * 返回一个新的 WritePageGuard 对象
 */
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();  // 获取页面的写锁，确保在升级过程中页面不会被其他线程修改
  // 升级为 WritePageGuard
  WritePageGuard write_guard(bpm_, page_);

  //不应该使用Drop()方法，因为它会导致页面被 unpin
  bpm_ = nullptr;     // 清空 bpm_ 指针，防止重复 unpin
  page_ = nullptr;    // 清空 page_ 指针，防止重复 unpin
  is_dirty_ = false;  // 重置脏标记，确保升级

  return write_guard;  // 返回新的 WritePageGuard
}

/**
 * 移动构造：同 BasicPageGuard，转移内部 guard_，并释放自己的旧资源
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {
}  //调用 BasicPageGuard 的移动构造函数,内部已经负责把 that（即源 BasicPageGuard）的指针置空、脏标记重置了

/**
 * 移动赋值：同 BasicPageGuard，先释放当前 guard 的资源，再转移 that 的资源
 * 注意：ReadPageGuard 继承自 BasicPageGuard，因此
 *       需要调用 BasicPageGuard 的移动赋值操作符
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();                           // 先释放当前 guard 的资源
    guard_ = std::move(that.guard_);  // 转移 that 的资源到当前对象
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();  // 释放页面的读锁
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();  // 调用 Drop 方法释放资源
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {
}  // 调用 BasicPageGuard 的移动构造函数,内部已经负责把 that（即源 BasicPageGuard）的指针置空、脏标记重置了

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();                           // 先释放当前 guard 的资源
    guard_ = std::move(that.guard_);  // 转移 that 的资源到当前对象
  }
  // that 的指针已经在 BasicPageGuard 的移动赋值中被置空
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();  // 释放页面的写锁
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();  // 调用 Drop 方法释放资源
}  // NOLINT

}  // namespace bustub
