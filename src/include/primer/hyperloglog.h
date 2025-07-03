#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64  //哈希值64位

namespace bustub {

template <typename KeyType>
class HyperLogLog {
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT =
      0.79402;  //估计公式中的常数   cradinality = CONSTANT * m^2 / sum(2^-R_i) 其中m为寄存器个数，R_i为寄存器i的值

 public:
  /** @brief Disable default constructor. */
  HyperLogLog() = delete;

  /** @brief Parameterized constructor. */
  explicit HyperLogLog(int16_t n_bits);

  /**
   * @brief Getter value for cardinality.
   *
   * @returns cardinality value
   */
  auto GetCardinality() { return cardinality_; }

  /**
   * @brief Adds a value into the HyperLogLog.
   *
   * @param[in] val - value that's added into hyperloglog
   */
  auto AddElem(KeyType val) -> void;

  /**
   * @brief Function that computes cardinality.
   */
  auto ComputeCardinality() -> void;

 private:
  /**
   * @brief Calculates Hash of a given value.
   *
   * @param[in] val - value
   * @returns hash integer of given input value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
    } else {
      val_obj = Value(BIGINT, val);
    }
    return bustub::HashUtil::HashValue(&val_obj);
  }

  /**
   * @brief Function that computes binary.
   *
   *
   * @param[in] hash
   * @returns binary of a given hash
   */
  auto ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY>;

  /**
   * @brief Function that computes leading zeros.
   *
   * @param[in] bset - binary values of a given bitset
   * @returns leading zeros of given binary set
   */
  auto PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  /** @brief Cardinality value. */
  size_t cardinality_;

  /** @todo (student) can add their data structures that support HyperLogLog */
  int16_t n_bits_;                   //前缀位数b
  size_t m_;                         //寄存器个数，等于2^b
  std::vector<uint64_t> registers_;  //寄存器数组，大小为 1 << n_bits_，即2^b个寄存器
};

}  // namespace bustub
