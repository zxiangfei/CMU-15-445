#include "primer/hyperloglog.h"

namespace bustub {

/**
 * 构造函数
 */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  n_bits_ = n_bits;  // 前缀位数b
  if (n_bits < 0) {
    return;
  }
  m_ = 1 << n_bits_;         // 寄存器个数，等于2^b
  registers_.resize(m_, 0);  // 寄存器数组，大小为 1 << n_bits_，即2^b个寄存器
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return {hash};  // 将哈希值转换为二进制位集
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  uint64_t count = 0;
  for (int i = BITSET_CAPACITY - n_bits_ - 1; i >= 0; --i) {
    if (!bset[i]) {
      count++;
    } else {
      break;  //找到第一个1就停止
    }
  }
  return count + 1;  //返回左侧第一个1的位置，从1开始计数
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  const auto hash = CalculateHash(val);   //计算哈希值
  const auto bset = ComputeBinary(hash);  //将哈希值转换为二进制表示

  //取前n_bits_位作为寄存器索引
  size_t index = 0;
  for (int i = 0; i < n_bits_; ++i) {
    index |= (bset[BITSET_CAPACITY - 1 - i] << (n_bits_ - i - 1));
  }

  //剩下的哈希值用来计算
  // auto remaining_bits = bset >> n_bits_;

  //计算剩余部分的左侧第一个1的位置
  uint64_t position = PositionOfLeftmostOne(bset);

  registers_[index] = std::max(registers_[index], position);  //更新寄存器值
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  if (n_bits_ < 0) {
    return;
  }
  double sum = 0.0;
  for (const auto &reg : registers_) {
    sum += 1.0 / std::pow(2, reg);  //计算每个寄存器的2^-R_i
  }

  cardinality_ = static_cast<size_t>(CONSTANT * m_ * m_ / sum);  //使用估计公式计算基数值
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
