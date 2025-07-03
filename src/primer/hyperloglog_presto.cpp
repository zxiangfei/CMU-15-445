#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  n_leading_bits_ = n_leading_bits;  //前缀位数 b
  if (n_leading_bits < 0) {
    return;  //如果前缀位数小于0，直接返回
  }
  num_buckets_ = 1 << n_leading_bits_;    //寄存器个数，等于2^b
  dense_bucket_.resize(num_buckets_, 0);  //初始化 dense_bucket_
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  auto hash = CalculateHash(val);   //计算哈希值
  auto bset = ComputeBinary(hash);  //将哈希值转换为二进制表示

  // 取前 n_leading_bits_ 位作为桶索引
  uint16_t index = 0;
  for (int i = 0; i < n_leading_bits_; ++i) {
    index |= (bset[BITSET_CAPACITY - 1 - i] << (n_leading_bits_ - i - 1));
  }

  //计算最右侧连续0的个数
  int trailing_zeros = 0;
  for (int i = 0; i < static_cast<int>(BITSET_CAPACITY) - n_leading_bits_; ++i) {
    if (!bset[i]) {
      trailing_zeros++;
    } else {
      break;
    }
  }

  //将 trailing_zeros 转换为二进制表示，并分为两部分
  auto dense_value = static_cast<uint8_t>(trailing_zeros & ((1U << DENSE_BUCKET_SIZE) - 1));  // bucket_size = 4 bits
  auto overflow_value = static_cast<uint8_t>(trailing_zeros >> DENSE_BUCKET_SIZE);            // 溢出部分

  //取出旧的桶值
  auto old_dense_value = static_cast<uint8_t>(dense_bucket_[index].to_ulong());
  auto old_overflow_value = 0;
  if (overflow_bucket_.find(index) != overflow_bucket_.end()) {
    old_overflow_value = static_cast<uint8_t>(overflow_bucket_[index].to_ulong());
  }
  uint32_t old_trailing_zeros = (old_overflow_value << DENSE_BUCKET_SIZE) | old_dense_value;

  //比较新的值和旧的值，更新桶
  if (trailing_zeros > static_cast<int>(old_trailing_zeros)) {
    dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(dense_value);  //更新 dense_bucket_
    if (overflow_value > 0) {
      //如果有溢出部分，更新 overflow_bucket_
      overflow_bucket_[static_cast<uint16_t>(index)] = std::bitset<OVERFLOW_BUCKET_SIZE>(overflow_value);
    } else {
      //如果没有溢出部分，删除对应的溢出桶
      overflow_bucket_.erase(static_cast<uint16_t>(index));
    }
  }
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeCardinality() -> void {
  if (n_leading_bits_ < 0) {
    return;  //如果前缀位数小于0，直接返回
  }

  double sum = 0.0;
  size_t m = dense_bucket_.size();  //寄存器个数

  for (size_t i = 0; i < m; ++i) {
    //计算每个桶的值
    auto dense_value = static_cast<uint8_t>(dense_bucket_[i].to_ulong());
    auto overflow_value = 0;
    if (overflow_bucket_.find(i) != overflow_bucket_.end()) {
      overflow_value = static_cast<uint8_t>(overflow_bucket_[i].to_ulong());
    }
    uint16_t trailing_zeros = (overflow_value << DENSE_BUCKET_SIZE) | dense_value;

    sum += 1.0 / std::pow(2, trailing_zeros);  //计算每个桶的2^-R_i
  }

  double estimated_cardinality = CONSTANT * m * m / sum;        //使用估计公式计算基数值
  cardinality_ = static_cast<uint64_t>(estimated_cardinality);  //将结果转换为无符号整数
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return {hash};  //将哈希值转换为二进制表示
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
