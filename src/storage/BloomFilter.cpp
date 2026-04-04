
#include "../../include/storage/BloomFilter.h"
#include <cstring>
#include <stdexcept>
#include <string>
#include <cmath>

BloomFilter::BloomFilter(){};

// 构造函数，初始化布隆过滤器
// expected_elements: 预期插入的元素数量
// false_positive_rate: 允许的假阳性率
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements), false_positive_rate_(false_positive_rate) {
  // 计算布隆过滤器的位数组大小
  double m = -static_cast<double>(expected_elements) * std::log(false_positive_rate) /
             std::pow(std::log(2), 2);
  //-n*ln(p)/[ln(2)^2]：这是布隆过滤器理论中的最优位数公式.
  num_bits_ = static_cast<size_t>(std::ceil(m));
  // 向上取整,实际存储时，位数只能用整数，所以向上取整，确保不会小于理论值.
  //  计算哈希函数的数量
  num_hashes_ = static_cast<size_t>(std::ceil(m / expected_elements * std::log(2)));
  // 这是布隆过滤器理论中的最优哈希函数数量公式 k = (m/n) * ln(2).

  // 初始化位数组
  bits_.resize(num_bits_, false);
}

void BloomFilter::add(const std::string& key) {
  // 对每个哈希函数计算哈希值，并将对应位置的位设置为true
  for (size_t i = 0; i < num_hashes_; ++i) {
    bits_[hash(key, i)] = true;
  }
}

//  如果key可能存在于布隆过滤器中，返回true；否则返回false
bool BloomFilter::possibly_contains(const std::string& key) const {
  // 对每个哈希函数计算哈希值，检查对应位置的位是否都为true
  for (size_t i = 0; i < num_hashes_; ++i) {
    auto bit_idx = hash(key, i);
    if (!bits_[bit_idx]) {
      return false;
    }
  }
  return true;
}

// 清空布隆过滤器
void BloomFilter::clear() {
  bits_.assign(bits_.size(), false);
}

std::pair<size_t, size_t> BloomFilter::hash_pair(const std::string& key) const {
  size_t h1 = hasher_(key);
  // 使用更好的第二个哈希函数
  size_t h2 = hasher_(key + std::to_string(h1)) | 1;  // 确保 h2 为奇数
  return {h1, h2};
}

size_t BloomFilter::hash(const std::string& key, size_t idx) const {
  auto [h1, h2] = hash_pair(key);
  return (h1 + idx * h2) % num_bits_;
}

// 编码布隆过滤器为 std::vector<uint8_t>
std::vector<uint8_t> BloomFilter::encode() const {
  const size_t header_size = sizeof(expected_elements_) + sizeof(false_positive_rate_) +
                             sizeof(num_bits_) + sizeof(num_hashes_);
  const size_t num_bytes  = (num_bits_ + 7) / 8;
  const size_t total_size = header_size + num_bytes;

  std::vector<uint8_t> data(total_size);  // 直接分配确定大小
  auto*                ptr = data.data();

  // 直接内存拷贝头部数据
  std::memcpy(ptr, &expected_elements_, sizeof(expected_elements_));
  ptr += sizeof(expected_elements_);

  std::memcpy(ptr, &false_positive_rate_, sizeof(false_positive_rate_));
  ptr += sizeof(false_positive_rate_);

  std::memcpy(ptr, &num_bits_, sizeof(num_bits_));
  ptr += sizeof(num_bits_);

  std::memcpy(ptr, &num_hashes_, sizeof(num_hashes_));
  ptr += sizeof(num_hashes_);

  // 快速位编码
  encode_bits_fast(ptr, num_bytes);

  return data;
}

// 优化的解码函数
BloomFilter BloomFilter::decode(const std::vector<uint8_t>& data) {
  if (data.size() < sizeof(size_t) * 3 + sizeof(double)) {
    throw std::runtime_error("Invalid data size");
  }

  const auto* ptr = data.data();
  BloomFilter bf;

  // 直接内存拷贝解码
  std::memcpy(&bf.expected_elements_, ptr, sizeof(bf.expected_elements_));
  ptr += sizeof(bf.expected_elements_);

  std::memcpy(&bf.false_positive_rate_, ptr, sizeof(bf.false_positive_rate_));
  ptr += sizeof(bf.false_positive_rate_);

  std::memcpy(&bf.num_bits_, ptr, sizeof(bf.num_bits_));
  ptr += sizeof(bf.num_bits_);

  std::memcpy(&bf.num_hashes_, ptr, sizeof(bf.num_hashes_));
  ptr += sizeof(bf.num_hashes_);

  // 验证数据完整性
  const size_t expected_bytes = (bf.num_bits_ + 7) / 8;
  const size_t header_size    = ptr - data.data();
  if (data.size() != header_size + expected_bytes) {
    throw std::invalid_argument("Data size mismatch");
  }

  // 快速位解码
  bf.bits_.resize(bf.num_bits_);
  bf.decode_bits_fast(ptr, expected_bytes);

  return bf;
}

void BloomFilter::encode_bits_fast(uint8_t* ptr, size_t num_bytes) const {
  // 如果 bits_ 使用普通 bool 数组或 bitset，可以进一步优化
  for (size_t i = 0; i < num_bytes; ++i) {
    uint8_t      byte      = 0;
    const size_t bit_start = i * 8;

    // 手动展开最常见的情况
    if (bit_start < num_bits_ && bits_[bit_start])
      byte |= 0x01;
    if (bit_start + 1 < num_bits_ && bits_[bit_start + 1])
      byte |= 0x02;
    if (bit_start + 2 < num_bits_ && bits_[bit_start + 2])
      byte |= 0x04;
    if (bit_start + 3 < num_bits_ && bits_[bit_start + 3])
      byte |= 0x08;
    if (bit_start + 4 < num_bits_ && bits_[bit_start + 4])
      byte |= 0x10;
    if (bit_start + 5 < num_bits_ && bits_[bit_start + 5])
      byte |= 0x20;
    if (bit_start + 6 < num_bits_ && bits_[bit_start + 6])
      byte |= 0x40;
    if (bit_start + 7 < num_bits_ && bits_[bit_start + 7])
      byte |= 0x80;

    *ptr++ = byte;
  }
}

// 快速位解码
void BloomFilter::decode_bits_fast(const uint8_t* ptr, size_t num_bytes) {
  for (size_t i = 0; i < num_bytes; ++i) {
    const uint8_t byte      = *ptr++;
    const size_t  bit_start = i * 8;

    // 正确的位解码
    if (bit_start < num_bits_)
      bits_[bit_start] = (byte & 0x01) != 0;
    if (bit_start + 1 < num_bits_)
      bits_[bit_start + 1] = (byte & 0x02) != 0;
    if (bit_start + 2 < num_bits_)
      bits_[bit_start + 2] = (byte & 0x04) != 0;
    if (bit_start + 3 < num_bits_)
      bits_[bit_start + 3] = (byte & 0x08) != 0;
    if (bit_start + 4 < num_bits_)
      bits_[bit_start + 4] = (byte & 0x10) != 0;
    if (bit_start + 5 < num_bits_)
      bits_[bit_start + 5] = (byte & 0x20) != 0;
    if (bit_start + 6 < num_bits_)
      bits_[bit_start + 6] = (byte & 0x40) != 0;
    if (bit_start + 7 < num_bits_)
      bits_[bit_start + 7] = (byte & 0x80) != 0;
  }
}
