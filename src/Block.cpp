#include "../include/Block.h"
#include "../include/BlockIterator.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <utility>

class BlockIterator;
Block::Block(std::size_t capacity) : capcity(capacity) {}
Block::Block() : capcity(4096) {
  // 默认构造函数，初始化容量为4096
}

std::vector<uint8_t> Block::encode(bool with_hash) {
  // Data_ + offsets(uint16_t) + num(uint16_t) [+ hash(uint32_t)]
  size_t offsets_bytes = Offset_.size() * sizeof(uint16_t);
  size_t total         = Data_.size() * sizeof(uint8_t) + offsets_bytes + sizeof(uint16_t) +
                 (with_hash ? sizeof(uint32_t) : 0);
  std::vector<uint8_t> encoded(total, 0);

  std::memcpy(encoded.data(), Data_.data(), Data_.size() * sizeof(uint8_t));

  // write offsets as uint16_t (explicit conversion + check)
  size_t off_pos = Data_.size() * sizeof(uint8_t);
  memcpy(encoded.data() + off_pos, Offset_.data(), Offset_.size() * sizeof(uint16_t));

  // write num elements
  size_t   num_pos      = Data_.size() * sizeof(uint8_t) + Offset_.size() * sizeof(uint16_t);
  uint16_t num_elements = Offset_.size();
  memcpy(encoded.data() + num_pos, &num_elements, sizeof(uint16_t));

  // write hash if needed (hash over everything before hash)
  if (with_hash) {
    uint32_t hash_value = std::hash<std::string_view>{}(std::string_view(
        reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t)));
    std::memcpy(encoded.data() + encoded.size() - sizeof(uint32_t), &hash_value, sizeof(uint32_t));
  }
  return encoded;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t>& encoded, bool with_hash) {
  // 使用 make_shared 创建对象
  auto block = std::make_shared<Block>();

  // 1. 安全性检查
  if (with_hash && encoded.size() <= sizeof(uint16_t) + sizeof(uint32_t)) {
    throw std::runtime_error("Encoded data too small");
  }

  // 2. 读取元素个数
  uint16_t num_elements;
  size_t   num_elements_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_elements_pos -= sizeof(uint32_t);
    auto     hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_value;
    memcpy(&hash_value, encoded.data() + hash_pos, sizeof(uint32_t));

    uint32_t compute_hash = std::hash<std::string_view>{}(std::string_view(
        reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t)));
    if (hash_value != compute_hash) {
      throw std::runtime_error("Block hash verification failed");
    }
  }
  memcpy(&num_elements, encoded.data() + num_elements_pos, sizeof(uint16_t));

  // 4. 计算各段位置
  size_t offsets_section_start = num_elements_pos - num_elements * sizeof(uint16_t);

  // 5. 读取偏移数组
  block->Offset_.resize(num_elements);
  memcpy(block->Offset_.data(), encoded.data() + offsets_section_start,
         num_elements * sizeof(uint16_t));

  // 6. 复制数据段
  block->Data_.reserve(offsets_section_start);  // 优化内存分配
  block->Data_.assign(encoded.begin(), encoded.begin() + offsets_section_start);

  return block;
}

// safe get_key/get_value/get_tranc_id with bounds checks
std::string Block::get_key(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    std::print("Invaild offset to much");
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(Data_.data() + offset + sizeof(uint16_t)),
                     key_len);
}

std::string Block::get_value(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    std::print("Invaild offset to much");
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  uint16_t value_len;
  std::memcpy(&value_len, Data_.data() + offset + sizeof(uint16_t) + key_len, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(Data_.data() + offset + sizeof(uint16_t) +
                                                   key_len + sizeof(uint16_t)),
                     value_len);
}
std::shared_ptr<Block::Entry> Block::get_entry(std::size_t offset) {
  if (offset > Offset_[Offset_.size() - 1]) {
    std::print("Invaild offset to much");
  }
  auto key      = get_key(offset);
  auto value    = get_value(offset);
  auto tranc_id = get_tranc_id(offset);
  return std::make_shared<Block::Entry>(Block::Entry{key, value, tranc_id.value()});
}

std::optional<uint64_t> Block::get_tranc_id(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    std::print("Invaild offset to much");
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  size_t   pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  std::memcpy(&value_len, Data_.data() + pos, sizeof(uint16_t));
  pos += sizeof(uint16_t) + value_len;
  uint64_t tr;
  std::memcpy(&tr, Data_.data() + pos, sizeof(const uint64_t));
  return tr;
}
std::string Block::get_first_key() {
  if (Offset_.empty()) {
    return "";
  }
  return get_key(Offset_[0]);
}

std::optional<size_t> Block::get_idx_binary(const std::string& key, const uint64_t tranc_id) {
  if (Offset_.empty()) {
    return std::nullopt;
  }
  // 二分查找
  int left  = 0;
  int right = Offset_.size();
  while (left < right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);
    if (mid_key == key && get_tranc_id(Offset_[mid]).value() <= tranc_id) {
      return mid;
    } else if (mid_key < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (left < Offset_.size()) {
    if (get_key(Offset_[left]) == key && get_tranc_id(Offset_[left]).value() <= tranc_id) {
      return left;
    }
  } else {
    if (get_key(Offset_[left - 1]) == key && get_tranc_id(Offset_[left]).value() <= tranc_id) {
      return left - 1;
    }
  }
  // 如果没有找到完全匹配的键，返回 std::nullopt
  return std::nullopt;
}

std::optional<size_t> Block::get_prefix_begin_idx_binary(const std::string& key,
                                                         const uint64_t           tranc_id) {
  if (Offset_.empty())
    return std::nullopt;

  // 优先检查完全匹配（并传入 tranc_id）
  auto exact = get_idx_binary(key, tranc_id);
  if (exact.has_value())
    return exact.value();

  // 二分查找插入点（第一个 >= key）
  int left  = 0;
  int right = Offset_.size();
  while (left < right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);
    if (mid_key < key) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  if (left < Offset_.size()) {
    if (get_key(Offset_[left]).starts_with(key) &&
        get_tranc_id(Offset_[left]).value() <= tranc_id) {
      return left;
    }
  } else {
    if (get_key(Offset_[left - 1]).starts_with(key) &&
        get_tranc_id(Offset_[left - 1]).value() <= tranc_id) {
      return left - 1;
    }
  }
  return std::nullopt;
}

std::optional<size_t> Block::get_prefix_end_idx_binary(const std::string& key, const uint64_t tranc_id) {
  if (Offset_.empty()) {
    return std::nullopt;
  }
  const std::string want = key + '\xff';
  // 优先检查完全匹配（并传入 tranc_id）
  auto exact = get_idx_binary(want, tranc_id);
  if (exact.has_value())
    return exact.value() + 1;
  // 二分查找插入点：找到最后一个匹配前缀的位置
  // 查找策略：找到第一个 >= want 的位置，然后往前找一个匹配前缀的位置
  int left  = 0;
  int right = Offset_.size();
  while (left < right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);

    // 如果 mid_key 匹配前缀，继续往后找（可能有更多匹配的）
    if (mid_key.starts_with(key)) {
      if (get_tranc_id(Offset_[mid]).value() <= tranc_id) {
        left = mid + 1;  // 继续往后找
      } else {
        // 事务ID不符合，往前找
        right = mid;
      }
    } else if (mid_key < want) {
      // mid_key 小于 want，说明匹配前缀的项可能在后面
      left = mid + 1;
    } else {
      // mid_key >= want，说明匹配前缀的项在前面
      right = mid;
    }
  }

  // 现在 left 指向第一个不匹配前缀的位置
  // 需要往前找最后一个匹配前缀的位置
  if (left > 0) {
    left--;  // 往前移动一个位置
    if (get_key(Offset_[left]).starts_with(key) &&
        get_tranc_id(Offset_[left]).value() <= tranc_id) {
      return left + 1;  // 返回最后一个匹配位置 + 1
    }
  }
  if (get_key(Offset_[left]).starts_with(key) && get_tranc_id(Offset_[left]).value() <= tranc_id) {
    return left + 1;
  }
  return std::nullopt;
}
std::optional<size_t> Block::get_offset(const std::size_t index) {
  if (index >= Offset_.size()) {
    throw std::out_of_range("Index out of range");
  }
  return Offset_[index];
}
size_t Block::get_cur_size() const {
  return Data_.size() + Offset_.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

std::optional<std::string> Block::get_value_binary(const std::string& key) {
  auto idx = get_idx_binary(key);
  if (idx.has_value()) {
    return get_value(Offset_[idx.value()]);
  }
  return std::nullopt;
}

bool Block::KeyExists(const std::string& key) {
  auto idx = get_idx_binary(key);
  return idx.has_value();
}

std::pair<std::string, std::string> Block::get_first_and_last_key() {
  if (Offset_.empty()) {
    return {"", ""};
  }
  std::string first_key = get_key(Offset_[0]);
  std::string last_key  = get_key(Offset_[Offset_.size() - 1]);
  return {first_key, last_key};
}
bool Block::add_entry(const std::string& key, const std::string& value, const uint64_t tranc_id,
                      bool force_write) {
  if ((!force_write) &&
      (get_cur_size() + key.size() + value.size() + 3 * sizeof(uint16_t) > capcity) &&
      !Offset_.empty()) {
    return false;
  }
  // 计算entry大小：key长度(2B) + key + value长度(2B) + value + tranc_id
  size_t entry_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + value.size() + sizeof(const uint64_t);
  size_t old_size = Data_.size();
  Data_.resize(old_size + entry_size);

  // 写入key长度
  uint16_t key_len = key.size();
  memcpy(Data_.data() + old_size, &key_len, sizeof(uint16_t));

  // 写入key
  memcpy(Data_.data() + old_size + sizeof(uint16_t), key.data(), key_len);

  // 写入value长度
  uint16_t value_len = value.size();
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len, &value_len, sizeof(uint16_t));

  // 写入value
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len + sizeof(uint16_t), value.data(),
         value_len);
  // 写入tranc_id
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len + sizeof(uint16_t) + value_len,
         &tranc_id, sizeof(const uint64_t));
  // 记录偏移
  Offset_.push_back(old_size);
  return true;
}
bool Block::is_empty() const {
  return Data_.empty() && Offset_.empty();
}

BlockIterator Block::get_iterator(const std::string& key, const uint64_t tranc_id) {
  auto idx = get_idx_binary(key, tranc_id);
  if (!idx.has_value()) {
    return end();
  }
  return BlockIterator(shared_from_this(), idx.value(), tranc_id);
}
BlockIterator Block::begin() {
  auto shared = shared_from_this();
  return BlockIterator(shared, 0, 0);
}
BlockIterator Block::end() {
  auto shared = shared_from_this();
  return BlockIterator(shared, Offset_.size(), 0);
}

std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_prefix_iterator(std::string key, const uint64_t tranc_id) {
  auto result1 = get_prefix_begin_idx_binary(key, tranc_id);
  if (!result1.has_value()) {
    return std::nullopt;
  }
  auto begin = std::make_shared<BlockIterator>(shared_from_this(), result1.value(), tranc_id);
  if (result1.value() == Offset_.size() - 1) {
    auto end = std::make_shared<BlockIterator>(shared_from_this(), Offset_.size(), tranc_id, false);
    return std::make_pair(begin, end);
  }
  auto result2 = get_prefix_end_idx_binary(key, tranc_id);

  // 如果找不到 end 索引，使用 Offset_.size() 作为结束位置
  size_t end_index = result2.has_value() ? result2.value() : Offset_.size();
  auto   end = std::make_shared<BlockIterator>(shared_from_this(), end_index, tranc_id, false);
  return std::make_pair(begin, end);
}