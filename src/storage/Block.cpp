#include "../../include/storage/Block.h"
#include <spdlog/spdlog.h>
#include "../../include/iterator/BlockIterator.h"
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <print>

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
    spdlog::info(
        "Block::decode(const std::vector<uint8_t>& encoded, bool with_hash) Encoded data too "
        "small");
    return nullptr;
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
std::string_view Block::get_key_view(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info(" Block::get_key(const std::size_t offset) {} Invaild offset to much{}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  return std::string_view(reinterpret_cast<const char*>(Data_.data() + offset + sizeof(uint16_t)),
                     key_len);
}
std::string Block::get_key(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info(" Block::get_key(const std::size_t offset) {} Invaild offset to much{}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(Data_.data() + offset + sizeof(uint16_t)),
                     key_len);
}
std::optional<std::pair<std::string, uint64_t>> Block::get_value(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_value(const std::size_t offset) {} Invalid offset too much {}", offset,
                 Offset_[Offset_.size() - 1]);
    return std::nullopt;
  }

  // 获取底层字节数据指针（unsigned char 类型）
  const char* base = reinterpret_cast<const char*>(Data_.data());

  // 读取 key_len
  uint16_t key_len;
  std::memcpy(&key_len, base + offset, sizeof(uint16_t));

  // value_len 紧跟在 key 数据之后
  const char* value_len_ptr = base + offset + sizeof(uint16_t) + key_len;
  uint16_t    value_len;
  std::memcpy(&value_len, value_len_ptr, sizeof(uint16_t));

  // value 数据的起始位置
  const char* value_start = value_len_ptr + sizeof(uint16_t);

  // tr 紧跟在 value 数据之后
  const char* tr_ptr = value_start + value_len;
  uint64_t    tr;
  std::memcpy(&tr, tr_ptr, sizeof(uint64_t));

  std::string value(reinterpret_cast<const char*>(value_start), value_len);
  return std::make_pair(value, tr);
}
std::shared_ptr<Block::Entry> Block::get_entry(std::size_t offset) {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_entry(std::size_t offset) {} Invaild offset to much{}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  auto key            = get_key(offset);
  auto value_tranc_id = get_value(offset);
  return std::make_shared<Block::Entry>(
      Block::Entry{key, std::string(value_tranc_id->first), value_tranc_id->second});
}

std::optional<uint64_t> Block::get_tranc_id(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_tranc_id(const std::size_t offset) {} Invaild offset {}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  size_t   pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  std::memcpy(&value_len, Data_.data() + pos, sizeof(uint16_t));
  pos += sizeof(uint16_t) + value_len;
  uint64_t tr;
  std::memcpy(&tr, Data_.data() + pos, sizeof(uint64_t));
  return tr;
}
std::string Block::get_first_key() {
  if (Offset_.empty()) {
    return std::string();
  }
  return get_key(Offset_[0]);
}

std::optional<std::pair<size_t, size_t>> Block::get_offset_binary(std::string_view key,
                                                                  const uint64_t   tranc_id) {
  if (Offset_.empty()) {
    return std::nullopt;
  }
  // 二分查找任意一个命中的 key。
  int left = 0;
  int right = static_cast<int>(Offset_.size()) - 1;
  int found = -1;
  while (left <= right) {
    int         mid     = left + (right - left) / 2;
    auto mid_key = get_key_view(Offset_[mid]);
    if (mid_key == key) {
      found = mid;
      break;
    } else if (mid_key < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (found == -1) {
    return std::nullopt;
  }

  // 同 key 的版本是连续存放的，并且按新到旧的顺序写入。
  int first = found;
  while (first > 0 && get_key_view(Offset_[first - 1]) == key) {
    --first;
  }

  if (tranc_id == 0) {
    return std::make_pair<size_t, size_t>(Offset_[first], first);
  }

  for (size_t idx = static_cast<size_t>(first); idx < Offset_.size(); ++idx) {
    if (get_key(Offset_[idx]) != key) {
      break;
    }
    auto cur_tranc_id = get_tranc_id(Offset_[idx]);
    if (cur_tranc_id.has_value() && cur_tranc_id.value() <= tranc_id) {
      return std::make_pair(Offset_[idx], idx);
    }
  }
  return std::nullopt;
}

std::optional<std::pair<size_t, size_t>> Block::get_prefix_begin_offset_binary(
    std::string_view key_prefix) {
  if (Offset_.empty())
    return std::nullopt;

  // 优先检查完全匹配（并传入 tranc_id）
  auto exact = get_offset_binary(key_prefix);
  if (exact.has_value()) {
    auto index = exact.value().second;
    while (index != 0 && get_key_view(Offset_[index - 1]) == key_prefix) {
      index--;
    }
    return std::make_optional<std::pair<size_t, size_t>>(Offset_[index], index);
  }

  // 二分查找插入点（第一个 >= key_prefix）
  int left       = 0;
  int right      = Offset_.size() - 1;
  int result_idx = -1;
  while (left <= right) {
    int         mid     = left + (right - left) / 2;
    auto mid_key = get_key_view(Offset_[mid]);
    if (mid_key.starts_with(key_prefix)) {
      result_idx = mid;
      right      = mid - 1;  // 继续查找可能更早的匹配项
    } else if (mid_key < key_prefix) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (result_idx != -1) {
    return std::make_pair(Offset_[result_idx], result_idx);
  }
  return std::nullopt;
}

std::optional<std::pair<size_t, size_t>> Block::get_prefix_end_offset_binary(
    std::string_view key_prefix) {
  if (Offset_.empty()) return std::nullopt;

  size_t left = 0;
  size_t right = Offset_.size();
  size_t res_idx = Offset_.size();

  while (left < right) {
    size_t mid = left + (right - left) / 2;
    auto mid_key = get_key_view(Offset_[mid]);

    // 只要当前 mid_key 还是以前缀开头，或者是小于前缀的
    // 我们就往右找，直到找到第一个“大于前缀且不以前缀开头”的键
    if (mid_key.starts_with(key_prefix) || mid_key < key_prefix) {
      left = mid + 1;
    } else {
      res_idx = mid;
      right = mid;
    }
  }

  // res_idx 现在是第一个不属于该前缀的元素的索引
  // 我们返回它，作为 [begin, end) 的开区间终点
  if (res_idx > 0) {
    // 检查一下前一个元素是否真的匹配前缀，如果不匹配，说明整个 block 都没有
    auto last_match_key = get_key_view(Offset_[res_idx - 1]);
    if (last_match_key.starts_with(key_prefix)) {
      return std::make_pair(Offset_[res_idx - 1], res_idx);
    }
  }
  
  return std::nullopt;
}

std::vector<std::tuple<std::string, std::string, uint64_t>> Block::get_prefix_tran_id(
    std::string_view key, const uint64_t tranc_id) {
  std::vector<std::tuple<std::string, std::string, uint64_t>> retrieved_data;
  auto result1 = get_prefix_begin_offset_binary(key);
  if (!result1.has_value()) {
    return retrieved_data;
  }
  auto begin = std::make_shared<BlockIterator>(shared_from_this(), result1->second);
  if (result1.value().second == Offset_.size() - 1) {
    if (begin->get_cur_tranc_id() <= tranc_id) {
      auto entry = begin->getValue();
      retrieved_data.push_back({entry.first, entry.second, begin->get_cur_tranc_id()});
    }
    return retrieved_data;
  }
  auto result2 = get_prefix_end_offset_binary(key);

  // 如果 result2 没有值，说明所有剩余 key 都以前缀开头
  size_t end_idx;
  if (!result2.has_value()) {
    end_idx = Offset_.size();
  } else {
    end_idx = result2->second;
  }

  auto end = std::make_shared<BlockIterator>(
      shared_from_this(), end_idx);
  while (*begin != *end) {
    if (begin->get_cur_tranc_id() <= tranc_id) {
      auto entry = begin->getValue();
      retrieved_data.push_back({entry.first, entry.second, begin->get_cur_tranc_id()});
    }
    ++(*begin);
  }
  return retrieved_data;
}
std::optional<size_t> Block::get_offset(const std::size_t index) {
  if (index >= Offset_.size()) {
    spdlog::info("Block::get_offset(const std::size_t index) Index out of range");
    return std::nullopt;
  }
  return Offset_[index];
}
size_t Block::get_cur_size() const {
  return Data_.size() + Offset_.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

std::optional<std::pair<std::string, uint64_t>> Block::get_value_binary(std::string_view key,
                                                                        const uint64_t   tranc_id) {
  auto idx = get_offset_binary(key, tranc_id);
  if (idx.has_value()) {
    return get_value(idx->first);
  }
  return std::nullopt;
}

bool Block::KeyExists(std::string_view key) {
  auto idx = get_offset_binary(key);
  return idx.has_value();
}

std::pair<std::string, std::string> Block::get_first_and_last_key() {
  if (Offset_.empty()) {
    return {std::string(), std::string()};
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
void Block::print_debug() const {
  if (is_empty()) {
    return;
  }
  for (size_t i = 0; i < Offset_.size(); i++) {
    uint16_t key_len;
    std::memcpy(&key_len, Data_.data() + Offset_[i], sizeof(uint16_t));
    auto key = std::string(
        reinterpret_cast<const char*>(Data_.data() + Offset_[i] + sizeof(uint16_t)), key_len);
    size_t   pos = Offset_[i] + sizeof(uint16_t) + key_len;
    uint16_t value_len;
    std::memcpy(&value_len, Data_.data() + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);
    auto value = std::string(reinterpret_cast<const char*>(Data_.data() + pos), value_len);
    pos += value_len;
    uint64_t tr;
    std::memcpy(&tr, Data_.data() + pos, sizeof(uint64_t));
    std::print("Block Entry {}: key={}, value={}, tranc_id={}\n", i, key, value, tr);
  }
}

BlockIterator Block::get_iterator(std::string_view key, const uint64_t tranc_id) {
  auto idx = get_offset_binary(key, tranc_id);
  if (!idx.has_value()) {
    return end();
  }
  return BlockIterator(shared_from_this(), idx->second);
}
BlockIterator Block::begin() {
  return BlockIterator(shared_from_this(), 0);
}
BlockIterator Block::back(){
  return  BlockIterator(shared_from_this(),Offset_.size()-1);
}
BlockIterator Block::end() {
  return BlockIterator(shared_from_this(), Offset_.size());
}

std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_prefix_iterator(std::string key) {
  auto result1 = get_prefix_begin_offset_binary(key);
  if (!result1.has_value()) {
    return std::nullopt;
  }
  auto begin = std::make_shared<BlockIterator>(shared_from_this(), result1->second);
  if (result1->second == Offset_.size() - 1) {
    auto end =
        std::make_shared<BlockIterator>(shared_from_this(), Offset_.size());
    return std::make_pair(begin, end);
  }

  auto   result2 = get_prefix_end_offset_binary(key);
  size_t end_idx;

  // 如果 result2 没有值，说明所有剩余key都以前缀开头
  if (!result2.has_value()) {
    end_idx = Offset_.size();
  } else {
    end_idx = result2->second;
  }

  auto end = std::make_shared<BlockIterator>(
      shared_from_this(), end_idx);
  return std::make_pair(begin, end);
}
