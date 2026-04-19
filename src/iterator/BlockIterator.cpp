#include "../../include/iterator/BlockIterator.h"
#include <spdlog/spdlog.h>
#include "../../include/storage/Block.h"
#include <optional>
#include <string>
#include <print>
#include <utility>

BlockIterator::BlockIterator() : block(nullptr), current_index(0), tranc_id_(0) {}
BlockIterator::BlockIterator(std::shared_ptr<Block> block_, const std::string& key,bool is_prefix)
    : block(block_), tranc_id_(0) {  // 初始化为0
  if (!block) {
    return;
  }
  if (is_prefix) {
    auto iter = block->get_prefix_begin_offset_binary(key);
    if (iter.has_value()) {
      auto index = iter->second;
      current_index = index;
      update_current();
      return;
    }
  }
  auto inres = block_->get_offset_binary(key);
  if (inres.has_value()) {
    current_index = inres->second;
    update_current();
    return;
  }
  current_index = block->Offset_.size();
}
BlockIterator::BlockIterator(std::shared_ptr<Block> block_, size_t index)
    : block(block_), current_index(index), tranc_id_(0) {
  update_current();
}

bool BlockIterator::is_end() {
  if (block) {
    return current_index >= block->Offset_.size();
  }
  return true;
}
BlockIterator::con_pointer BlockIterator::operator->() {
  if (cached_value.has_value()) {
    return &(*cached_value);
  }
  return nullptr;
}
BlockIterator& BlockIterator::operator++() {
  if (block) {
    current_index++;
    update_current();
  }
  return *this;
}
BlockIterator::value_type BlockIterator::operator*() {
  if (!cached_value.has_value()) {
    update_current();
  }
  return *cached_value;
}

bool BlockIterator::operator==(const BlockIterator& other) const {
  return block == other.block && current_index == other.current_index &&
         tranc_id_ == other.tranc_id_;
}
auto BlockIterator::operator<=>(const BlockIterator& other) const -> std::strong_ordering {
  if (block != other.block) {
    return block <=> other.block;
  }
  if (current_index != other.current_index) {
    return current_index <=> other.current_index;
  }
  return tranc_id_ <=> other.tranc_id_;
}

BlockIterator::value_type BlockIterator::getValue() const {
  if (current_index < 0 || current_index >= block->Offset_.size()) {
    spdlog::info(
        "BlockIterator::value_type BlockIterator::getValue() Index out of range in BlockIterator");
    return {std::string(), std::string()};
  }
  return cached_value.value();
}
size_t BlockIterator::getIndex() const {
  return current_index;
}
uint64_t BlockIterator::get_cur_tranc_id() const {
  return tranc_id_;
}

std::shared_ptr<Block> BlockIterator::get_block() const {
  return block;
}
void BlockIterator::update_current() {
  cached_value = std::nullopt;  // 每次都清空缓存
  if (block && current_index < block->Offset_.size()) {
    auto offset  = block->Offset_[current_index];
    auto entry   = block->get_entry(offset);
    tranc_id_    = entry->tranc_id;
    cached_value = std::make_pair(entry->key, entry->value);
  }
  else {
  tranc_id_=0;
  }
}
