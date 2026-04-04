#include "../../include/storage/BlockMeta.h"
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <vector>

BlockMeta::BlockMeta() : first_key_(""), last_key_(""), offset_(0) {}
BlockMeta::BlockMeta(std::string first_key, std::string last_key, size_t offset)
    : first_key_(first_key), last_key_(last_key), offset_(offset) {}

std::vector<uint8_t> BlockMeta::encode_meta_to_slice(std::vector<BlockMeta>& meta) {
  size_t               num_meta = meta.size();
  std::vector<uint8_t> slice;
  if (num_meta == 0) {
    return std::vector<uint8_t>{};
  }
  size_t total_size = sizeof(size_t);  // num
  for (auto metas : meta) {
    total_size += sizeof(size_t) + sizeof(uint16_t) + metas.first_key_.size() + sizeof(uint16_t) +
                  metas.last_key_.size();
  }
  total_size += sizeof(uint32_t);  // hash
  slice.resize(total_size);
  uint8_t* ptr = slice.data();
  memcpy(ptr, &num_meta, sizeof(size_t));
  ptr += sizeof(size_t);
  for (const auto& metas : meta) {
    auto offset = metas.offset_;
    memcpy(ptr, &offset, sizeof(size_t));
    ptr += sizeof(size_t);
    uint16_t first_key_size = metas.first_key_.size();
    memcpy(ptr, &first_key_size, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr, metas.first_key_.data(), first_key_size);
    ptr += first_key_size;

    uint16_t last_key_size = metas.last_key_.size();
    memcpy(ptr, &last_key_size, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr, metas.last_key_.data(), last_key_size);
    ptr += last_key_size;
  }
  uint8_t* hash_start = slice.data() + sizeof(size_t);
  uint8_t* hash_end   = ptr;
  size_t   hash_size  = hash_end - hash_start;  //==data.size();
  uint32_t hash       = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char*>(hash_start), hash_size));
  memcpy(ptr, &hash, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  return slice;
}

std::vector<BlockMeta> BlockMeta::decode_meta_from_slice(std::vector<uint8_t>& slice) {
  if (slice.empty()) {
    return {};
  }
  if (slice.size() < sizeof(size_t) * 2) {
    throw std::runtime_error("Slice is too small to contain the number of meta blocks");
  }
  uint8_t* ptr        = slice.data();
  size_t   total_size = slice.size();
  size_t   num_meta   = 0;
  memcpy(&num_meta, slice.data(), sizeof(size_t));
  ptr += sizeof(size_t);
  std::vector<BlockMeta> meta;
  for (int i{}; i < num_meta; i++) {
    size_t offset = 0;
    memcpy(&offset, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    uint16_t first_key_size = 0;
    memcpy(&first_key_size, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    std::string first_key(reinterpret_cast<char*>(ptr), first_key_size);
    ptr += first_key_size;

    uint16_t last_key_size = 0;
    memcpy(&last_key_size, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    std::string last_key(reinterpret_cast<char*>(ptr), last_key_size);
    ptr += last_key_size;
    meta.emplace_back(first_key, last_key, offset);
  }
  uint32_t hash = 0;
  memcpy(&hash, ptr, sizeof(uint32_t));
  uint8_t* hash_start = slice.data() + sizeof(size_t);
  uint8_t* hash_end   = ptr;
  ptr += sizeof(uint32_t);
  size_t   hash_size = hash_end - hash_start;
  uint32_t hash2     = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char*>(hash_start), hash_size));
  if (hash != hash2) {
    throw std::runtime_error("Hash mismatch: data may be corrupted");
  }
  return meta;
}