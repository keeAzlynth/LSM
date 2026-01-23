#pragma once
#include "BlockIterator.h"
#include "memtable.h"
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

class SstIterator;
class Sstable;

std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<Sstable> sst, uint64_t tranc_id,
    std::function<int(const std::string&)> predicate);

bool operator==(const SstIterator& lhs, const SstIterator& rhs) noexcept;
class SstIterator : public BaseIterator {
  friend std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
      std::shared_ptr<Sstable> sst, uint64_t tranc_id,
      std::function<int(const std::string&)> predicate);

  friend class Sstable;
  friend bool operator==(const SstIterator& lhs, const SstIterator& rhs) noexcept;

 public:
  // 创建迭代器, 并移动到第一个key
  SstIterator();
  SstIterator(std::shared_ptr<Sstable> sst, uint64_t tranc_id);
  // 创建迭代器, 并移动到第指定key
  SstIterator(std::shared_ptr<Sstable> sst, const std::string& key, uint64_t tranc_id);
  SstIterator(std::shared_ptr<Sstable> sst, size_t block_idx, const std::string& key,
              uint64_t tranc_id);
  SstIterator(std::shared_ptr<Sstable> sst, std::shared_ptr<BlockIterator> block_iter,
              std::string key, uint64_t tranc_id);

  static std::optional<std::pair<SstIterator, SstIterator>> find_prefix_key(
      std::shared_ptr<Sstable> sst, std::string prefix, uint64_t tranc_id);

  void          set_end();
  void          seek(const std::string& key);
  std::string   key();
  std::string   value();
  bool          valid() const override;
  bool          isEnd() override;
  bool          exists_key_prefix(std::string key) const;
  BaseIterator& operator++() override;
  auto          operator<=>(BaseIterator& rhs) const -> std::strong_ordering;
  valuetype     operator*() const override;

  uint64_t     get_tranc_id() const override;
  IteratorType type() const override;

  BlockIterator::pointer   operator->() const;
  size_t                   get_block_idx() const;
  std::shared_ptr<Sstable> get_sstable() const;

  // static std::pair<MemTableIterator, MemTableIterator> merge_sst_iterator(
  //   std::vector<SstIterator> iter_vec, uint64_t tranc_id);

 private:
  std::shared_ptr<Sstable>         m_sst;
  std::shared_ptr<BlockIterator>   m_block_it;
  mutable std::optional<valuetype> cached_value;  // 缓存当前值
  size_t                           m_block_idx;
  uint64_t                         max_tranc_id_;
  void                             update_current() const;
  void                             set_block_idx(size_t idx);
  void                             set_block_it(std::shared_ptr<BlockIterator> it);
};
