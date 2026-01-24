#pragma once
#include "BaseIterator.h"
#include <array>
#include <atomic>
#include <memory>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "../include/Global.h"

const int MAX_LEVEL = 16;

class SkiplistIterator;
class Node {
 public:
  std::string                        key_;
  std::string                        value_;
  std::vector<std::shared_ptr<Node>> forward;
  uint64_t                           transaction_id;
  Node(const std::string& key, const std::string& value, int level,
       const uint64_t transaction_ids = 0)
      : key_(key), value_(value), forward(level, nullptr), transaction_id(transaction_ids) {}
  auto operator<=>(const Node& other) const;
};

bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;
class SkiplistIterator : public BaseIterator {
  friend class Skiplist;  // 让 Skiplist 可以访问私有成员
  friend bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;

 public:
  using valuetype = std::pair<std::string, std::string>;
  SkiplistIterator(std::shared_ptr<Node> skiplist);
  SkiplistIterator();
  ~SkiplistIterator() = default;
  BaseIterator& operator++() override;
  auto          operator<=>(const BaseIterator& other) const;

  valuetype                           operator*() const override;
  SkiplistIterator                    operator+=(int offset) const;
  bool                                valid() const override;
  bool                                isEnd() override;
  IteratorType                        type() const override;
  uint64_t                            get_tranc_id() const override;
  std::pair<std::string, std::string> getValue() const;

 private:
  std::shared_ptr<Node> current;
};

inline int cmp(const std::string& a, const std::string& b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}
class Skiplist {
 public:
  explicit Skiplist(int max_level_ = MAX_LEVEL)
      : max_level(max_level_), current_level(1), size_bytes(0), nodecount(0), dis(0.0, 1.0) {
    head = std::make_shared<Node>("", "", max_level_);
  }  // 默认最大16层

  bool Insert(const std::string& key, const std::string& value, const uint64_t transaction_id = 0);

  bool Delete(const std::string& key);

  std::optional<std::string> Contain(const std::string& key, const uint64_t transaction_id = 0);
  std::shared_ptr<Node>      Get(const std::string& key, const uint64_t transaction_id = 0);
  std::vector<std::pair<std::string, std::string>> flush();
  std::size_t                                      get_size();
  std::size_t                                      getnodecount();
  int                                              get_range_index(const std::string& key);
  auto                                             seekToFirst();
  auto                                             seekToLast();
  SkiplistIterator                                 end();
  SkiplistIterator                                 begin();
  SkiplistIterator                                 prefix_serach_begin(const std::string& key);
  SkiplistIterator                                 prefix_serach_end(const std::string& key);

  void                    set_status(Global_::SkiplistStatus status);
  Global_::SkiplistStatus get_status() const;

 private:
  static constexpr int             MAX_RANGES = 256;  // 最大范围
  std::shared_ptr<Node>            head;
  int                              max_level;      // 最大层级
  int                              current_level;  // 当前层级
  std::atomic_size_t               size_bytes;     // 内存占用，达到。flush到disk
  std::atomic_int                  nodecount = 0;  // 节点数量
  std::random_device               rd;             // 随机数生成器
  static thread_local std::mt19937 gen;            // 随机数引擎
  std::uniform_real_distribution<> dis;            // 随机数分布

  Global_::SkiplistStatus cur_status = Global_::SkiplistStatus::kNormal;
  int                     random_level();
};
