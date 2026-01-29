#pragma once
#include "Skiplist.h"
#include "Sstable.h"
#include <list>
#include <memory>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>

class MemTableIterator;
class Sstbuild;
bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept;

class MemTableIterator : public BaseIterator {
 public:
  friend bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept;
  using valuetype = std::pair<std::string, std::string>;
  MemTableIterator(std::vector<SerachIterator> iter, const uint64_t max_transaction_id);
  MemTableIterator(const SkiplistIterator& iter, const uint64_t max_transaction_id);
  ~MemTableIterator() = default;

  bool valid() const override;

  auto              operator<=>(const BaseIterator& other) const;
  valuetype         operator*() const override;
  pvaluetype        operator->() const;
  MemTableIterator& operator++() override;

  bool         isEnd() override;
  uint64_t     get_tranc_id() const override;
  IteratorType type() const override;
  valuetype    getValue() const;
  void         pop_value();
  void         update_current_key_value() const;

 private:
  bool                               top_value_legal() const;
  void                               skip_transaction_id();
  mutable std::shared_ptr<valuetype> current_value_;
  std::shared_ptr<SkiplistIterator>  list_iter_;
  std::priority_queue<SerachIterator, std::vector<SerachIterator>, std::greater<SerachIterator>>
           queue_;
  uint64_t max_transaction_id;
};

class MemTable {
  friend class MemTableIterator;  // 让 MemTableIterator 可以访问私有成员

 public:
  MemTable();
  MemTable(const MemTable& other)            = delete;
  MemTable& operator=(const MemTable& other) = delete;
  ~MemTable();

  void put(const std::string& key, const std::string& value, const uint64_t transaction_id = 0);
  void put_mutex(const std::string& key, const std::string& value,
                 const uint64_t transaction_id = 0);
  void put_batch(const std::vector<std::pair<std::string, std::string>>& key_value_pairs,
                 const uint64_t                                          transaction_id = 0);
  std::optional<std::pair<std::string, uint64_t>> get(const std::string& key,
                                                      const uint64_t     transaction_id = 0);

  SkiplistIterator cur_get(const std::string& key, const uint64_t transaction_id = 0);
  SkiplistIterator fix_get(const std::string& key, const uint64_t transaction_id = 0);
  SkiplistIterator get_mutex(const std::string& key, std::vector<std::string>& values);
  std::vector<std::tuple<std::string, std::optional<std::string>, std::optional<uint64_t>>>
  get_batch(const std::vector<std::string>& key_s, const uint64_t transaction_id = 0);

  size_t get_cur_size();
  size_t get_fixed_size();
  size_t get_total_size();
  void   remove(const std::string& key, const uint64_t transaction_id = 0);
  void   remove_mutex(const std::string& key, const uint64_t transaction_id = 0);
  void   remove_batch(const std::vector<std::string>& key_pairs, const uint64_t transaction_id = 0);
  bool   IsFull();
  std::shared_ptr<Skiplist>            flush();
  std::list<std::shared_ptr<Skiplist>> flushsync();
  void                                 frozen_cur_table();
  MemTableIterator                     begin();
  MemTableIterator                     end();
  MemTableIterator prefix_serach(const std::string& key, const uint64_t transaction_id = 0);
  enum class SkiplistStatus {
    kNormal,
    KFreezing,
    kFrozen,
  };

 private:
  std::shared_ptr<Skiplist>            current_table;  // 活跃 SkipList
  std::list<std::shared_ptr<Skiplist>> fixed_tables;   // 不可写的 SkipList==InmutTable
  size_t                               fixed_bytes;    // fixed_tables的跳表的大小
  std::shared_mutex                    fix_lock_;      // 保护当前跳表的锁
  std::shared_mutex                    cur_lock_;
  std::atomic<SkiplistStatus>          cur_status;  // 当前跳表的状态
  // 保护当前跳表的读写锁
};