#include "../include/memtable.h"
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept {
  if (lhs.queue_.empty() || rhs.queue_.empty()) {
    return lhs.queue_.empty() && rhs.queue_.empty();
  }
  return lhs.queue_.top().key_ == rhs.queue_.top().key_ &&
         lhs.queue_.top().value_ == rhs.queue_.top().value_ &&
         lhs.queue_.top().transaction_id_ == rhs.queue_.top().transaction_id_;
}
MemTableIterator::MemTableIterator(std::vector<SerachIterator> iter, const uint64_t transaction_id)
    : max_transaction_id(0) {
  for (auto& it : iter) {
    queue_.push(it);
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
}
MemTableIterator::MemTableIterator(const SkiplistIterator& iter, const uint64_t transaction_id)
    : max_transaction_id(transaction_id) {
  list_iter_ = std::make_shared<SkiplistIterator>(iter);
}
MemTable::~MemTable() = default;

auto MemTableIterator::operator<=>(const BaseIterator& other) const {
  if (other.type() != IteratorType::MemTableIterator) {
    return std::strong_ordering::less;
  }
  const MemTableIterator& other_iter = static_cast<const MemTableIterator&>(other);
  if (queue_.empty() || other_iter.queue_.empty()) {
    return queue_.empty() ? (other_iter.queue_.empty() ? std::strong_ordering::equal
                                                       : std::strong_ordering::less)
                          : std::strong_ordering::greater;
  }
  return queue_.top() <=> other_iter.queue_.top();
}

MemTableIterator::valuetype MemTableIterator::operator*() const {
  return {current_value_->first, current_value_->second};
}
BaseIterator::pvaluetype MemTableIterator::operator->() const {
  update_current_key_value();
  return current_value_.get();
}
MemTableIterator& MemTableIterator::operator++() {
  if (queue_.empty()) {
    return *this;
  }
  auto temp = queue_.top().key_;
  while (!queue_.empty() && queue_.top().key_ == temp) {
    queue_.pop();
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
  return *this;
}
bool MemTableIterator::isEnd() {
  return queue_.empty();
}
uint64_t MemTableIterator::getseq() const {
  if (queue_.empty()) {
    return 0;
  }
  return queue_.top().transaction_id_;
}
IteratorType MemTableIterator::type() const {
  return IteratorType::MemTableIterator;
}
MemTableIterator::valuetype MemTableIterator::getValue() const {
  if (queue_.empty()) {
    return std::make_pair("", "");
  }
  return std::make_pair(queue_.top().key_, queue_.top().value_);
}
void MemTableIterator::pop_value() {
  if (queue_.empty()) {
    return;
  }
  auto temp = queue_.top().key_;
  while (!queue_.empty() && queue_.top().key_ == temp) {
    queue_.pop();
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
}
void MemTableIterator::update_current_key_value() const {
  if (!queue_.empty()) {
    current_value_ = std::make_shared<valuetype>(queue_.top().key_, queue_.top().value_);
  } else {
    current_value_.reset();
  }
}
void MemTableIterator::skip_transaction_id() {
  if (max_transaction_id == 0) {
    return;
  }
  while (!queue_.empty() && queue_.top().transaction_id_ <= max_transaction_id) {
    queue_.pop();
  }
}
bool MemTableIterator::top_value_legal() const {
  if (queue_.empty()) {
    return true;
  }
  if (max_transaction_id == 0) {
    return !queue_.top().value_.empty();
  }
  if (queue_.top().transaction_id_ < max_transaction_id) {
    return !queue_.top().value_.empty();
  } else {
    return false;
  }
}

MemTable::MemTable() {
  current_table = std::make_shared<Skiplist>(MAX_LEVEL);
  fixed_bytes   = 0;
  cur_status    = SkiplistStatus::kNormal;
}

bool MemTableIterator::valid() const {
  return !queue_.empty();
}
void MemTable::put(const std::string& key, const std::string& value,
                   const uint64_t transaction_id) {
  current_table->Insert(key, value, transaction_id);
}

void MemTable::put_mutex(const std::string& key, const std::string& value,
                         const uint64_t transaction_id) {
  {
    std::unique_lock<std::shared_mutex> lock(cur_lock_);
    current_table->Insert(key, value, transaction_id);
  }
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    std::unique_lock<std::shared_mutex> lock(fix_lock_);
    frozen_cur_table();
  }
}
void MemTable::put_batch(const std::vector<std::pair<std::string, std::string>>& key_value_pairs,
                         const uint64_t                                          transaction_id) {
  for (const auto& pair : key_value_pairs) {
    current_table->Insert(pair.first, pair.second, transaction_id);
  }
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    std::unique_lock<std::shared_mutex> lock(fix_lock_);
    frozen_cur_table();
  }
}
std::optional<std::pair<std::string, uint64_t>> MemTable::get(const std::string& key,
                                                              const uint64_t     transaction_id) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  auto                                result = current_table->Get(key, transaction_id);
  if (result) {
    // 检查是否为删除标记s
    if (result->value_.empty()) {
      return std::nullopt;  // 如果是空值，表示该key已被删除
    }
    return std::make_pair(result->value_, result->transaction_id);
  }

  lock.unlock();
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  for (const auto& fixed_table : fixed_tables) {
    auto result = fixed_table->Get(key, transaction_id);
    if (result) {
      // 检查是否为删除标记
      if (result->value_.empty()) {
        return std::nullopt;  // 如果是空值，表示该key已被删除
      }
      return std::make_pair(result->value_, result->transaction_id);
    }
  }
  return std::nullopt;
}

SkiplistIterator MemTable::cur_get(const std::string& key, const uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  return SkiplistIterator(current_table->Get(key, transaction_id));
}
SkiplistIterator MemTable::fix_get(const std::string& key, const uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(fix_lock_);
  for (const auto& result : fixed_tables) {
    if (result->Contain(key, transaction_id).has_value()) {
      return SkiplistIterator(result->Get(key, transaction_id));
    }
  }
  return SkiplistIterator();
}
SkiplistIterator MemTable::get_mutex(const std::string& key, std::vector<std::string>& values) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  return SkiplistIterator(current_table->Get(key));
  lock.unlock();
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  for (const auto& result : fixed_tables) {
    if (result->Contain(key).has_value()) {
      return SkiplistIterator(result->Get(key));
    }
  }
  return SkiplistIterator();
}

std::vector<std::tuple<std::string, std::optional<std::string>, std::optional<uint64_t>>>
MemTable::get_batch(const std::vector<std::string>& key_pairs, const uint64_t transaction_id) {
  std::vector<std::tuple<std::string, std::optional<std::string>, std::optional<uint64_t>>> result;
  for (const auto& pair : key_pairs) {
    auto value = get(pair, transaction_id);
    if (value.has_value() && !value.value().first.empty()) {  // 添加空值检查
      result.emplace_back(pair, value->first, value->second);
    } else {
      result.emplace_back(pair, std::nullopt, std::nullopt);
    }
  }
  return result;
}
std::size_t MemTable::get_fixed_size() {
  std::shared_lock<std::shared_mutex> lock(fix_lock_);
  return fixed_bytes;
}
std::size_t MemTable::get_cur_size() {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  return current_table->get_size();
}
std::size_t MemTable::get_total_size() {
  return get_cur_size() + get_fixed_size();
}

void MemTable::remove(const std::string& key, const uint64_t transaction_id) {
  current_table->Insert(key, "", transaction_id);
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    frozen_cur_table();
  }
}
void MemTable::remove_mutex(const std::string& key, const uint64_t transaction_id) {
  {
    std::unique_lock<std::shared_mutex> lock(cur_lock_);
    current_table->Insert(key, "", transaction_id);
  }
  if (fixed_tables.size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    std::unique_lock<std::shared_mutex> lock(fix_lock_);
    frozen_cur_table();
  }
}
// 批量删除
void MemTable::remove_batch(const std::vector<std::string>& key_pairs,
                            const uint64_t                  transaction_id) {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  for (const auto& pair : key_pairs) {
    current_table->Insert(pair, "", transaction_id);
  }
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    std::unique_lock<std::shared_mutex> lock(fix_lock_);
    frozen_cur_table();
  }
}
bool MemTable::IsFull() {
  return current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
}

// This function is used to flush the current memtable to disk,刷新到磁盘
std::shared_ptr<Skiplist> MemTable::flush() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  auto                                new_table = std::make_shared<Skiplist>(MAX_LEVEL);
  fixed_tables.push_back(std::move(current_table));
  current_table = std::move(new_table);
  fixed_bytes += current_table->get_size();
  if (fixed_tables.size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    auto frozen_memtable = fixed_tables.front();
    fixed_tables.pop_front();
    return frozen_memtable;
  }
  return std::make_shared<Skiplist>();
}
std::shared_ptr<Skiplist> MemTable::flushsync() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  auto                                new_table = std::make_shared<Skiplist>(MAX_LEVEL);
  fixed_tables.push_back(std::move(current_table));
  current_table = std::move(new_table);
  fixed_bytes += current_table->get_size();
  auto it = fixed_tables.front();
  fixed_tables.clear();
  return it;
}
void MemTable::frozen_cur_table() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  auto                                new_table = std::make_shared<Skiplist>(MAX_LEVEL);
  auto                                temp_size = current_table->get_size();
  std::unique_lock<std::shared_mutex> lock2(fix_lock_);
  fixed_tables.push_front(current_table);
  current_table = new_table;
  fixed_bytes += temp_size;
}

MemTableIterator MemTable::begin() {
  return MemTableIterator(fixed_tables.begin()->get()->begin(), 0);
}
MemTableIterator MemTable::end() {
  return MemTableIterator(fixed_tables.end()->get()->end(), 0);
}
// 迭代器

MemTableIterator MemTable::prefix_serach(const std::string& key, const uint64_t transaction_id) {
  std::vector<SerachIterator>         iter;
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  if (!current_table) {
    throw std::runtime_error("current_table is null");
  }
  for (auto begin = current_table->prefix_serach_begin(key);
       begin != current_table->prefix_serach_end(key); ++begin) {
    iter.push_back(
        SerachIterator(begin.getValue().first, begin.getValue().second, transaction_id, 0, 0));
  }
  lock.unlock();
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  if (fixed_tables.empty()) {
    return MemTableIterator(iter, transaction_id);
  }
  for (const auto& fixed_table : fixed_tables) {
    for (auto begin = fixed_table->prefix_serach_begin(key);
         begin != fixed_table->prefix_serach_end(key); ++begin) {
      iter.push_back(
          SerachIterator(begin.getValue().first, begin.getValue().second, transaction_id, 0, 0));
    }
  }
  return MemTableIterator(iter, transaction_id);
}
