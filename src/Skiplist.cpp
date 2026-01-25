#include "../include/Skiplist.h"
#include <cstddef>

auto Node::operator<=>(const Node& other) const {
  return key_ <=> other.key_;
}

SkiplistIterator::SkiplistIterator(std::shared_ptr<Node> skiplist) {
  current = skiplist;
}
SkiplistIterator::SkiplistIterator() : current(nullptr) {}
BaseIterator& SkiplistIterator::operator++() {
  if (current) {
    current = current->forward[0];
  }
  return *this;
}
auto SkiplistIterator::operator<=>(const BaseIterator& other) const {
  if (other.type() != IteratorType::SkiplistIterator) {
    return std::strong_ordering::less;
  }
  const SkiplistIterator& other_skiplist = static_cast<const SkiplistIterator&>(other);
  return current <=> other_skiplist.current;
}
bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept {
  return lhs.current == rhs.current;
}

SkiplistIterator::valuetype SkiplistIterator::operator*() const {
  return {current->key_, current->value_};
}
SkiplistIterator SkiplistIterator::operator+=(int offset) const {
  SkiplistIterator result = *this;
  for (int i = 0; i < offset && result.valid(); ++i) {
    ++result;
  }
  return result;
}
bool SkiplistIterator::valid() const {
  return current != nullptr;
}
bool SkiplistIterator::isEnd() {
  return current == nullptr;
}
IteratorType SkiplistIterator::type() const {
  return IteratorType::SkiplistIterator;
}
uint64_t SkiplistIterator::get_tranc_id() const {
  if (current) {
    return current->transaction_id;
  }
  return 0;
}
std::pair<std::string, std::string> SkiplistIterator::getValue() const {
  if (current) {
    return {current->key_, current->value_};
  }
  return {std::string(), std::string()};
}

bool Skiplist::Insert(const std::string& key, const std::string& value,
                      const uint64_t transaction_id) {
  std::vector<std::shared_ptr<Node>> update(MAX_LEVEL, nullptr);
  auto                               current = head;

  // 查找插入位置
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
    update[i] = current;  // 记录需要更新的节点
  }
  // 拿到新的节点的高度
  int Newlevel = random_level();
  // 创建新节点
  std::shared_ptr<Node> NewNode = std::make_shared<Node>(key, value, Newlevel, transaction_id);
  // 插入新节点
  for (int i = 0; i < Newlevel; i++) {
    if (update[i]) {  // 确保 update[i] 不为空
      NewNode->forward[i]   = update[i]->forward[i];
      update[i]->forward[i] = NewNode;
    }
  }
  // 更新当前层级
  current_level = std::max(current_level, Newlevel);

  // 更新内存占用
  size_bytes += (key.size() + value.size() + sizeof(uint64_t)) * Newlevel;
  nodecount++;
  return true;
}

bool Skiplist::Delete(const std::string& key) {
  auto                               current = head;
  std::vector<std::shared_ptr<Node>> update(MAX_LEVEL, nullptr);

  // 查找删除位置
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
    update[i] = current;  // 记录需要更新的节点
  }
  // 删除节点
  auto target = current->forward[0];
  if (target && target->key_ == key) {
    for (int i = 0; i < current_level; ++i) {
      if (update[i]->forward[i] != target) {
        size_bytes -= (target->key_.size() + target->value_.size() + target->transaction_id) * i;
        break;
      }
      update[i]->forward[i] = target->forward[i];
    }
    nodecount--;
  }

  // 更新当前层级
  // 如果当前层级的节点为空，则需要更新当前层级
  for (int i = current_level - 1; i >= 0; --i) {
    if (head->forward[i] == nullptr) {
      current_level--;
    }
  }
  return true;
}

std::optional<std::string> Skiplist::Contain(const std::string& key,
                                             const uint64_t     transaction_id) {
  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
    if (transaction_id == 0) {
      return current->forward[0]->value_;
    } else {
      while (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
             current->forward[0]->transaction_id > transaction_id) {
        current = current->forward[0];
      }
      if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
          (!current->forward[0]->value_.empty())) {
        return current->forward[0]->value_;
      } else if (cmp(current->key_, key) == 0 && current->transaction_id <= transaction_id &&
                 (!current->value_.empty())) {
        return current->value_;
      }
      return std::nullopt;
    }
  }
  return std::nullopt;  // 如果没有找到，返回空值
}

std::shared_ptr<Node> Skiplist::Get(const std::string& key, const uint64_t transaction_id) {
  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
    if (transaction_id == 0) {
      return current->forward[0];
    } else {
      if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
          (!current->forward[0]->value_.empty())) {
        return current->forward[0];
      } else if (cmp(current->key_, key) == 0 && current->transaction_id <= transaction_id &&
                 (!current->value_.empty())) {
        return current;
      }
      return nullptr;
    }
  }
  return nullptr;  // 如果没有找到，返回空值
}

std::vector<std::pair<std::string, std::string>> Skiplist::flush() {
  std::vector<std::pair<std::string, std::string>> result;
  auto                                             current = head->forward[0];
  while (current) {
    result.emplace_back(current->key_, current->value_);
    current = current->forward[0];
  }
  return result;
}
std::size_t Skiplist::get_size() {
  return size_bytes;
}

std::size_t Skiplist::getnodecount() {
  return nodecount;
}

auto Skiplist::seekToFirst() {
  return head->forward[0];
}  // 定位到第一个元素
auto Skiplist::seekToLast() {
  auto current = head;
  current      = current->forward[0];
  while (current->forward[0]) {
    current = current->forward[0];
  }
  return current;
}
SkiplistIterator Skiplist::end() {
  return SkiplistIterator(nullptr);
}
SkiplistIterator Skiplist::begin() {
  return SkiplistIterator(head->forward[0]);
}

SkiplistIterator Skiplist::prefix_serach_begin(const std::string& key) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->key_.starts_with(key)) {
    return SkiplistIterator(current);
  }
  if (current->forward[0] && current->forward[0]->key_.starts_with(key)) {
    return SkiplistIterator(current->forward[0]);
  }
  return SkiplistIterator(nullptr);
}
SkiplistIterator Skiplist::prefix_serach_end(const std::string& key) {
  auto Newkey  = key + '\xff';
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && cmp(current->forward[i]->key_, Newkey) == -1) {
      current = current->forward[i];
    }
  }
  return SkiplistIterator(current->forward[0]);
}
void Skiplist::set_status(Global_::SkiplistStatus status) {
  cur_status = status;
}

Global_::SkiplistStatus Skiplist::get_status() const {
  return cur_status;
}

thread_local std::mt19937 Skiplist::gen(std::random_device{}());

int Skiplist::random_level() {
  static constexpr double P     = 0.25;  // 每一层的概率
  int                     level = 1;
  while (dis(gen) < P && level < max_level) {
    ++level;
  }
  return level;
}