#include "../include/Skiplist.h"


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
uint64_t SkiplistIterator::getseq() const {
  if (current) {
    return current->transaction_id;
  }
  return 0;
}
std::pair<std::string, std::string> SkiplistIterator::getValue() const {
  if (current) {
    return {current->key_, current->value_};
  }
  return {"", ""};
}

bool Skiplist::Insert(const std::string& key, const std::string& value, const uint64_t transaction_id) {
  auto result = Contain(key);
  if (result.has_value() && result.value() == value) {
    // 如果已经存在，则不插入
    return false;
  }
  std::vector<std::shared_ptr<Node>> update(MAX_LEVEL, nullptr);
  auto                               current = head;

  // 查找插入位置
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
    if (current->forward[i] && current->forward[i]->key_ == key &&
        current->forward[i]->transaction_id <= transaction_id) {
      // 如果当前节点的key等于要插入的key，同时transation_id小于等于当前id,则更新
        size_bytes += (value.size() - current->forward[i]->value_.size())*i;
      while (i>=0) {
      // 更新节点的值
      current->forward[i]->value_         = value;
      current->forward[i]->transaction_id = transaction_id;
      --i;
      }
      return true;
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
  size_bytes += key.size() + value.size();
  nodecount++;
  return true;
}

bool Skiplist::Delete(const std::string& key) {
  auto result = Contain(key);
  if (!result.has_value()) {
    return false;  // 如果不存在，则不执行删除
  }
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
        break;
      }
      update[i]->forward[i] = target->forward[i];
    }
    // 更新内存占用
    size_bytes -= (target->key_.size() + target->value_.size());
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

std::optional<std::string> Skiplist::Contain(const std::string& key, const uint64_t transaction_id) {
  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
    if (current->forward[i] && current->forward[i]->key_ == key) {
      return current->forward[i]->value_;
    }
  }
  return std::nullopt;  // 如果没有找到，返回空值
}

std::shared_ptr<Node> Skiplist::Get(const std::string& key, const uint64_t transaction_id) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
    if (current->forward[i] && current->forward[i]->key_ == key) {
      if (transaction_id == 0) {
        return current->forward[i];
      } else if (current->forward[i]->transaction_id <= transaction_id) {
        return current->forward[i];
      }
    }
  }
  return nullptr;
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

SkiplistIterator Skiplist::prefix_serach_begin(const std::string& key, const uint64_t transaction_id) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && current->forward[0]->transaction_id <= transaction_id) {
    return SkiplistIterator(current->forward[0]);
  }
  return SkiplistIterator(nullptr);
}
SkiplistIterator Skiplist::prefix_serach_end(const std::string& key, const uint64_t transaction_id) {
  auto Newkey = key + '\xff';
  auto result = prefix_serach_begin(key);
  if (result.current == nullptr) {
    return SkiplistIterator(nullptr);
  }
  auto                     current = result.current;
  std::vector<std::string> skipmap;
  // 从最高层开始查找

  while (current && current->key_ < Newkey) {
    skipmap.push_back(current->key_);
    current = current->forward[0];
  }
  if (current) {
    skipmap.push_back(current->key_);
  }
  return SkiplistIterator(Get(skipmap.back(), transaction_id));
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

// 定位到最后一个元素
// 这里的实现是一个简单的跳表迭代器，提供了基本的迭代功能
// 你可以根据需要扩展更多功能，比如范围查询、反向迭代等
// 当然，实际的实现可能会更复杂，取决于你的需求
// 这里的实现是一个简单的跳表迭代器，提供了基本的迭代功能
// 你可以根据需要扩展更多功能，比如范围查询、反向迭代等
// 当然，实际的实现可能会更复杂，取决于你的需求
