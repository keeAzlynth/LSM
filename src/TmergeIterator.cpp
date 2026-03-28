#include "../include/TmergeIterator.h"

bool operator==(const TwoMergeIterator& lhs, const TwoMergeIterator& rhs) {
  if (lhs.isEnd() || rhs.isEnd()) {
    return false;
  }
  if (lhs.isEnd() && rhs.isEnd()) {
    return true;
  }
  return lhs.it_a == rhs.it_a && lhs.it_b == rhs.it_b && lhs.choose_a == rhs.choose_a;
}
TwoMergeIterator::TwoMergeIterator() {}

TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> it_a,
                                   std::shared_ptr<BaseIterator> it_b, uint64_t max_tranc_id,
                                   bool keep_all_versions)
    : it_a(std::move(it_a)),
      it_b(std::move(it_b)),
      max_tranc_id_(max_tranc_id),
      keep_all_versions_(keep_all_versions) {
  // 先跳过不可见的事务
  skip_by_tranc_id();
  skip_it_b();               // 跳过与 it_a 重复的 key
  choose_a = choose_it_a();  // 决定使用哪个迭代器
}

bool TwoMergeIterator::choose_it_a() {
  if (it_a->isEnd()) {
    return false;
  }
  if (it_b->isEnd()) {
    return true;
  }
  auto key_a = (**it_a).first;
  auto key_b = (**it_b).first;
  if (key_a != key_b) {
    return key_a < key_b;  // 比较 key
  }
  if (keep_all_versions_) {
    return it_a->get_tranc_id() > it_b->get_tranc_id();
  }
  return true;
}

void TwoMergeIterator::skip_it_b() {
  if (keep_all_versions_) {
    return;
  }
  if (!it_a->isEnd() && !it_b->isEnd() && (**it_a).first == (**it_b).first) {
    ++(*it_b);
  }
}

void TwoMergeIterator::skip_by_tranc_id() {
  if (max_tranc_id_ == 0) {
    return;
  }
  while (it_a->get_tranc_id() > max_tranc_id_) {
    ++(*it_a);
  }
  while (it_b->get_tranc_id() > max_tranc_id_) {
    ++(*it_b);
  }
}

BaseIterator& TwoMergeIterator::operator++() {
  if (choose_a) {
    ++(*it_a);
  } else {
    ++(*it_b);
  }
  // 先跳过不可见的事务
  skip_by_tranc_id();
  skip_it_b();               // 跳过重复的 key
  choose_a = choose_it_a();  // 重新决定使用哪个迭代器
  return *this;
}

BaseIterator::valuetype TwoMergeIterator::operator*() const {
  if (choose_a) {
    return **it_a;
  } else {
    return **it_b;
  }
}

IteratorType TwoMergeIterator::type() const {
  return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_tranc_id() const {
  if (keep_all_versions_) {
    if (choose_a && it_a && !it_a->isEnd()) {
      return it_a->get_tranc_id();
    }
    if (!choose_a && it_b && !it_b->isEnd()) {
      return it_b->get_tranc_id();
    }
  }
  return max_tranc_id_;
}

bool TwoMergeIterator::isEnd() const {
  if (it_a == nullptr && it_b == nullptr) {
    return true;
  }
  if (it_a == nullptr) {
    return it_b->isEnd();
  }
  if (it_b == nullptr) {
    return it_a->isEnd();
  }
  return it_a->isEnd() && it_b->isEnd();
}

bool TwoMergeIterator::valid() const {
  if (it_a == nullptr && it_b == nullptr) {
    return false;
  }
  if (it_a == nullptr) {
    return it_b->valid();
  }
  if (it_b == nullptr) {
    return it_a->valid();
  }
  return it_a->valid() || it_b->valid();
}

void TwoMergeIterator::update_current() const {
  if (choose_a) {
    current = std::make_shared<valuetype>(**it_a);
  } else {
    current = std::make_shared<valuetype>(**it_b);
  }
}
