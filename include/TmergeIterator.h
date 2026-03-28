#pragma once
#include "SstableIterator.h"

class TwoMergeIterator;
bool operator==(const TwoMergeIterator& a, const TwoMergeIterator& b);

class TwoMergeIterator : public BaseIterator {
  friend bool operator==(const TwoMergeIterator& a, const TwoMergeIterator& b);

 private:
  std::shared_ptr<BaseIterator>      it_a;
  std::shared_ptr<BaseIterator>      it_b;
  bool                               choose_a = false;
  mutable std::shared_ptr<valuetype> current;  // 用于存储当前元素
  uint64_t                           max_tranc_id_      = 0;
  bool                               keep_all_versions_ = false;

  void update_current() const;

 public:
  TwoMergeIterator();
  TwoMergeIterator(std::shared_ptr<BaseIterator> it_a, std::shared_ptr<BaseIterator> it_b,
                   uint64_t max_tranc_id, bool keep_all_versions = false);
  bool choose_it_a();
  // 跳过当前不可见事务的id (如果开启了事务功能)
  void                  skip_by_tranc_id();
  void                  skip_it_b();
  auto                  operator<=>(const TwoMergeIterator& other) const;
  virtual BaseIterator& operator++() override;
  virtual valuetype     operator*() const override;
  virtual IteratorType  type() const override;
  virtual uint64_t      get_tranc_id() const override;
  virtual bool          isEnd() const override;
  virtual bool          valid() const override;
};
