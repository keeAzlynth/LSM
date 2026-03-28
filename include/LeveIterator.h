#pragma once
#include "BaseIterator.h"
#include <memory>
#include <optional>
#include <vector>
#include <shared_mutex>

class LSM_Engine;

class Level_Iterator : public BaseIterator {
 public:
  Level_Iterator() = default;
  Level_Iterator(std::shared_ptr<LSM_Engine> engine_, uint64_t max_tranc_id);

  virtual BaseIterator& operator++() override;
  virtual valuetype     operator*() const override;
  virtual IteratorType  type() const override;
  virtual uint64_t      get_tranc_id() const override;
  virtual bool          isEnd() const override;
  virtual bool          valid() const override;

 private:
  std::shared_ptr<LSM_Engine>                engine_;
  std::vector<std::shared_ptr<BaseIterator>> iter_vec;
  size_t                                     cur_idx_;
  uint64_t                                   max_tranc_id_;
  mutable std::optional<valuetype>           cached_value;  // 缓存当前值
  std::shared_lock<std::shared_mutex>        rlock_;

 private:
  void                           update_current() const;
  std::pair<size_t, std::string> get_min_key_idx() const;
  void                           skip_key(const std::string& key);
};
