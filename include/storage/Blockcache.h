#pragma once
#include "Block.h"
#include "../core/Global.h"
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stdatomic.h>
#include <sys/types.h>
#include <unordered_map>
#include <utility>
#include <vector>

// 定义缓存项
struct CacheItem {
  int                    sst_id;
  int                    block_id;
  std::shared_ptr<Block> cache_block;
  uint64_t               access_count;  // 访问时间戳
};

// 自定义哈希函数
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2>& p) const {
    auto hash1 = std::hash<T1>{}(p.first);
    auto hash2 = std::hash<T2>{}(p.second);
    return hash1 ^ hash2;
  }
};

// 自定义相等比较函数
struct pair_equal {
  template <class T1, class T2>
  bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }
};

// 定义缓存池
class BlockCache {
 public:
  BlockCache(size_t capacity = Global_::Block_CACHE_capacity, size_t k = Global_::Block_CACHE_K);
  ~BlockCache();

  // 获取缓存项
  std::shared_ptr<Block> get(int sst_id, int block_id);

  // 插入缓存项
  void put(int sst_id, int block_id, std::shared_ptr<Block> data);

  // 获取缓存命中率
  double hit_rate() const;

 private:
  size_t             capacity_;  // 缓存容量
  size_t             k_;         // LRU-K 中的 K 值
  mutable std::mutex mutex_;     // 互斥锁保护缓存池

  // 双向链表存储缓存项
  std::list<CacheItem> cache_list_greater_k;
  std::list<CacheItem> cache_list_less_k;

  // 哈希表索引缓存项
  std::unordered_map<std::pair<int, int>, std::list<CacheItem>::iterator, pair_hash, pair_equal>
      cache_map_;

  // 更新缓存项的访问时间戳
  void update_access_count(std::list<CacheItem>::iterator it);

  // 记录请求数和命中数
  std::atomic<std::size_t> total_requests = 0;
  std::atomic<std::size_t> hits_requests  = 0;
};
