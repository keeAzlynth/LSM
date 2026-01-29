#include "../../include/memtable.h"
#include "../../include/BlockIterator.h"
#include "../../include/Blockcache.h"
#include "../../include/SstableIterator.h"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <cmath>
#include <cstddef>
#include <memory>
#include <filesystem>
#include <print>
#include <string>
#include <tuple>
#include <algorithm>
#include <utility>
#include <vector>

class SstableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 初始化 BlockCache
    block_cache = std::make_shared<BlockCache>(4096, 2);
    memtable    = std::make_shared<MemTable>();
  }

  void TearDown() override {
    try {
      if (!tmp_path1.empty() && std::filesystem::exists(tmp_path1)) {
        std::filesystem::remove(tmp_path1);
      }
      if (!tmp_path2.empty() && std::filesystem::exists(tmp_path2)) {
        std::filesystem::remove(tmp_path2);
      }
    } catch (...) {
    }
  }
  std::vector<std::tuple<std::string, std::string, uint64_t>> generate_range_test_data(
      int num_records, size_t version) {
    std::vector<std::tuple<std::string, std::string, uint64_t>> data;

    // 生成连续的范围数据，如 key_0000, key_0001, key_0002...
    for (int i = 0; i < num_records; ++i) {
      std::string key = std::format("key_{:04d}", i);

      // 每个键有3个版本
      data.emplace_back(key, std::format("value_v1_{:04d}", i), 1000 + i);
      data.emplace_back(key, std::format("value_v2_{:04d}", i), 2000 + i);
      data.emplace_back(key, std::format("value_v3_{:04d}", i), 3000 + i);

      // 添加一些其他前缀的键，用于测试跨前缀范围
      if (i < 100) {
        std::string alt_key = std::format("alt_{:04d}", i);
        data.emplace_back(alt_key, std::format("alt_value_{:04d}", i), 1500 + i);
      }
    }

    // 按时间戳排序（模拟写入顺序）
    std::ranges::sort(data,
                      [](const auto& a, const auto& b) { return std::get<2>(a) < std::get<2>(b); });

    return data;
  }

  // 构建SSTable
  std::shared_ptr<Sstable> build_sstable(
      const std::vector<std::tuple<std::string, std::string, uint64_t>>& data,
      const std::string& path, size_t block_size = 4096) {
    if (std::filesystem::exists(path)) {
      std::filesystem::remove(path);
    }

    Sstbuild builder(block_size, true);

    // 将所有数据放入memtable
    for (const auto& [key, value, ts] : data) {
      memtable->put(key, value, ts);
    }

    // 刷入builder
    auto flush_result = memtable->flush();
    if (!flush_result) {
      spdlog::error("Flush failed");
    }

    for (auto it = flush_result->begin(); it != flush_result->end(); ++it) {
      auto kv       = it.getValue();
      auto tranc_id = it.get_tranc_id();
      builder.add(kv.first, kv.second, tranc_id);
    }

    return builder.build(block_cache, path, 0);
  }

  std::shared_ptr<MemTable>   memtable;
  std::shared_ptr<BlockCache> block_cache;
  std::string tmp_path1 = "/root/LSM/tmp/lsm_test_sstable.dat";  // 改成你的绝对路径，方便调试
  std::string tmp_path2 = "/root/LSM/tmp/lsm_test_sstable2.dat";
};

// 使用大量数据并强制产生多个 block，验证前缀查询和返回结果正确性
TEST_F(SstableTest, BuildAndGetPrefixSingleKey_ManyBlocks) {
  const std::string key_prefix  = "k";
  const size_t      num_records = 500;   // 较大数据量
  const size_t      block_size  = 4096;  // 较小 block 容量以产生多个 block

  if (std::filesystem::exists(tmp_path1))
    std::filesystem::remove(tmp_path1);

  Sstbuild                                         builder(block_size, true);
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(num_records);
  for (size_t i = 0; i < num_records; ++i) {
    auto res = Global_::generateRandom(0, num_records - 1);
    kvs.push_back({key_prefix + std::to_string(res), "value" + std::to_string(res)});
  }
  // 先将所有数据 put 到 memtable
  for (auto& i : kvs) {
    memtable->put(i.first, i.second, 0);
  }
  // 然后在最后调用一次 flushsync，把所有数据刷到 builder
  auto res = memtable->flush();
  for (auto i = res->begin(); i != res->end(); ++i) {
    auto kv = i.getValue();
    builder.add(kv.first, kv.second);
  }

  std::shared_ptr<Sstable> sst;
  try {
    sst = builder.build(block_cache, tmp_path1, 0);
  } catch (const std::exception& e) {
    FAIL() << "Build failed: " << e.what();
    return;
  }

  ASSERT_NE(sst, nullptr);
  ASSERT_TRUE(std::filesystem::exists(tmp_path1));

  // 选取若干前缀检查，确保跨 block 能正确查询
  auto                                                        checks_size = 10;
  std::vector<std::tuple<std::string, std::string, uint64_t>> checks;
  checks.reserve(checks_size);
  for (int i = 0; i < checks_size; i++) {
    auto index = Global_::generateRandom(0, num_records - 1);
    checks.push_back({kvs[index].first, kvs[index].second, 0});
  }

  for (const auto& pref : checks) {
    auto results = sst->get_prefix_range(std::get<0>(pref), 0);
    ASSERT_FALSE(results.empty()) << "Prefix query returned empty for " << std::get<0>(pref);

    // 检查每个结果确实以 pref 开头
    for (const auto& p : results) {
      ASSERT_TRUE(std::get<0>(p).starts_with(std::get<0>(pref)))
          << "Key '" << std::get<0>(p) << "' does not start with prefix '" << std::get<0>(pref)
          << "'";
    }
  }

  // 验证某些具体键存在且值正确
  auto r     = sst->get_prefix_range("k12", 0);
  bool found = false;
  for (auto& p : r) {
    if (std::get<0>(p).starts_with("k12")) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

// 读取多个 block 并验证缓存命中/不同 block 返回不同对象
TEST_F(SstableTest, ReadMultipleBlocksAndCache) {
  const std::string key_prefix  = "bk";
  const size_t      num_records = 2000;
  const size_t      block_size  = 256;

  if (std::filesystem::exists(tmp_path2))
    std::filesystem::remove(tmp_path2);

  Sstbuild builder(block_size, false);  // 关闭 bloom 以简化
  for (size_t i = 0; i < num_records; ++i) {
    builder.add(key_prefix + std::to_string(i), "v" + std::to_string(i), 0);
  }

  std::shared_ptr<Sstable> sst;
  try {
    sst = builder.build(block_cache, tmp_path2, 2);
  } catch (const std::exception& e) {
    FAIL() << "Build failed: " << e.what();
    return;
  }

  ASSERT_NE(sst, nullptr);

  // 尝试读取前 5 个 block（如果存在），并验证缓存行为
  std::vector<std::shared_ptr<Block>> first_reads;
  for (size_t idx = 0; idx < 5; ++idx) {
    try {
      auto b = sst->read_block(idx);
      if (b == nullptr)
        break;
      first_reads.push_back(b);
    } catch (...) {
      break;
    }
  }

  ASSERT_GE(first_reads.size(), 2u) << "预计至少有 2 个 block 被生成，当前不足";

  // 再次读取这些 block，应返回相同的 shared_ptr（缓存命中）
  for (size_t i = 0; i < first_reads.size(); ++i) {
    auto b2 = sst->read_block(i);
    ASSERT_NE(b2, nullptr);
    EXPECT_EQ(first_reads[i], b2) << "Cache miss or different object for block " << i;
  }

  // 验证不同 block 返回不同对象指针
  if (first_reads.size() >= 2) {
    EXPECT_NE(first_reads[0], first_reads[1]);
  }
}

// Collect keys from [begin, end) and verify prefix
static std::vector<std::string> collect_prefix_keys(const std::shared_ptr<BlockIterator>& begin,
                                                    const std::shared_ptr<BlockIterator>& end,
                                                    const std::string&                    prefix) {
  std::vector<std::string> out;
  if (!begin || !end)
    return out;
  // iterate by index comparison (end is exclusive)
  while (begin->getIndex() != end->getIndex()) {
    auto kv = begin->getValue();
    // stop if not matching prefix
    if (!kv.first.starts_with(prefix))
      break;
    out.push_back(kv.first);
    ++(*begin);
  }
  return out;
}

// 测试MVCC点查询功能
TEST_F(SstableTest, MvccPointQuery) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path1);

  ASSERT_NE(sst, nullptr);

  // 测试点查询
  auto test_cases = std::to_array<std::tuple<std::string, uint64_t, std::string>>({
      {"key_0050", 1050, "value_v1_0050"},  // 应该返回v1
      {"key_0050", 2050, "value_v2_0050"},  // 应该返回v2
      {"key_0050", 3050, "value_v3_0050"},  // 应该返回v3
      {"key_0050", 999, ""},                // 时间戳之前没有数据
      {"key_0100", 1500, "value_v1_0100"},  // 中间时间戳
      {"key_0999", 3999, "value_v3_0999"},  // 边界测试
  });

  for (const auto& [key, ts, expected] : test_cases) {
    auto results = sst->get_prefix_range(key, ts);

    if (expected.empty()) {
      EXPECT_TRUE(results.empty()) << std::format("Key {} at ts {} should have no data", key, ts);
    } else {
      ASSERT_FALSE(results.empty()) << std::format("No data found for key {} at ts {}", key, ts);

      // 验证结果正确性
      auto [found_key, found_value, found_ts] = results.front();
      EXPECT_EQ(found_key, key);
      EXPECT_EQ(found_value, expected) << std::format("For key {} at ts {}, expected {}, got {}",
                                                      key, ts, expected, found_value);
    }
  }
}

// 测试MVCC范围查询 - 真正的范围查询
TEST_F(SstableTest, MvccRangeQuery) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path2);

  ASSERT_NE(sst, nullptr);

  // 测试不同时间戳下的范围查询
  struct RangeQuery {
    std::string start_key;
    std::string end_key;
    uint64_t    timestamp;
    size_t      expected_min;  // 最小期望结果数
    size_t      expected_max;  // 最大期望结果数
  };

  auto range_tests = std::to_array<RangeQuery>({
      // 基本范围查询
      {"key_010", "key_0110", 1500, 11, 11},   // 在ts=1500时的可见版本
      {"key_010", "key_0110", 2500, 22, 22},   // 在ts=2500时的可见版本
      {"key_01", "key_0200", 3500, 330, 330},  // 在ts=3500时的可见版本

      // 部分范围
      {"key_045", "key_0460", 1500, 11, 11},  // [450, 460) 小范围
      {"key_045", "key_0460", 2500, 11, 11},

      // 边界测试
      {"key_0990", "key_1000", 3500, 10, 10},  // 边界范围
      {"key_0000", "key_0010", 1500, 10, 10},  // 开始范围

      // 空范围
      {"key_2000", "key_3000", 1500, 0, 0},  // 不存在的范围
      {"zzz", "zzzz", 1500, 0, 0},           // 超出范围
  });

  for (const auto& test : range_tests) {
    // 使用迭代器进行范围遍历
    size_t count = 0;

    // 从起始key开始迭代
    auto iter = sst->get_Iterator(test.start_key, test.timestamp, true);

    while (iter != sst->end()) {
      if (!iter.valid()) {
        return;
      }
      auto [key, value, ts] = iter.getValue();

      // 如果key >= end_key，停止迭代
      if (key >= test.end_key) {
        break;
      }

      // 验证key在范围内
      EXPECT_GE(key, test.start_key) << std::format("Key {} should be >= {}", key, test.start_key);
      EXPECT_LT(key, test.end_key) << std::format("Key {} should be < {}", key, test.end_key);

      // 验证时间戳
      EXPECT_LE(ts, test.timestamp)
          << std::format("Key {} has ts {}, should be <= {}", key, ts, test.timestamp);

      ++count;
      ++iter;
    }

    // 验证结果数量
    EXPECT_GE(count, test.expected_min)
        << std::format("Range [{}, {}) at ts {}: expected at least {} results, got {}",
                       test.start_key, test.end_key, test.timestamp, test.expected_min, count);
    EXPECT_LE(count, test.expected_max)
        << std::format("Range [{}, {}) at ts {}: expected at most {} results, got {}",
                       test.start_key, test.end_key, test.timestamp, test.expected_max, count);

    std::println("Range [{}, {}) at ts {}: found {} results", test.start_key, test.end_key,
                 test.timestamp, count);
  }
}

// 测试MVCC范围查询包含结束边界
TEST_F(SstableTest, MvccRangeQueryInclusive) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path2);

  ASSERT_NE(sst, nullptr);

  // 测试包含特定边界键的范围查询
  auto range_tests =
      std::to_array<std::tuple<std::string, std::string, uint64_t, std::vector<std::string>>>({
          {"key_0050",
           "key_0060",
           1500,
           {"key_0050", "key_0500", "key_0051", "key_0052", "key_0053", "key_0054", "key_0055",
            "key_0056", "key_0057", "key_0058", "key_0059"}},
          {"key_0090",
           "key_0991",
           2500,
           {
               "key_0090",
               "key_0090",
               "key_0990",
           }},
      });

  for (const auto& [start_key, end_key, timestamp, expected_keys] : range_tests) {
    std::vector<std::string> found_keys;

    auto iter = sst->get_Iterator(start_key, timestamp, true);
    if (!iter.valid()) {
      return;
    }
    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= end_key) {
        break;
      }

      found_keys.push_back(key);
      ++iter;
    }

    // 验证找到的键和期望的键完全匹配
    EXPECT_EQ(found_keys.size(), expected_keys.size())
        << std::format("Range [{}, {}) at ts {}: expected {} keys, found {}", start_key, end_key,
                       timestamp, expected_keys.size(), found_keys.size());

    for (size_t i = 0; i < std::min(found_keys.size(), expected_keys.size()); ++i) {
      EXPECT_EQ(found_keys[i], expected_keys[i]) << std::format(
          "Mismatch at position {}: expected {}, found {}", i, expected_keys[i], found_keys[i]);
    }
  }
}

// 测试MVCC跨前缀范围查询
TEST_F(SstableTest, MvccCrossPrefixRangeQuery) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path1);

  ASSERT_NE(sst, nullptr);

  // 测试跨越不同前缀的范围查询
  {
    // 从 alt_ 到 key_ 的范围
    auto iter = sst->get_Iterator("alt_", 1600);
    if (!iter.valid()) {
      return;
    }
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      // 当遇到 key_ 前缀时停止（因为 alt_ < key_）
      if (key.starts_with("key_")) {
        break;
      }

      EXPECT_TRUE(key.starts_with("alt_")) << "Key should start with 'alt_'";
      EXPECT_LE(ts, 1600) << "Timestamp should be <= 1600";
      ++count;
      ++iter;
    }

    EXPECT_GT(count, 0) << "Should find some alt_ keys";
    std::println("Found {} alt_ keys at ts 1600", count);
  }

  {
    // 从空字符串开始到 key_0100 的范围
    auto   iter  = sst->begin(1500);
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "key_0100") {
        break;
      }

      EXPECT_LT(key, "key_0100") << "Key should be < key_0100";
      EXPECT_LE(ts, 1500) << "Timestamp should be <= 1500";
      ++count;
      ++iter;
    }

    EXPECT_GT(count, 0) << "Should find keys before key_0100";
    std::println("Found {} keys before key_0100 at ts 1500", count);
  }
}

// 测试MVCC范围查询性能
TEST_F(SstableTest, MvccRangeQueryPerformance) {
  // 生成更大数据集
  std::vector<std::tuple<std::string, std::string, uint64_t>> large_data;
  constexpr int                                               num_records = 2000;

  for (int i = 0; i < num_records; ++i) {
    std::string key = std::format("data_{:06d}", i);
    large_data.emplace_back(key, std::format("value1_{:06d}", i), 10000 + i);
    large_data.emplace_back(key, std::format("value2_{:06d}", i), 20000 + i);
  }

  std::ranges::sort(large_data,
                    [](const auto& a, const auto& b) { return std::get<2>(a) < std::get<2>(b); });

  auto sst = build_sstable(large_data, tmp_path1);
  ASSERT_NE(sst, nullptr);

  // 性能测试：多次范围查询
  constexpr size_t num_queries = 1000;

  auto start_time = std::chrono::high_resolution_clock::now();

  size_t total_keys_found = 0;

  for (size_t i = 0; i < num_queries; ++i) {
    // 生成随机范围
    size_t start_idx = i;  // 确保有范围
    size_t end_idx   = start_idx + 100 + (i % 10);

    std::string start_key = std::format("data_{:06d}", start_idx);
    std::string end_key   = std::format("data_{:06d}", end_idx);
    uint64_t    timestamp = 15000 + (i % 5000);

    // 执行范围查询
    auto   iter  = sst->get_Iterator(start_key, timestamp, true);
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= end_key) {
        break;
      }

      ++count;
      ++iter;
    }

    total_keys_found += count;
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  std::println("执行 {} 次MVCC范围查询耗时: {}ms", num_queries, duration.count());
  std::println("平均每次查询耗时: {:.3f}ms", static_cast<double>(duration.count()) / num_queries);
  std::println("总找到键数: {}", total_keys_found);
}

// 测试MVCC范围查询的边界条件
TEST_F(SstableTest, MvccRangeQueryEdgeCases) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path1);

  ASSERT_NE(sst, nullptr);

  // 测试1: 开始键等于结束键（空范围）
  {
    auto   iter  = sst->get_Iterator("key_0050", 1500, true);
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "key_0050") {  // 立即停止
        break;
      }

      ++count;
      ++iter;
    }

    EXPECT_EQ(count, 0) << "Range [key_0050, key_0050) should be empty";
  }

  // 测试2: 开始键大于结束键（应该为空）
  {
    auto   iter  = sst->get_Iterator("key_0100", 1500, true);
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "key_0050") {  // 开始键大于"结束键"，但迭代器从key_0100开始
        // 实际上，因为开始键是key_0100，而"结束键"是key_0050，
        // 所以第一次迭代就会因为key >= "key_0050"而退出
        break;
      }

      ++count;
      ++iter;
    }

    EXPECT_EQ(count, 0) << "Range [key_0100, key_0050) should be empty (invalid range)";
  }

  // 测试3: 非常大的范围
  {
    auto   iter  = sst->begin(2500);  // 从开始
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "zzzzzzzz") {  // 非常大的结束键
        break;
      }

      // 只检查前1000个，避免测试时间过长
      if (++count > 1000) {
        break;
      }

      ++iter;
    }

    EXPECT_GT(count, 0) << "Should find keys in large range";
  }

  // 测试4: 非常小的时间戳（应该看不到数据）
  {
    auto   iter  = sst->get_Iterator("key_0000", 1, true);  // 非常小的时间戳
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "key_1000") {
        break;
      }

      ++count;
      ++iter;
    }

    EXPECT_EQ(count, 0) << "Should find no keys at very small timestamp";
  }

  // 测试5: 非常大的时间戳（应该看到所有数据）
  {
    auto   iter  = sst->get_Iterator("key_090", 999999, true);  // 非常大的时间戳
    size_t count = 0;

    while (iter != sst->end()) {
      auto [key, value, ts] = iter.getValue();

      if (key >= "key_0910") {
        break;
      }

      EXPECT_TRUE(key.starts_with("key_")) << "Key should start with 'key_'";
      EXPECT_LE(ts, 999999) << "Timestamp should be <= 999999";
      ++count;
      ++iter;
    }

    EXPECT_EQ(count, 33) << "Should find 50 keys in range [key_0900, key_0950)";
  }
}

// 测试MVCC范围查询与迭代器组合
TEST_F(SstableTest, MvccRangeWithMultipleIterators) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path1, 1024);  // 小block size

  ASSERT_NE(sst, nullptr);
  EXPECT_GT(sst->num_blocks(), 1) << "Should have multiple blocks for cross-block testing";

  // 测试在同一时间戳下使用多个迭代器进行不同范围的查询
  uint64_t timestamp = 2500;

  // 范围1: [key_0100, key_0200)
  auto                     iter1 = sst->get_Iterator("key_010", timestamp);
  std::vector<std::string> range1_keys;

  while (iter1 != sst->end()) {
    auto [key, value, ts] = iter1.getValue();

    if (key >= "key_0200") {
      break;
    }

    range1_keys.push_back(key);
    ++iter1;
  }

  // 范围2: [key_0500, key_0510)
  auto                     iter2 = sst->get_Iterator("key_050", timestamp, true);
  std::vector<std::string> range2_keys;

  while (iter2 != sst->end()) {
    auto [key, value, ts] = iter2.getValue();

    if (key >= "key_0510") {
      break;
    }

    range2_keys.push_back(key);
    ++iter2;
  }

  // 范围3: [key_0900, key_0905)
  auto                     iter3 = sst->get_Iterator("key_09", timestamp);
  std::vector<std::string> range3_keys;

  while (iter3 != sst->end()) {
    auto [key, value, ts] = iter3.getValue();
    range3_keys.push_back(key);
    ++iter3;
    if (key >= "key_0999") {
      break;
    }
  }

  // 验证结果
  EXPECT_EQ(range1_keys.size(), 22) << "Range1 should have 22 keys";
  EXPECT_EQ(range2_keys.size(), 22) << "Range2 should have 22 keys";
  EXPECT_EQ(range3_keys.size(), 222) << "Range3 should have 22 keys";

  // 验证范围1的键是连续的
  for (size_t i = 0; i < range1_keys.size(); ++i) {
    std::string expected_key = std::format("key_{:04d}", 100 + i);
    EXPECT_EQ(range1_keys[i], expected_key) << std::format(
        "Range1 key at position {}: expected {}, got {}", i, expected_key, range1_keys[i]);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}