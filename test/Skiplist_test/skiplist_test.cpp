#include "../../include/Skiplist.h"
#include <gtest/gtest.h>
#include <cmath>
#include <string>
#include <print>
#include <utility>
#include <vector>
#include <chrono>
#include <format>
#include <memory>
#include <random>

class SkiplistTest : public ::testing::Test {
 protected:
  void SetUp() override {
    skiplist = std::make_unique<Skiplist>();
    // 提前准备好测试数据，避免在计时循环中构造
    prepare_test_data(10000);
  }

  void prepare_test_data(size_t size) {
    test_keys.clear();
    test_values.clear();
    test_keys.reserve(size);
    test_values.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      // 使用format代替字符串拼接
      test_keys.emplace_back(std::format("key_{:08}", i));
      test_values.emplace_back(std::format("value_{:08}", i));
    }
  }

  std::unique_ptr<Skiplist> skiplist;
  std::vector<std::string>  test_keys;
  std::vector<std::string>  test_values;
};

// 基本插入和查询测试
TEST_F(SkiplistTest, BasicInsertAndGet) {
  EXPECT_TRUE(skiplist->Insert("key1", "value1"));
  EXPECT_TRUE(skiplist->Insert("key2", "value2"));

  auto result1 = skiplist->Contain("key1");
  EXPECT_TRUE(result1.has_value());
  EXPECT_EQ(result1.value(), "value1");

  auto result2 = skiplist->Contain("key2");
  EXPECT_TRUE(result2.has_value());
  EXPECT_EQ(result2.value(), "value2");
}

// 测试删除功能
TEST_F(SkiplistTest, DeleteTest) {
  EXPECT_TRUE(skiplist->Insert("key1", "value1"));
  EXPECT_TRUE(skiplist->Delete("key1"));
  auto result = skiplist->Contain("key1");
  EXPECT_FALSE(result.has_value());
}

// 微基准测试：纯操作时间
TEST_F(SkiplistTest, MicroBenchmark) {
  constexpr size_t NUM_OPS = 10000;

  // 纯插入时间（不含数据构造）
  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPS; ++i) {
    skiplist->Insert(test_keys[i], test_values[i]);
  }
  auto end         = std::chrono::high_resolution_clock::now();
  auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // 纯查找时间
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPS; ++i) {
    auto result = skiplist->Contain(test_keys[i]);
    EXPECT_TRUE(result.has_value());
  }
  end              = std::chrono::high_resolution_clock::now();
  auto lookup_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // 纯删除时间
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPS; ++i) {
    EXPECT_TRUE(skiplist->Delete(test_keys[i]));
  }
  end              = std::chrono::high_resolution_clock::now();
  auto delete_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::print(stdout, "微基准测试 ({} 次操作):\n", NUM_OPS);
  std::print(stdout, "  插入: {} µs (平均: {:.2f} µs/操作)\n", insert_time.count(),
             static_cast<double>(insert_time.count()) / NUM_OPS);
  std::print(stdout, "  查找: {} µs (平均: {:.2f} µs/操作)\n", lookup_time.count(),
             static_cast<double>(lookup_time.count()) / NUM_OPS);
  std::print(stdout, "  删除: {} µs (平均: {:.2f} µs/操作)\n", delete_time.count(),
             static_cast<double>(delete_time.count()) / NUM_OPS);
}

// 精确性能测试
TEST_F(SkiplistTest, PrecisionPerformanceTest) {
  constexpr size_t NUM_OPERATIONS = 10000;

  // 预热：避免冷启动影响
  {
    Skiplist warmup_list;
    for (size_t i = 0; i < 10000; ++i) {
      warmup_list.Insert(test_keys[i], test_values[i]);
      warmup_list.Contain(test_keys[i]);
      warmup_list.Delete(test_keys[i]);
    }
  }

  // 基准测试：插入
  auto insert_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
    skiplist->Insert(test_keys[i], test_values[i]);
  }
  auto insert_end = std::chrono::high_resolution_clock::now();
  auto insert_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(insert_end - insert_start);

  // 基准测试：随机查找
  std::mt19937                          rng(std::random_device{}());
  std::uniform_int_distribution<size_t> dist(0, NUM_OPERATIONS - 1);

  auto lookup_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
    size_t idx    = dist(rng);
    auto   result = skiplist->Contain(test_keys[idx]);
    EXPECT_TRUE(result.has_value());
  }
  auto lookup_end = std::chrono::high_resolution_clock::now();
  auto lookup_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(lookup_end - lookup_start);

  // 基准测试：顺序查找（更好的缓存局部性）
  auto seq_lookup_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
    auto result = skiplist->Contain(test_keys[i]);
    EXPECT_TRUE(result.has_value());
  }
  auto seq_lookup_end = std::chrono::high_resolution_clock::now();
  auto seq_lookup_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(seq_lookup_end - seq_lookup_start);

  // 基准测试：删除
  auto delete_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
    EXPECT_TRUE(skiplist->Delete(test_keys[i]));
  }
  auto delete_end = std::chrono::high_resolution_clock::now();
  auto delete_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(delete_end - delete_start);

  std::print(stdout, "\n精确性能测试结果 ({} 次操作):\n", NUM_OPERATIONS);
  std::print(stdout, "插入性能:\n");
  std::print(stdout, "  总时间: {} µs\n", insert_duration.count());
  std::print(stdout, "  平均时间: {:.3f} µs/操作\n",
             static_cast<double>(insert_duration.count()) / NUM_OPERATIONS);
  std::print(stdout, "  吞吐量: {:.0f} 操作/秒\n",
             1e6 / (static_cast<double>(insert_duration.count()) / NUM_OPERATIONS));

  std::print(stdout, "\n随机查找性能:\n");
  std::print(stdout, "  总时间: {} µs\n", lookup_duration.count());
  std::print(stdout, "  平均时间: {:.3f} µs/操作\n",
             static_cast<double>(lookup_duration.count()) / NUM_OPERATIONS);

  std::print(stdout, "\n顺序查找性能:\n");
  std::print(stdout, "  总时间: {} µs\n", seq_lookup_duration.count());
  std::print(stdout, "  平均时间: {:.3f} µs/操作\n",
             static_cast<double>(seq_lookup_duration.count()) / NUM_OPERATIONS);
  std::print(
      stdout, "  缓存优势: {:.1f}%\n",
      (1.0 - static_cast<double>(seq_lookup_duration.count()) / lookup_duration.count()) * 100);

  std::print(stdout, "\n删除性能:\n");
  std::print(stdout, "  总时间: {} µs\n", delete_duration.count());
  std::print(stdout, "  平均时间: {:.3f} µs/操作\n",
             static_cast<double>(delete_duration.count()) / NUM_OPERATIONS);
}

// 内存分析测试
TEST_F(SkiplistTest, MemoryAnalysisTest1) {
  constexpr std::array<size_t, 3> TEST_SIZES = {100, 1000, 10000};
  std::vector<double>             memory_efficiency;

  for (size_t size : TEST_SIZES) {
    std::print("\n=== 测试数据量: {} 条记录 ===\n", size);

    // 创建新的skiplist实例
    auto test_list = std::make_unique<Skiplist>();

    // 记录初始内存使用
    size_t initial_memory = test_list->get_size();
    std::print("初始内存占用: {} bytes\n", initial_memory);
    std::print("头节点数量: {}\n", test_list->getnodecount());

    // 插入数据
    auto insert_start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < size; ++i) {
      std::string key     = std::format("test_key_{:08}", i);
      std::string value   = std::format("test_value_{:08}", i);
      bool        success = test_list->Insert(key, value);
      if (!success) {
        std::print("警告: 插入失败 at i={}\n", i);
      }
    }
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto insert_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start);

    // 计算内存使用
    size_t final_memory = test_list->get_size();
    size_t memory_used  = final_memory - initial_memory;

    std::print("最终内存占用: {} bytes\n", final_memory);
    std::print("节点数量: {}\n", test_list->getnodecount());

    // 验证节点数量
    EXPECT_EQ(test_list->getnodecount(), size)
        << std::format("节点数量不匹配! 期望: {}, 实际: {}", size, test_list->getnodecount());

    // 计算内存效率
    if (size > 0) {
      double bytes_per_entry = static_cast<double>(memory_used) / size;
      memory_efficiency.push_back(bytes_per_entry);

      std::print("净内存增加: {} bytes\n", memory_used);
      std::print("平均每节点: {:.2f} bytes\n", bytes_per_entry);

      // 估算预期内存
      double estimated_bytes = 20 + 20 + (Global_::FIX_LEVEL * 8) + 8 + 8;
      std::print("估算每节点: {:.0f} bytes\n", estimated_bytes);
      std::print("内存使用率: {:.1f}%\n", (bytes_per_entry / estimated_bytes * 100));

      // 插入性能
      std::print("插入时间: {} ms\n", insert_time.count());
      std::print("平均插入时间: {:.3f} ms/操作\n", static_cast<double>(insert_time.count()) / size);

      // 内存使用验证
      EXPECT_GT(bytes_per_entry, 20.0) << "内存使用过少，可能统计有误";
      EXPECT_LT(bytes_per_entry, 200.0) << "内存使用过多，可能内存泄漏";
    }

    // 验证数据完整性
    std::print("验证数据完整性...\n");
    size_t found_count = 0;
    for (size_t i = 0; i < size; ++i) {
      std::string key    = std::format("test_key_{:08}", i);
      auto        result = test_list->Contain(key);
      if (result.has_value()) {
        found_count++;
      } else {
        std::print("警告: 找不到键 {}\n", key);
      }
    }
    std::print("找到 {}/{} 个键\n", found_count, size);
    EXPECT_EQ(found_count, size) << "数据完整性检查失败";

    // 测试删除后的内存释放
    std::print("测试删除所有节点...\n");
    auto delete_start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < size; ++i) {
      std::string key = std::format("test_key_{:08}", i);
      test_list->Delete(key);
    }
    auto delete_end = std::chrono::high_resolution_clock::now();
    auto delete_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(delete_end - delete_start);

    size_t after_delete_memory = test_list->get_size();
    std::print("删除后内存占用: {} bytes\n", after_delete_memory);
    std::print("删除后节点数量: {}\n", test_list->getnodecount());
    std::print("删除时间: {} ms\n", delete_time.count());

    // 删除后应该只剩下头节点
    EXPECT_EQ(test_list->getnodecount(), 0) << "删除后节点数应为0";

    // 内存回收检查
    double memory_reclaim_ratio = static_cast<double>(after_delete_memory) / initial_memory;
    std::print("内存回收率: {:.1f}%\n", memory_reclaim_ratio * 100);
    EXPECT_LT(memory_reclaim_ratio, 2.0) << "内存回收异常，可能内存泄漏";
  }

  // 分析内存使用趋势
  if (memory_efficiency.size() > 1) {
    std::print("\n=== 内存使用趋势分析 ===\n");
    for (size_t i = 1; i < memory_efficiency.size(); ++i) {
      double growth = memory_efficiency[i] / memory_efficiency[i - 1];
      std::print("从 {} 到 {}: 内存效率变化比率 = {:.3f}\n", TEST_SIZES[i - 1], TEST_SIZES[i],
                 growth);

      // 随着数据量增加，内存效率应趋于稳定
      EXPECT_GT(growth, 0.9) << std::format("内存效率下降过多: {:.3f}", growth);
      EXPECT_LT(growth, 1.1) << std::format("内存效率增加过多: {:.3f}", growth);
    }
  }

  // 额外测试：随机操作内存稳定性
  std::print("\n=== 随机操作内存稳定性测试 ===\n");
  {
    Skiplist                 random_list;
    constexpr size_t         OPERATIONS = 1000;
    std::vector<std::string> inserted_keys;

    std::mt19937                       rng(std::random_device{}());
    std::uniform_int_distribution<int> op_dist(0, 2);
    std::uniform_int_distribution<int> key_dist(0, 1000);

    size_t max_memory = 0;
    size_t min_memory = std::numeric_limits<size_t>::max();

    for (size_t i = 0; i < OPERATIONS; ++i) {
      int         op      = op_dist(rng);
      int         key_num = key_dist(rng);
      std::string key     = std::format("rand_key_{:04}", key_num);

      switch (op) {
        case 0:  // 插入
          random_list.Insert(key, "random_value");
          inserted_keys.push_back(key);
          break;
        case 1:  // 删除
          if (!inserted_keys.empty()) {
            size_t idx = key_num % inserted_keys.size();
            random_list.Delete(inserted_keys[idx]);
            inserted_keys.erase(inserted_keys.begin() + idx);
          }
          break;
        case 2:  // 查找
          random_list.Contain(key);
          break;
      }

      // 记录内存使用
      size_t current_memory = random_list.get_size();
      max_memory            = std::max(max_memory, current_memory);
      min_memory            = std::min(min_memory, current_memory);
    }

    std::print("随机操作后节点数: {}\n", random_list.getnodecount());
    std::print("最大内存使用: {} bytes\n", max_memory);
    std::print("最小内存使用: {} bytes\n", min_memory);
    std::print("内存波动: {} bytes\n", max_memory - min_memory);

    // 验证随机操作后跳表仍然可用
    for (const auto& key : inserted_keys) {
      auto result = random_list.Contain(key);
      EXPECT_TRUE(result.has_value()) << std::format("随机操作后键 {} 应该存在", key);
    }
  }
}
// 范围查询性能测试
// 范围查询性能测试（简化的前缀匹配测试）
TEST_F(SkiplistTest, RangeQueryPerformanceTest) {
  constexpr size_t NUM_ENTRIES = 101;

  // 插入测试数据 - 包含各种key模式
  for (size_t i = 0; i < NUM_ENTRIES; ++i) {
    // 插入 key1, key10, key11, key100,
    std::string key   = std::format("key{}", i);
    std::string value = std::format("value{}", i);
    skiplist->Insert(key, value, 0);
  }

  // 额外插入一些特定模式的key用于测试
  std::print("跳表统计 - 节点数: {}, 大小: {} bytes\n", skiplist->getnodecount(),
             skiplist->get_size());

  // 测试不同的范围/前缀查询
  std::vector<std::pair<std::string, std::string>> test_cases = {
      {"key1", "value1"},  // 查找 key1 开头的所有键
      {"key10", "value10"}, {"key100", "value100"}, {"key11", "value11"}, {"key12", "value12"},
      {"key13", "value13"}, {"key14", "value14"},   {"key15", "value15"}, {"key16", "value16"},
      {"key17", "value17"}, {"key18", "value18"},   {"key19", "value19"}  // 查找所有key,value
  };
  size_t                                           count = 0;
  auto                                             BEFIN = skiplist->prefix_serach_begin("key1");
  auto                                             END   = skiplist->prefix_serach_end("key1");
  std::vector<std::pair<std::string, std::string>> results;
  for (auto begin = BEFIN; begin != END; ++begin) {
    results.push_back(begin.getValue());
    count++;
  }
  EXPECT_TRUE(results == test_cases);
  for (auto [k, v] : results) {
    std::print("  key: {}, value: {}\n", k, v);
  }
  std::print("  查询到的记录数: {}\n", count);
}

// ==================== 测试1：基本MVCC可见性 ====================
TEST_F(SkiplistTest, BasicMVCCVisibility) {
  std::print("=== 测试1：基本MVCC可见性 ===\n");

  const std::string key = "test_key";

  // 插入不同事务ID的版本
  // 版本1: txId=100, value="value_100"
  EXPECT_TRUE(skiplist->Insert(key, "value_100", 100));

  // 版本2: txId=200, value="value_200"
  EXPECT_TRUE(skiplist->Insert(key, "value_200", 200));

  // 版本3: txId=300, value="value_300"
  EXPECT_TRUE(skiplist->Insert(key, "value_300", 300));

  std::print("插入完成: key={} 有3个版本 (txId=100,200,300)\n", key);

  // ============ 验证MVCC可见性 ============

  // 测试1: txId=99时，应该看不到任何版本（因为都大于99）
  {
    auto result = skiplist->Contain(key, 99);
    EXPECT_FALSE(result.has_value())
        << "错误：txId=99应该看不到任何版本，实际看到了: " << result.value();
    std::print("  txId=99 -> 期望: [空], 实际: [空] ✓\n");
  }

  // 测试2: txId=100时，应该看到版本1
  {
    auto result = skiplist->Contain(key, 100);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_100")
        << "错误：txId=100应该看到'value_100'，实际: " << result.value();
    std::print("  txId=100 -> 期望: value_100, 实际: {} ✓\n", result.value());
  }

  // 测试3: txId=150时，应该看到版本1（最接近且≤150的是100）
  {
    auto result = skiplist->Contain(key, 150);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_100")
        << "错误：txId=150应该看到'value_100'，实际: " << result.value();
    std::print("  txId=150 -> 期望: value_100, 实际: {} ✓\n", result.value());
  }

  // 测试4: txId=200时，应该看到版本2
  {
    auto result = skiplist->Contain(key, 200);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_200")
        << "错误：txId=200应该看到'value_200'，实际: " << result.value();
    std::print("  txId=200 -> 期望: value_200, 实际: {} ✓\n", result.value());
  }

  // 测试5: txId=250时，应该看到版本2
  {
    auto result = skiplist->Contain(key, 250);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_200")
        << "错误：txId=250应该看到'value_200'，实际: " << result.value();
    std::print("  txId=250 -> 期望: value_200, 实际: {} ✓\n", result.value());
  }

  // 测试6: txId=300时，应该看到版本3
  {
    auto result = skiplist->Contain(key, 300);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_300")
        << "错误：txId=300应该看到'value_300'，实际: " << result.value();
    std::print("  txId=300 -> 期望: value_300, 实际: {} ✓\n", result.value());
  }

  // 测试7: txId=400时，应该看到版本3
  {
    auto result = skiplist->Contain(key, 400);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_300")
        << "错误：txId=400应该看到'value_300'，实际: " << result.value();
    std::print("  txId=400 -> 期望: value_300, 实际: {} ✓\n", result.value());
  }

  std::print("=== 测试1完成 ===\n\n");
}

// ==================== 测试2：MVCC删除可见性 ====================
TEST_F(SkiplistTest, MVCCDeleteVisibility) {
  std::print("=== 测试2：MVCC删除可见性 ===\n");

  const std::string key = "test_key_delete";

  // 插入不同事务ID的版本
  // 版本1: txId=100, value="value_100"
  EXPECT_TRUE(skiplist->Insert(key, "value_100", 100));

  // 版本2: txId=200, value="value_200"
  EXPECT_TRUE(skiplist->Insert(key, "value_200", 200));

  // 版本3: txId=300, 删除标记（空值）
  EXPECT_TRUE(skiplist->Insert(key, "", 300));

  // 版本4: txId=400, 重新插入
  EXPECT_TRUE(skiplist->Insert(key, "value_400", 400));

  std::print("插入完成: key={} 有4个版本 (插入→更新→删除→重新插入)\n", key);

  // ============ 验证MVCC可见性（包含删除） ============

  // 测试1: txId=250时，应该看到版本2（删除之前）
  {
    auto result = skiplist->Contain(key, 250);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_200")
        << "错误：txId=250应该看到'value_200'，实际: " << result.value();
    std::print("  txId=250 -> 期望: value_200, 实际: {} ✓\n", result.value());
  }

  // 测试2: txId=300时，应该看到删除（空值）
  {
    auto result = skiplist->Contain(key, 300);
    EXPECT_FALSE(result.has_value())  // 删除标记应该返回空
        << "错误：txId=300应该看到删除（空），实际: " << result.value();
    std::print("  txId=300 -> 期望: [空], 实际: [空] ✓\n");
  }

  // 测试3: txId=350时，应该看到删除（空值）
  {
    auto result = skiplist->Contain(key, 350);
    EXPECT_FALSE(result.has_value()) << "错误：txId=350应该看到删除（空），实际: "
                                     << (result.has_value() ? result.value() : "[空]");
    std::print("  txId=350 -> 期望: [空], 实际: [空] ✓\n");
  }

  // 测试4: txId=400时，应该看到重新插入的值
  {
    auto result = skiplist->Contain(key, 400);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_400")
        << "错误：txId=400应该看到'value_400'，实际: " << result.value();
    std::print("  txId=400 -> 期望: value_400, 实际: {} ✓\n", result.value());
  }

  // 测试5: txId=500时，应该看到重新插入的值
  {
    auto result = skiplist->Contain(key, 500);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_400")
        << "错误：txId=500应该看到'value_400'，实际: " << result.value();
    std::print("  txId=500 -> 期望: value_400, 实际: {} ✓\n", result.value());
  }

  std::print("=== 测试2完成 ===\n\n");
}

// ==================== 测试3：多key MVCC可见性 ====================
TEST_F(SkiplistTest, MultipleKeysMVCCVisibility) {
  std::print("=== 测试3：多key MVCC可见性 ===\n");

  // 为多个key创建不同的版本历史
  struct KeyVersion {
    std::string                                   key;
    std::vector<std::pair<uint64_t, std::string>> versions;  // (txId, value)
  };

  std::vector<KeyVersion> testKeys = {
      {"key1", {{100, "v1_100"}, {200, "v1_200"}, {300, ""}, {400, "v1_400"}}},
      {"key2", {{150, "v2_150"}, {250, "v2_250"}, {350, "v2_350"}}},
      {"key3", {{120, "v3_120"}, {220, "v3_220"}, {320, "v3_320"}, {420, ""}}},
  };

  // 插入所有版本
  for (const auto& keyVer : testKeys) {
    for (const auto& [txId, value] : keyVer.versions) {
      EXPECT_TRUE(skiplist->Insert(keyVer.key, value, txId));
    }
    std::print("插入key={}: {}个版本\n", keyVer.key, keyVer.versions.size());
  }

  // ============ 测试不同事务时间点的全局快照 ============
  std::print("\n测试不同事务时间点的全局快照:\n");

  // 定义测试时间点和期望结果
  struct SnapshotTest {
    uint64_t                                                    txId;
    std::unordered_map<std::string, std::optional<std::string>> expected;  // key -> value/nullopt
  };

  std::vector<SnapshotTest> snapshotTests = {
      // txId=90: 所有key都不可见
      {90, {{"key1", std::nullopt}, {"key2", std::nullopt}, {"key3", std::nullopt}}},

      // txId=110: key1:v1_100, key2:空, key3:v3_120
      {110, {{"key1", "v1_100"}, {"key2", std::nullopt}, {"key3", std::nullopt}}},

      // txId=180: key1:v1_100, key2:v2_150, key3:v3_120
      {180, {{"key1", "v1_100"}, {"key2", "v2_150"}, {"key3", "v3_120"}}},

      // txId=250: key1:v1_200, key2:v2_250, key3:v3_220
      {250, {{"key1", "v1_200"}, {"key2", "v2_250"}, {"key3", "v3_220"}}},

      // txId=320: key1:空(删除), key2:v2_350, key3:v3_320
      {320, {{"key1", std::nullopt}, {"key2", "v2_250"}, {"key3", "v3_320"}}},

      // txId=450: key1:v1_400, key2:v2_350, key3:空(删除)
      {450, {{"key1", "v1_400"}, {"key2", "v2_350"}, {"key3", std::nullopt}}},
  };

  size_t totalTests  = 0;
  size_t passedTests = 0;

  for (const auto& snapshot : snapshotTests) {
    std::print("\n测试全局快照 txId={}:\n", snapshot.txId);

    for (const auto& [key, expectedValue] : snapshot.expected) {
      totalTests++;

      auto actual = skiplist->Contain(key, snapshot.txId);

      if (expectedValue.has_value()) {
        // 期望有值
        if (actual.has_value() && actual.value() == expectedValue.value()) {
          passedTests++;
          std::print("  key={}: 期望 {}, 实际 {} ✓\n", key, expectedValue.value(), actual.value());
        } else {
          std::print("  key={}: 期望 {}, 实际 {} ✗\n", key, expectedValue.value(),
                     actual.has_value() ? actual.value() : "[空]");
        }
      } else {
        // 期望空值
        if (!actual.has_value()) {
          passedTests++;
          std::print("  key={}: 期望 [空], 实际 [空] ✓\n", key);
        } else {
          std::print("  key={}: 期望 [空], 实际 {} ✗\n", key, actual.value());
        }
      }
    }
  }

  std::print("\n全局快照测试结果: {}/{} 通过\n", passedTests, totalTests);
  EXPECT_EQ(passedTests, totalTests) << "全局快照测试失败";

  std::print("=== 测试3完成 ===\n\n");
}

// ==================== 测试4：复杂版本交错测试 ====================
TEST_F(SkiplistTest, ComplexVersionInterleaving) {
  std::print("=== 测试4：复杂版本交错测试 ===\n");

  // 创建多个key，交错插入版本，模拟真实事务交错
  const std::vector<std::string> keys = {"A", "B", "C", "D", "E"};

  // 按事务ID顺序执行的操作列表: (txId, key, value)
  std::vector<std::tuple<uint64_t, std::string, std::string>> operations = {
      // 第一批操作
      {100, "A", "A_v100"},
      {150, "B", "B_v150"},
      {200, "C", "C_v200"},
      {250, "D", "D_v250"},
      {300, "E", "E_v300"},

      // 第二批操作（更新）
      {350, "A", "A_v350"},
      {400, "C", "C_v400"},
      {450, "E", ""},  // 删除E

      // 第三批操作（更多更新）
      {500, "B", "B_v500"},
      {550, "D", ""},  // 删除D
      {600, "A", ""},  // 删除A

      // 第四批操作（重新插入）
      {650, "A", "A_v650"},
      {700, "D", "D_v700"},
  };

  // 执行所有操作
  for (const auto& [txId, key, value] : operations) {
    EXPECT_TRUE(skiplist->Insert(key, value, txId));
  }

  std::print("执行了{}个操作，涉及{}个key\n", operations.size(), keys.size());

  // ============ 验证关键时间点的状态 ============
  std::print("\n验证关键时间点的状态:\n");

  // 测试点1: txId=325（第一和第二批次之间）
  {
    uint64_t testTxId = 325;
    std::print("\ntxId={}:\n", testTxId);

    std::unordered_map<std::string, std::optional<std::string>> expected = {
        {"A", "A_v100"}, {"B", "B_v150"}, {"C", "C_v200"}, {"D", "D_v250"}, {"E", "E_v300"},
    };

    for (const auto& key : keys) {
      auto actual        = skiplist->Contain(key, testTxId);
      auto expectedValue = expected[key];

      if (expectedValue.has_value()) {
        EXPECT_TRUE(actual.has_value());
        EXPECT_EQ(actual.value(), expectedValue.value());
        std::print("  key={}: 期望 {}, 实际 {} ✓\n", key, expectedValue.value(), actual.value());
      } else {
        EXPECT_FALSE(actual.has_value());
        std::print("  key={}: 期望 [空], 实际 [空] ✓\n", key);
      }
    }
  }

  // 测试点2: txId=475（第二和第三批次之间）
  {
    uint64_t testTxId = 475;
    std::print("\ntxId={}:\n", testTxId);

    std::unordered_map<std::string, std::optional<std::string>> expected = {
        {"A", "A_v350"}, {"B", "B_v150"},     {"C", "C_v400"},
        {"D", "D_v250"}, {"E", std::nullopt},  // 已删除
    };

    for (const auto& key : keys) {
      auto actual        = skiplist->Contain(key, testTxId);
      auto expectedValue = expected[key];

      bool passed = false;
      if (expectedValue.has_value()) {
        passed = actual.has_value() && actual.value() == expectedValue.value();
        std::print("  key={}: 期望 {}, 实际 {} {}\n", key, expectedValue.value(),
                   actual.has_value() ? actual.value() : "[空]", passed ? "✓" : "✗");
      } else {
        passed = !actual.has_value();
        std::print("  key={}: 期望 [空], 实际 [空] {}\n", key, passed ? "✓" : "✗");
      }

      EXPECT_TRUE(passed);
    }
  }

  // 测试点3: txId=625（第三和第四批次之间）
  {
    uint64_t testTxId = 625;
    std::print("\ntxId={}:\n", testTxId);

    std::unordered_map<std::string, std::optional<std::string>> expected = {
        {"A", std::nullopt},                                        // 已删除
        {"B", "B_v500"},     {"C", "C_v400"}, {"D", std::nullopt},  // 已删除
        {"E", std::nullopt},                                        // 已删除
    };

    for (const auto& key : keys) {
      auto actual        = skiplist->Contain(key, testTxId);
      auto expectedValue = expected[key];

      bool passed = false;
      if (expectedValue.has_value()) {
        passed = actual.has_value() && actual.value() == expectedValue.value();
        std::print("  key={}: 期望 {}, 实际 {} {}\n", key, expectedValue.value(),
                   actual.has_value() ? actual.value() : "[空]", passed ? "✓" : "✗");
      } else {
        passed = !actual.has_value();
        std::print("  key={}: 期望 [空], 实际 {} {}\n", key,
                   actual.has_value() ? actual.value() : "[空]", passed ? "✓" : "✗");
      }

      EXPECT_TRUE(passed);
    }
  }

  // 测试点4: txId=750（所有操作之后）
  {
    uint64_t testTxId = 750;
    std::print("\ntxId={}:\n", testTxId);

    std::unordered_map<std::string, std::optional<std::string>> expected = {
        {"A", "A_v650"}, {"B", "B_v500"},     {"C", "C_v400"},
        {"D", "D_v700"}, {"E", std::nullopt},  // 已删除
    };

    for (const auto& key : keys) {
      auto actual        = skiplist->Contain(key, testTxId);
      auto expectedValue = expected[key];

      bool passed = false;
      if (expectedValue.has_value()) {
        passed = actual.has_value() && actual.value() == expectedValue.value();
        std::print("  key={}: 期望 {}, 实际 {} {}\n", key, expectedValue.value(),
                   actual.has_value() ? actual.value() : "[空]", passed ? "✓" : "✗");
      } else {
        passed = !actual.has_value();
        std::print("  key={}: 期望 [空], 实际 {} {}\n", key,
                   actual.has_value() ? actual.value() : "[空]", passed ? "✓" : "✗");
      }

      EXPECT_TRUE(passed);
    }
  }

  std::print("\n=== 测试4完成 ===\n\n");
}

// ==================== 测试5：边界条件测试 ====================
TEST_F(SkiplistTest, EdgeCases) {
  std::print("=== 测试5：边界条件测试 ===\n");

  // 测试1: 完全不存在的key
  {
    auto result = skiplist->Contain("nonexistent_key", 1000);
    EXPECT_FALSE(result.has_value());
    std::print("不存在的key -> 期望 [空], 实际 [空] ✓\n");
  }
  // 测试3: 相同事务ID的多个插入（后插入的覆盖前面的）
  {
    const std::string key = "same_tx_key";

    EXPECT_TRUE(skiplist->Insert(key, "first", 100));
    EXPECT_TRUE(skiplist->Insert(key, "second", 100));  // 相同txId
    EXPECT_TRUE(skiplist->Insert(key, "third", 100));   // 相同txId

    // 查询txId=100应该看到最后一个插入的值
    auto result = skiplist->Contain(key, 100);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "third");
    std::print("相同txId多次插入 -> 期望 third, 实际 {} ✓\n", result.value());

    // 查询txId=99应该看不到
    result = skiplist->Contain(key, 99);
    EXPECT_FALSE(result.has_value());
    std::print("相同txId多次插入(txId=99) -> 期望 [空], 实际 [空] ✓\n");
  }

  // 测试4: 仅删除标记的key
  {
    const std::string key = "only_deleted_key";

    EXPECT_TRUE(skiplist->Insert(key, "", 100));  // 只有删除标记

    // 任何大于等于100的事务ID都应该看到空
    auto result = skiplist->Contain(key, 100);
    EXPECT_FALSE(result.has_value());
    std::print("只有删除标记(txId=100) -> 期望 [空], 实际 [空] ✓\n");

    result = skiplist->Contain(key, 200);
    EXPECT_FALSE(result.has_value());
    std::print("只有删除标记(txId=200) -> 期望 [空], 实际 [空] ✓\n");

    // 小于100的事务ID应该看不到
    result = skiplist->Contain(key, 99);
    EXPECT_FALSE(result.has_value());
    std::print("只有删除标记(txId=99) -> 期望 [空], 实际 [空] ✓\n");
  }

  // 测试5: 事务ID为0的情况
  {
    const std::string key = "zero_tx_key";

    EXPECT_TRUE(skiplist->Insert(key, "value_at_zero", 0));

    auto result = skiplist->Contain(key, 0);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_at_zero");
    std::print("txId=0插入并查询 -> 期望 value_at_zero, 实际 {} ✓\n", result.value());

    result = skiplist->Contain(key, 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "value_at_zero");
    std::print("txId=1查询 -> 期望 value_at_zero, 实际 {} ✓\n", result.value());
  }

  std::print("=== 测试5完成 ===\n\n");
}

// ==================== 主函数 ====================
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
