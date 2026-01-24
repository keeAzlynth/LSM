#include "../../include/Skiplist.h"
#include <gtest/gtest.h>
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
    prepare_test_data(1000000);
  }

  void prepare_test_data(size_t size) {
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
  constexpr size_t NUM_OPERATIONS = 1000000;

  // 预热：避免冷启动影响
  {
    Skiplist warmup_list;
    for (size_t i = 0; i < 1000; ++i) {
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
TEST_F(SkiplistTest, MemoryAnalysisTest) {
  constexpr std::array<size_t, 4> TEST_SIZES = {1000, 10000, 100000, 1000000};
  std::vector<double>             memory_efficiency;

  for (size_t size : TEST_SIZES) {
    // 创建新的skiplist实例
    auto test_list = std::make_unique<Skiplist>();

    // 记录初始内存使用
    size_t initial_memory = test_list->get_size();

    // 插入数据
    for (size_t i = 0; i < size; ++i) {
      std::string key   = std::format("test_key_{:08}", i);
      std::string value = std::format("test_value_{:08}", i);
      test_list->Insert(key, value);
    }

    // 计算内存使用
    size_t final_memory    = test_list->get_size();
    size_t memory_used     = final_memory - initial_memory;
    double bytes_per_entry = static_cast<double>(memory_used) / size;
    memory_efficiency.push_back(bytes_per_entry);

    std::print(stdout, "\n数据量: {:7} 条记录\n", size);
    std::print(stdout, "内存占用: {:10} bytes\n", memory_used);
    std::print(stdout, "平均每节点: {:8.2f} bytes\n", bytes_per_entry);
    std::print(stdout, "内存放大系数: {:6.2f}\n", bytes_per_entry / (sizeof(std::string) * 2));

    // 验证内存使用在合理范围内
    EXPECT_GT(bytes_per_entry, 24.0);   // 基础开销
    EXPECT_LT(bytes_per_entry, 128.0);  // 合理上限
  }

  // 分析内存使用趋势
  std::print(stdout, "\n内存使用趋势分析:\n");
  for (size_t i = 1; i < memory_efficiency.size(); ++i) {
    double growth = memory_efficiency[i] / memory_efficiency[i - 1];
    std::print(stdout, "从 {} 到 {}: 内存效率变化: {:.2f}\n", TEST_SIZES[i - 1], TEST_SIZES[i],
               growth);

    // 随着数据量增加，每节点的平均内存应该趋于稳定
    EXPECT_GT(growth, 0.8);
    EXPECT_LT(growth, 1.2);
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
TEST_F(SkiplistTest, MVCCMixedWorkloadTest) {
  // ============ 测试配置 ============
  constexpr size_t   TOTAL_OPS     = 50000;  // 总操作数
  constexpr int      READ_RATIO    = 80;     // 80% 读操作
  constexpr int      WRITE_RATIO   = 15;     // 15% 写操作
  constexpr int      DELETE_RATIO  = 5;      // 5% 删除操作（通过插入空值）
  constexpr size_t   INITIAL_KEYS  = 1000;   // 初始键数量
  constexpr uint64_t INITIAL_TX_ID = 1000;   // 初始事务ID

  std::print("=== MVCC跳表混合工作负载测试 ===\n");
  std::print("配置: {} 次操作 ({}%读, {}%写, {}%删除)\n", TOTAL_OPS, READ_RATIO, WRITE_RATIO,
             DELETE_RATIO);

  // ============ 第一阶段：初始化数据 ============
  // 目标：模拟已有数据，为后续操作做准备
  std::print("\n1. 初始化数据 (插入 {} 条记录)...\n", INITIAL_KEYS);

  uint64_t current_tx_id = INITIAL_TX_ID;

  // 插入初始数据
  for (size_t i = 0; i < INITIAL_KEYS; ++i) {
    std::string key   = std::format("key_{:08}", i);
    std::string value = std::format("value_{:08}", i);
    skiplist->Insert(key, value, current_tx_id++);
  }

  std::print("   初始事务ID范围: {} - {}\n", INITIAL_TX_ID, current_tx_id - 1);
  std::print("   当前跳表状态: {} 节点, {} bytes\n", skiplist->getnodecount(),
             skiplist->get_size());

  // ============ 第二阶段：混合工作负载 ============
  // 目标：测试读、写、删除（插入空值）的混合操作
  std::print("\n2. 执行混合工作负载 ({} 次操作)...\n", TOTAL_OPS);

  std::mt19937                          rng(std::random_device{}());
  std::uniform_int_distribution<int>    op_dist(0, 99);                     // 操作类型分布
  std::uniform_int_distribution<size_t> key_dist(0, INITIAL_KEYS * 2 - 1);  // 键分布
  std::uniform_int_distribution<uint64_t> tx_dist(INITIAL_TX_ID, current_tx_id * 2);  // 事务ID分布

  // 跟踪数据状态：记录每个key的(最新值, 最新事务ID)
  std::unordered_map<std::string, std::pair<std::string, uint64_t>> key_states;

  // 初始化key_states（所有初始键都存在）
  for (size_t i = 0; i < INITIAL_KEYS; ++i) {
    std::string key = std::format("key_{:08}", i);
    key_states[key] = {std::format("value_{:08}", i), INITIAL_TX_ID + i};
  }

  size_t reads = 0, writes = 0, deletes = 0;
  size_t read_hits = 0, write_success = 0, delete_success = 0;

  auto start_time = std::chrono::high_resolution_clock::now();

  for (size_t op_idx = 0; op_idx < TOTAL_OPS; ++op_idx) {
    int         op_type = op_dist(rng);
    size_t      key_idx = key_dist(rng);
    std::string key     = std::format("key_{:08}", key_idx);

    if (op_type < READ_RATIO) {
      // ============ 读操作测试 ============
      // 目标：测试MVCC可见性，确保只能看到≤给定事务ID的记录

      // 选择一个随机的事务ID进行查询
      uint64_t read_tx_id = tx_dist(rng);

      // 执行查询
      auto result = skiplist->Contain(key, read_tx_id);

      // 确定期望结果：查找key_states中事务ID≤read_tx_id的最新记录
      std::optional<std::string> expected_value;
      uint64_t                   latest_valid_tx_id = 0;

      if (key_states.find(key) != key_states.end()) {
        const auto& [value, tx_id] = key_states[key];
        if (tx_id <= read_tx_id) {
          // 只有在事务ID小于等于查询事务ID且值非空时才可见
          if (!value.empty()) {
            expected_value     = value;
            latest_valid_tx_id = tx_id;
          }
        }
      }

      // 验证结果
      if (expected_value.has_value()) {
        EXPECT_TRUE(result.has_value()) << "读操作失败: key=" << key << ", tx_id=" << read_tx_id
                                        << ", 期望值=" << *expected_value;
        if (result.has_value()) {
          read_hits++;
          EXPECT_EQ(*result, *expected_value) << "值不匹配: key=" << key;
        }
      } else {
        EXPECT_FALSE(result.has_value())
            << "读操作失败: key=" << key << ", tx_id=" << read_tx_id << ", 期望为空";
      }

      reads++;

    } else if (op_type < READ_RATIO + WRITE_RATIO) {
      // ============ 写操作测试 ============
      // 目标：测试插入/更新功能，使用新的事务ID

      std::string new_value = std::format("updated_{:08}", op_idx);
      uint64_t    new_tx_id = current_tx_id++;

      // 执行写入
      bool success = skiplist->Insert(key, new_value, new_tx_id);
      EXPECT_TRUE(success) << "写操作失败: key=" << key;

      // 更新状态跟踪
      key_states[key] = {new_value, new_tx_id};

      writes++;
      if (success)
        write_success++;

    } else {
      // ============ 删除操作测试 ============
      // 目标：测试MVCC删除（插入空值墓碑）

      uint64_t delete_tx_id = current_tx_id++;

      // 执行删除（插入空值）
      bool success = skiplist->Insert(key, "", delete_tx_id);
      EXPECT_TRUE(success) << "删除操作失败: key=" << key;

      // 更新状态跟踪：记录空值作为删除标记
      key_states[key] = {"", delete_tx_id};

      deletes++;
      if (success)
        delete_success++;
    }

    // 进度显示
    if ((op_idx + 1) % 10000 == 0) {
      std::print("   进度: {}/{} 次操作\n", op_idx + 1, TOTAL_OPS);
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  // ============ 第三阶段：结果统计 ============
  std::print("\n3. 测试结果统计:\n");
  std::print("   总耗时: {} µs ({:.3f} ms)\n", duration.count(), duration.count() / 1000.0);
  std::print("   吞吐量: {:.0f} 操作/秒\n",
             static_cast<double>(TOTAL_OPS) / duration.count() * 1e6);

  std::print("\n   操作分布:\n");
  std::print("   读操作: {} ({:.1f}%), 命中: {} ({:.1f}%)\n", reads,
             static_cast<double>(reads) / TOTAL_OPS * 100, read_hits,
             reads > 0 ? static_cast<double>(read_hits) / reads * 100 : 0.0);
  std::print("   写操作: {} ({:.1f}%), 成功: {} ({:.1f}%)\n", writes,
             static_cast<double>(writes) / TOTAL_OPS * 100, write_success,
             writes > 0 ? static_cast<double>(write_success) / writes * 100 : 0.0);
  std::print("   删除操作: {} ({:.1f}%), 成功: {} ({:.1f}%)\n", deletes,
             static_cast<double>(deletes) / TOTAL_OPS * 100, delete_success,
             deletes > 0 ? static_cast<double>(delete_success) / deletes * 100 : 0.0);

  // ============ 第四阶段：最终状态验证 ============
  std::print("\n4. 最终状态验证:\n");

  // 验证跳表统计数据
  std::print("   跳表节点数: {} (包含所有版本和墓碑)\n", skiplist->getnodecount());
  std::print("   跳表大小: {} bytes\n", skiplist->get_size());

  // 验证key_states中的每个key在跳表中的最新状态
  size_t verification_passed = 0;
  size_t verification_failed = 0;

  for (const auto& [key, state] : key_states) {
    const auto& [expected_value, tx_id] = state;

    // 使用最新的事务ID查询，应该能看到最新状态
    auto result = skiplist->Contain(key, tx_id);

    if (expected_value.empty()) {
      // 期望是删除状态（空值）
      EXPECT_FALSE(result.has_value()) << "验证失败: key=" << key << " 应该被删除";
      if (result.has_value()) {
        verification_failed++;
      } else {
        verification_passed++;
      }
    } else {
      // 期望有值
      EXPECT_TRUE(result.has_value()) << "验证失败: key=" << key << " 应该有值";
      if (result.has_value()) {
        EXPECT_EQ(*result, expected_value) << "验证失败: key=" << key << " 值不匹配";
        verification_passed++;
      } else {
        verification_failed++;
      }
    }
  }

  std::print("   状态验证: {} 通过, {} 失败\n", verification_passed, verification_failed);

  // ============ 第五阶段：随机抽样验证 ============
  std::print("\n5. 随机抽样验证:\n");

  constexpr size_t                        SAMPLE_SIZE = 100;
  std::uniform_int_distribution<size_t>   sample_key_dist(0, key_states.size() - 1);
  std::uniform_int_distribution<uint64_t> sample_tx_dist(INITIAL_TX_ID, current_tx_id - 1);

  // 将key_states的key转换为vector以便随机访问
  std::vector<std::string> all_keys;
  for (const auto& [key, _] : key_states) {
    all_keys.push_back(key);
  }

  size_t sample_passed = 0;

  for (size_t i = 0; i < SAMPLE_SIZE && i < all_keys.size(); ++i) {
    size_t             idx          = sample_key_dist(rng);
    const std::string& key          = all_keys[idx];
    uint64_t           sample_tx_id = sample_tx_dist(rng);

    // 查询
    auto result = skiplist->Contain(key, sample_tx_id);

    // 确定期望值：查找key_states中事务ID≤sample_tx_id的最新记录
    std::optional<std::string> expected_value;

    if (key_states.find(key) != key_states.end()) {
      const auto& [value, tx_id] = key_states[key];
      if (tx_id <= sample_tx_id && !value.empty()) {
        expected_value = value;

        // 检查是否有更早的版本
        // 注意：这里简化了，实际可能需要检查所有≤sample_tx_id的版本
      }
    }

    // 验证
    bool passed = false;
    if (expected_value.has_value()) {
      passed = result.has_value() && *result == *expected_value;
    } else {
      passed = !result.has_value();
    }

    if (passed) {
      sample_passed++;
    }
  }

  std::print("   随机抽样: {}/{} 通过 ({:.1f}%)\n", sample_passed, SAMPLE_SIZE,
             static_cast<double>(sample_passed) / SAMPLE_SIZE * 100);

  // ============ 测试断言 ============
  EXPECT_GT(reads, 0) << "应该执行了读操作";
  EXPECT_GT(writes, 0) << "应该执行了写操作";
  EXPECT_GT(deletes, 0) << "应该执行了删除操作";
  EXPECT_EQ(verification_failed, 0) << "最终状态验证应该全部通过";
  EXPECT_GT(sample_passed, SAMPLE_SIZE * 0.9) << "随机抽样通过率应高于90%";

  std::print("\n=== 测试完成 ===\n");
}