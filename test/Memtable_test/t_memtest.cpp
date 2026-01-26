#include "../../include/memtable.h"
#include <gtest/gtest.h>
#include <print>
#include <string>
#include <vector>
#include <format>
#include <chrono>
#include <memory>

class MemtableTest : public ::testing::Test {
protected:
    void SetUp() override {
        memtable = std::make_shared<MemTable>();
    }
    
    std::shared_ptr<MemTable> memtable;
    
    // 预先生成测试键值对，避免测试中的构造开销
    std::vector<std::pair<std::string, std::string>> GenerateTestData(int count, std::string_view prefix = "key") {
        std::vector<std::pair<std::string, std::string>> data;
        data.reserve(count);
        
        for (int i = 0; i < count; ++i) {
            // 使用format一次性构造，避免多次分配
            data.emplace_back(
                std::format("{}{}", prefix, i),
                std::format("value{}", i)
            );
        }
        return data;
    }
    
    // 生成带固定格式的测试数据（用于范围查询测试）
    std::vector<std::pair<std::string, std::string>> GenerateFormattedTestData(
        int count, std::string_view prefix = "key", std::string_view value_prefix = "value") {
        std::vector<std::pair<std::string, std::string>> data;
        data.reserve(count);
        
        for (int i = 0; i < count; ++i) {
            data.emplace_back(
                std::format("{}_{:03d}", prefix, i),
                std::format("{}_{:03d}", value_prefix, i)
            );
        }
        return data;
    }
};

// 基本的 put/get 操作测试
TEST_F(MemtableTest, BasicPutGet) {
    memtable->put("key1", "value1");
    auto result = memtable->get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().first, "value1");
}

// 测试带锁的 put/get 操作
TEST_F(MemtableTest, MutexPutGet) {
    memtable->put_mutex("key1", "value1");
    std::vector<std::string> values;
    auto it = memtable->get_mutex("key1", values);
    EXPECT_TRUE(it.valid());
    values.emplace_back(it.getValue().second);
    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values[0], "value1");
}

TEST_F(MemtableTest, BatchOperations) {
    constexpr int batch_size = 100;
    auto batch_data = GenerateTestData(batch_size);
    
    // 测试批量插入
    memtable->put_batch(batch_data);
    
    // 验证插入数量
    std::vector<std::string> keys;
    keys.reserve(batch_size);
    for (const auto& [key, _] : batch_data) {
        keys.push_back(key);
    }
    
    auto result = memtable->get_batch(keys);
    EXPECT_EQ(result.size(), batch_size);
    
    // 验证数据正确性 - 根据你的get_batch返回类型修正
    for (size_t i = 0; i < batch_size; ++i) {
        const auto& tuple_result = result[i];
        const std::string& key = std::get<0>(tuple_result);
        const auto& opt_value = std::get<1>(tuple_result);
        const auto& opt_txid = std::get<2>(tuple_result);
        
        EXPECT_EQ(key, std::format("key{}", i));
        EXPECT_TRUE(opt_value.has_value());
        if (opt_value.has_value()) {
            EXPECT_EQ(opt_value.value(), std::format("value{}", i));
        }
    }
}

// 删除操作测试
TEST_F(MemtableTest, RemoveOperations) {
    // 测试数据准备
    constexpr int test_count = 100;
    auto test_data = GenerateTestData(test_count);
    
    // 插入数据
    for (const auto& [key, value] : test_data) {
        memtable->put(key, value);
    }
    
    // 删除前50个键
    for (int i = 0; i < 50; ++i) {
        memtable->remove(std::format("key{}", i));
    }
    
    // 验证删除结果
    for (int i = 0; i < test_count; ++i) {
        auto key = std::format("key{}", i);
        auto result = memtable->get(key);
        if (i < 50) {
            // 已删除的键应返回空值
            EXPECT_TRUE(result.has_value());
            EXPECT_TRUE(result.value().first.empty());
        } else {
            // 未删除的键应有值
            EXPECT_TRUE(result.has_value());
            EXPECT_EQ(result.value().first, std::format("value{}", i));
        }
    }
}

TEST_F(MemtableTest, TransactionIdTest) {
    // 测试不同事务ID的数据版本
    constexpr int txid1 = 100;
    constexpr int txid2 = 200;
    
    memtable->put("shared_key", "value1", txid1);
    memtable->put("shared_key", "value2", txid2);
    
    // 测试按事务ID获取
    std::vector<std::string> values;
    auto it1 = memtable->cur_get("shared_key", txid1);
    EXPECT_TRUE(it1.valid());
    values.push_back(it1.getValue().second);
    EXPECT_EQ(values.back(), "value1");

    auto it2 = memtable->cur_get("shared_key", txid2);
    EXPECT_TRUE(it2.valid());
    values.push_back(it2.getValue().second);
    EXPECT_EQ(values.back(), "value2");
}

// 表冻结和刷新测试
TEST_F(MemtableTest, FrozenAndFlush) {
    constexpr int test_count = 1000;
    auto test_data = GenerateTestData(test_count);
    
    // 插入数据
    size_t total_size = 0;
    for (const auto& [key, value] : test_data) {
        memtable->put(key, value);
        total_size += key.size() + value.size();
    }
    
    size_t initial_size = memtable->get_cur_size();
    EXPECT_GT(initial_size, 0);
    
    // 冻结表
    memtable->frozen_cur_table();
    
    // 验证冻结结果
    EXPECT_EQ(memtable->get_cur_size(), 0);
    EXPECT_GT(memtable->get_fixed_size(), 0);
    EXPECT_GE(memtable->get_fixed_size(), total_size);
}

// 范围查询测试 - 使用精确的范围验证
TEST_F(MemtableTest, RangeSearchTest) {
    constexpr int num_records = 100;
    constexpr std::string_view prefix = "test_key";
    
    // 使用修正后的方法生成测试数据
    auto test_data = GenerateFormattedTestData(num_records, prefix, "value");
    
    // 插入测试数据
    for (const auto& [key, value] : test_data) {
        memtable->put(key, value);
    }
    
    // 执行范围查询
    auto range_iter = memtable->prefix_serach(std::string(prefix) + "_");
    std::vector<std::pair<std::string, std::string>> results;
    int num=100;
    while (range_iter.valid()&&num) {
        results.push_back(range_iter.getValue());
        ++range_iter;
        --num;
    }
    
    // 验证结果数量
    EXPECT_EQ(results.size(), num_records);
    
    // 验证每个结果 - 使用test_data作为预期值，确保一一对应
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& [expected_key, expected_value] = test_data[i];
        const auto& [actual_key, actual_value] = results[i];
        
        EXPECT_EQ(actual_key, expected_key);
        EXPECT_EQ(actual_value, expected_value);
    }
    
    // 测试不存在的前缀
    auto empty_iter = memtable->prefix_serach("nonexistent_prefix");
    EXPECT_FALSE(empty_iter.valid());
}

// 性能测试
TEST_F(MemtableTest, PerformanceAndMemoryUsageTest) {
    constexpr int num_records = 100000;
    constexpr int warmup_records = 1000;
    
    // 预热
    auto warmup_data = GenerateTestData(warmup_records, "warmup_key");
    for (const auto& [key, value] : warmup_data) {
        memtable->put(key, value);
    }
    
    // 生成测试数据
    auto test_data = GenerateTestData(num_records);
    
    // 插入性能测试
    auto insert_start = std::chrono::high_resolution_clock::now();
    for (const auto& [key, value] : test_data) {
        memtable->put(key, value);
    }
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start);
    
    std::print("插入 {} 条记录耗时: {} ms\n", num_records, insert_duration.count());
    
    // 查询性能测试
    auto query_start = std::chrono::high_resolution_clock::now();
    for (const auto& [key, _] : test_data) {
        auto result = memtable->get(key);
        EXPECT_TRUE(result.has_value());
    }
    auto query_end = std::chrono::high_resolution_clock::now();
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);
    
    std::print("查询 {} 条记录耗时: {} ms\n", num_records, query_duration.count());
    
    // 删除性能测试
    auto delete_start = std::chrono::high_resolution_clock::now();
    for (const auto& [key, _] : test_data) {
        memtable->remove(key);
    }
    auto delete_end = std::chrono::high_resolution_clock::now();
    auto delete_duration = std::chrono::duration_cast<std::chrono::milliseconds>(delete_end - delete_start);
    
    std::print("删除 {} 条记录耗时: {} ms\n", num_records, delete_duration.count());
    
    // 内存使用统计
    size_t cur_size = memtable->get_cur_size();
    size_t fixed_size = memtable->get_fixed_size();
    size_t total_size = memtable->get_total_size();
    
    std::print("\n内存使用统计:\n");
    std::print("当前表内存大小: {} bytes\n", cur_size);
    std::print("固定表内存大小: {} bytes\n", fixed_size);
    std::print("总内存大小: {} bytes\n", total_size);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}