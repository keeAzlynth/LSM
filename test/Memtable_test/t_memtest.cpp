#include "../../include/memtable.h"
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>
#include <format>

class MemtableTest : public ::testing::Test {
protected:
    void SetUp() override {
        memtable = std::make_shared<MemTable>();
    }
    std::shared_ptr<MemTable> memtable;
};

// 基本的 put/get 操作测试
TEST_F(MemtableTest, BasicPutGet) {
    memtable->put("key1", "value1");
    auto result = memtable->get("key1");
    EXPECT_EQ(result.value().first, "value1");
}

// 测试带锁的 put/get 操作
TEST_F(MemtableTest, MutexPutGet) {
    memtable->put_mutex("key1", "value1");
    std::vector<std::string> values;
    auto it = memtable->get_mutex("key1", values);
    values.emplace_back(it.getValue().second);
    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values[0], "value1");
}


TEST_F(MemtableTest, BatchOperations) {
    std::vector<std::pair<std::string,std::string>> batch_data = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
    };
    
    // 测试批量插入
    memtable->put_batch(batch_data);
    
    // 测试批量获取
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    auto result=memtable->get_batch(keys);
    EXPECT_EQ(result.size(), 3);
}

// 删除操作测试
TEST_F(MemtableTest, RemoveOperations) {
    // 普通删除
    memtable->put("key1", "value1");
    memtable->remove("key1",1);
    EXPECT_FALSE(memtable->get("key1").has_value());
    
    // 带锁的删除
    memtable->put("key2", "value2");
    memtable->remove_mutex("key2",1);
    EXPECT_FALSE(memtable->get("key2").has_value());
    
    // 批量删除
    std::vector<std::pair<std::string, std::string>> batch_data = {
        {"key3", "value3"},
        {"key4", "value4"}
    };  
    memtable->put_batch(batch_data);
    std::vector<std::string> keys_to_remove = {"key3", "key4"};
    memtable->remove_batch(keys_to_remove);
    EXPECT_FALSE(memtable->get("key3").has_value());
    EXPECT_FALSE(memtable->get("key4").has_value());
}

TEST_F(MemtableTest, TransactionIdTest) {
    memtable->put("key1", "value1", 1);
    memtable->put("key1", "value2", 2);
    
    std::vector<std::string> values;
    auto it = memtable->cur_get("key1", 1);
    values.push_back(it.getValue().second);
    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values[0], "");
}

// 表冻结和刷新测试
TEST_F(MemtableTest, FrozenAndFlush) {
    memtable->put("key1", "value1");
    size_t initial_size = memtable->get_cur_size();
    
    // 测试冻结当前表
    memtable->frozen_cur_table();
    EXPECT_EQ(memtable->get_cur_size(), 0);
    EXPECT_GT(memtable->get_fixed_size(), 0);
}

// 并发测试
TEST_F(MemtableTest, ConcurrentOperations) {
    std::vector<std::thread> threads;
    
    // 创建多个线程进行并发操作
    for(int i = 0; i < 10; i++) {
        threads.emplace_back([this, i]() {
            memtable->put_mutex("key" + std::to_string(i),                              "value" + std::to_string(i));
        });
    }
    
    // 等待所有线程完成
    for(auto& thread : threads) {
        thread.join();
    }
    
    // 验证所有数据都正确写入
    for(int i = 0; i < 10; i++) {
        auto result = memtable->get("key" + std::to_string(i));
        EXPECT_EQ(result.value().first, "value" + std::to_string(i));
    }
}

// 范围查询测试
TEST_F(MemtableTest, RangeSearchTest) {
    const int NUM_RECORDS = 10;  // 插入 1000 条数据
    const std::string PREFIX = "key";

    // 插入测试数据
    for (int i = 0; i < NUM_RECORDS; ++i) {
        std::string key = PREFIX + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        memtable->put(key, value);
    }

    // 插入一些不相关的数据
    memtable->put("other1", "other_value1");
    memtable->put("prefix1", "prefix_value1");

    // 测试范围查询
    auto range_iter = memtable->prefix_serach(PREFIX);
    std::vector<std::pair<std::string, std::string>> range_results;

    while (range_iter.valid()) {
        range_results.emplace_back(range_iter.getValue());
        ++range_iter;
    }

    // 验证范围内的元素数量
    EXPECT_EQ(range_results.size(), NUM_RECORDS);  // 应该有 NUM_RECORDS 个以 "key" 开头的键

    // 验证范围内的元素内容
    for (int i = 0; i < NUM_RECORDS; ++i) {
        EXPECT_EQ(range_results[i].first, PREFIX + std::to_string(i));
        EXPECT_EQ(range_results[i].second, "value" + std::to_string(i));
    }

    // 测试不存在的前缀
    auto empty_iter = memtable->prefix_serach("nonexistent");
    EXPECT_FALSE(empty_iter.valid());

    // 测试边界情况
    auto single_iter = memtable->prefix_serach("key0");
    EXPECT_TRUE(single_iter.valid());
    EXPECT_EQ(single_iter.getValue().first, "key0");
    EXPECT_EQ(single_iter.getValue().second, "value0");
}


// 单性能测试
TEST_F(MemtableTest, PerformanceAndMemoryUsageTest) {
    const int NUM_RECORDS = 100000;  // 测试 10 万条数据
    const std::string PREFIX = "key";

    // 插入测试数据
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_RECORDS; ++i) {
        std::string key = PREFIX + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        memtable->put(key, value);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("插入 {} 条记录耗时: {} ms", NUM_RECORDS, insert_duration.count()) << std::endl;

    // 测试查询性能
    start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_RECORDS; ++i) {
        std::string key = PREFIX + std::to_string(i);
        auto result = memtable->get(key);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value().first, "value" + std::to_string(i));
    }
    end_time = std::chrono::high_resolution_clock::now();
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("查询 {} 条记录耗时: {} ms", NUM_RECORDS, query_duration.count()) << std::endl;

    // 测试删除性能
    start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_RECORDS; ++i) {
        std::string key = PREFIX + std::to_string(i);
        memtable->remove(key);
    }
    end_time = std::chrono::high_resolution_clock::now();
    auto delete_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("删除 {} 条记录耗时: {} ms", NUM_RECORDS, delete_duration.count()) << std::endl;

    // 测试内存使用
    size_t cur_size = memtable->get_cur_size();
    size_t fixed_size = memtable->get_fixed_size();
    size_t total_size = memtable->get_total_size();
    std::cout << std::format("当前表内存大小: {} bytes", cur_size) << std::endl;
    std::cout << std::format("固定表内存大小: {} bytes", fixed_size) << std::endl;
    std::cout << std::format("总内存大小: {} bytes", total_size) << std::endl;

}

// 并发性能测试
TEST_F(MemtableTest, ConcurrentPerformanceAndMemoryUsageTest) {
    const int NUM_RECORDS = 50000;  // 测试 10 万条数据
    const int NUM_THREADS = 10;     // 使用 10 个线程
    const std::string PREFIX = "key";

    // 插入测试数据（多线程）
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([this, t, NUM_THREADS, NUM_RECORDS, &PREFIX]() {
            for (int i = t * (NUM_RECORDS / NUM_THREADS); i < (t + 1) * (NUM_RECORDS / NUM_THREADS); ++i) {
                std::string key = PREFIX + std::to_string(i);
                std::string value = "value" + std::to_string(i);
                memtable->put_mutex(key, value);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("多线程插入 {} 条记录耗时: {} ms", NUM_RECORDS, insert_duration.count()) << std::endl;

    // 查询测试数据（多线程）
    start_time = std::chrono::high_resolution_clock::now();
    threads.clear();
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([this, t, NUM_THREADS, NUM_RECORDS, &PREFIX]() {
            for (int i = t * (NUM_RECORDS / NUM_THREADS); i < (t + 1) * (NUM_RECORDS / NUM_THREADS); ++i) {
                std::string key = PREFIX + std::to_string(i);
                auto result = memtable->get(key);
                EXPECT_TRUE(result.has_value());
                EXPECT_EQ(result.value().first, "value" + std::to_string(i));
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    end_time = std::chrono::high_resolution_clock::now();
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("多线程查询 {} 条记录耗时: {} ms", NUM_RECORDS, query_duration.count()) << std::endl;

    // 删除测试数据（多线程）
    start_time = std::chrono::high_resolution_clock::now();
    threads.clear();
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([this, t, NUM_THREADS, NUM_RECORDS, &PREFIX]() {
            for (int i = t * (NUM_RECORDS / NUM_THREADS); i < (t + 1) * (NUM_RECORDS / NUM_THREADS); ++i) {
                std::string key = PREFIX + std::to_string(i);
                memtable->remove(key);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    end_time = std::chrono::high_resolution_clock::now();
    auto delete_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << std::format("多线程删除 {} 条记录耗时: {} ms", NUM_RECORDS, delete_duration.count()) << std::endl;

    // 测试内存使用
    size_t cur_size = memtable->get_cur_size();
    size_t fixed_size = memtable->get_fixed_size();
    size_t total_size = memtable->get_total_size();
    std::cout << std::format("当前表内存大小: {} bytes", cur_size) << std::endl;
    std::cout << std::format("固定表内存大小: {} bytes", fixed_size) << std::endl;
    std::cout << std::format("总内存大小: {} bytes", total_size) << std::endl;

}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}