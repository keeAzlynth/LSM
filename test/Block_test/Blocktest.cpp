#include "../../include/Block.h"
#include "../../include/BlockIterator.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <iostream>
#include <tuple>

class BlockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 在 SetUp 中初始化，确保 Block 对象完全构造
    block = std::make_shared<Block>(4096);
  }
  void                   TearDown() override { block.reset(); }
  std::shared_ptr<Block> block;
};

// 测试基本操作
TEST_F(BlockTest, BasicOperations) {
  // 测试添加条目
  EXPECT_TRUE(block->add_entry("key1", "value1", 0));
  EXPECT_TRUE(block->add_entry("key2", "value2", 0));

  // 测试获取值
  auto value1 = block->get_value_binary("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");
}

// 测试二分查找
TEST_F(BlockTest, BinarySearch) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);
  block->add_entry("key3", "value3", 0);

  auto idx = block->get_idx_binary("key2");
  EXPECT_TRUE(idx.has_value());
}

// 测试编码解码
TEST_F(BlockTest, EncodeAndDecode) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);

  auto encoded = block->encode();
  auto decoded = block->decode(encoded);

  EXPECT_EQ(decoded->get_value_binary("key1").value(), "value1");
  EXPECT_EQ(decoded->get_value_binary("key2").value(), "value2");
}

// 测试获取首尾键
TEST_F(BlockTest, FirstAndLastKey) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);

  auto keys = block->get_first_and_last_key();
  EXPECT_EQ(keys.first, "key1");
  EXPECT_EQ(keys.second, "key2");
}

// 测试迭代器
TEST_F(BlockTest, Iterator) {
  // 添加调试信息，确认block正确创建
  ASSERT_NE(block, nullptr) << "Block should not be null";

  // 添加多组测试数据
  const std::vector<std::pair<std::string, std::string>> test_data = {
      {"key1", "value1"}, {"key10", "value10"}, {"key2", "value2"}, {"key3", "value3"},
      {"key4", "value4"}, {"key5", "value5"},   {"key6", "value6"}, {"key7", "value7"},
      {"key8", "value8"}, {"key9", "value9"}};

  // 插入测试数据
  for (const auto& [key, value] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, 0)) << "Failed to add entry: " << key;
  }

  try {
    auto it  = block->begin();
    auto end = block->end();
    std::cout << it.getIndex() << std::endl;
    std::cout << end.getIndex() << std::endl;
    // 检查迭代器是否有效
    int                                              count = 0;
    std::vector<std::pair<std::string, std::string>> retrieved_data;

    // 遍历并收集数据
    while (it != end) {
      auto entry = it.getValue();  // 假设getValue返回pair<string,string>
      retrieved_data.push_back(entry);
      count++;
      ++it;
    }

    // 验证数量
    EXPECT_EQ(count, test_data.size()) << "Iterator count mismatch";

    // 验证顺序和内容
    ASSERT_EQ(retrieved_data.size(), test_data.size())
        << "Retrieved data size (" << retrieved_data.size() << ") doesn't match test data size ("
        << test_data.size() << ")";

    // 只有当数量匹配时才验证内容
    if (retrieved_data.size() == test_data.size()) {
      for (size_t i = 0; i < retrieved_data.size(); ++i) {
        EXPECT_EQ(retrieved_data[i].first, test_data[i].first)
            << "Key mismatch at position " << i << "\nExpected: " << test_data[i].first
            << "\nActual: " << retrieved_data[i].first;
        EXPECT_EQ(retrieved_data[i].second, test_data[i].second)
            << "Value mismatch at position " << i << "\nExpected: " << test_data[i].second
            << "\nActual: " << retrieved_data[i].second;
      }
    } else {
      // 如果数量不匹配，打印详细的调试信息
      std::cout << "\nRetrieved data contents:" << std::endl;
      for (const auto& [key, value] : retrieved_data) {
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
      }
    }

  } catch (const std::bad_weak_ptr& e) {
    FAIL() << "Bad weak ptr exception: " << e.what() << "\nBlock address: " << block.get()
           << "\nUse count: " << block.use_count();
  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }

  // 测试迭代器的边界情况
  try {
    // 测试空迭代器
    auto empty_it = block->end();
    EXPECT_EQ(empty_it, block->end());

    // 测试++操作是否正确到达end
    auto it = block->begin();
    for (size_t i = 0; i < test_data.size(); ++i) {
      EXPECT_NE(it, block->end());
      ++it;
    }
    EXPECT_EQ(it, block->end());

  } catch (const std::exception& e) {
    FAIL() << "Exception in boundary test: " << e.what();
  }
}

// 测试大小限制
TEST_F(BlockTest, SizeLimit) {
  std::string large_value(1024, 'x');  // 1KB大小的值
  EXPECT_TRUE(block->add_entry("key1", large_value, 0));
  EXPECT_GT(block->get_cur_size(), 1024);
}

// 测试空块
TEST_F(BlockTest, EmptyBlock) {
  EXPECT_TRUE(block->is_empty());
  EXPECT_EQ(block->get_cur_size(), 2);
}

TEST_F(BlockTest, RangeSearch) {
  // 添加多组测试数据
  const std::vector<std::pair<std::string, std::string>> test_data = {
      {"key1", "value1"}, {"key10", "value10"}, {"key11", "value11"}, {"key12", "value12"},
      {"key4", "value4"}, {"key5", "value5"},   {"key6", "value6"},   {"key7", "value7"},
      {"key8", "value8"}, {"key9", "value9"}};

  // 插入测试数据
  for (const auto& [key, value] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, 0)) << "Failed to add entry: " << key;
  }

  try {
    // 测试范围查询
    auto range_iterators = block->get_prefix_iterator("key1");
    ASSERT_TRUE(range_iterators.has_value()) << "Range iterators should not be null";

    auto begin = range_iterators->first;
    auto end   = range_iterators->second;

    ASSERT_NE(begin, nullptr) << "Begin iterator should not be null";
    ASSERT_NE(end, nullptr) << "End iterator should not be null";

    std::vector<std::pair<std::string, std::string>> retrieved_data;

    while (*begin != *end) {
      auto entry = begin->getValue();
      retrieved_data.push_back(entry);
      ++(*begin);
    }
    auto range_iterators2 = block->get_prefix_iterator("key9");
    ASSERT_TRUE(range_iterators2.has_value()) << "Range iterators should not be null";

    auto begin2 = range_iterators2->first;
    auto end2   = range_iterators2->second;

    std::vector<std::pair<std::string, std::string>> retrieved_data2;
    while (*begin2 != *end2) {
      auto entry = begin2->getValue();
      retrieved_data2.push_back(entry);
      ++(*begin2);
    }

    // 验证范围内的键值对
    const std::vector<std::pair<std::string, std::string>> expected_data = {
        {"key1", "value1"},
        {"key10", "value10"},
        {"key11", "value11"},
        {"key12", "value12"},
    };

    ASSERT_EQ(retrieved_data.size(), expected_data.size()) << "Retrieved data size mismatch";

    for (size_t i = 0; i < expected_data.size(); ++i) {
      EXPECT_EQ(retrieved_data[i].first, expected_data[i].first)
          << "Key mismatch at position " << i;
      EXPECT_EQ(retrieved_data[i].second, expected_data[i].second)
          << "Value mismatch at position " << i;
    }
    const std::vector<std::pair<std::string, std::string>> expected_data2 = {
        {"key9", "value9"},
    };

    ASSERT_EQ(retrieved_data2.size(), expected_data2.size()) << "Retrieved data2 size mismatch";

    for (size_t i = 0; i < expected_data2.size(); ++i) {
      EXPECT_EQ(retrieved_data2[i].first, expected_data2[i].first)
          << "Key mismatch at position " << i;
      EXPECT_EQ(retrieved_data2[i].second, expected_data2[i].second)
          << "Value mismatch at position " << i;
    }

  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }
}

TEST_F(BlockTest, RangeSearchandMVCC) {
  // 添加多组测试数据
  const std::vector<std::tuple<std::string, std::string, uint64_t>> test_data = {
      {"key1", "value1", 100},   {"key10", "value10", 120}, {"key11", "value11", 80},
      {"key12", "value12", 150}, {"key4", "value4", 60},    {"key5", "value5", 92},
      {"key6", "value6", 73},    {"key7", "value7", 110},   {"key8", "value8", 98},
      {"key9", "value9", 90},    {"key99", "value99", 99}};

  // 插入测试数据
  for (const auto& [key, value, tranc_id] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, tranc_id)) << "Failed to add entry: " << key;
  }

  try {
    // 测试范围查询
    auto range_iterators  = block->get_prefix_tran_id("key1", 120);
    auto range_iterators2 = block->get_prefix_tran_id("key9", 90);

    // 验证范围内的键值对
    const std::vector<std::tuple<std::string, std::string, uint64_t>> expected_data = {
        {"key1", "value1", 100},
        {"key10", "value10", 120},
        {"key11", "value11", 80},
    };

    ASSERT_EQ(range_iterators.size(), expected_data.size()) << "Retrieved data size mismatch";

    for (size_t i = 0; i < expected_data.size(); ++i) {
      EXPECT_EQ(std::get<0>(range_iterators[i]), std::get<0>(expected_data[i]))
          << "Key mismatch at position " << i;
      EXPECT_EQ(std::get<1>(range_iterators[i]), std::get<1>(expected_data[i]))
          << "Value mismatch at position " << i;
      EXPECT_EQ(std::get<2>(range_iterators[i]), std::get<2>(expected_data[i]))
          << "trans_id mismatch at position " << i;
    }
    const std::vector<std::tuple<std::string, std::string, uint64_t>> expected_data2 = {
        {"key9", "value9", 90}};

    ASSERT_EQ(range_iterators2.size(), expected_data2.size()) << "Retrieved data2 size mismatch";
    for (size_t i = 0; i < expected_data2.size(); ++i) {
      EXPECT_EQ(std::get<0>(range_iterators2[i]), std::get<0>(expected_data2[i]))
          << "Key mismatch at position " << i;
      EXPECT_EQ(std::get<1>(range_iterators2[i]), std::get<1>(expected_data2[i]))
          << "Value mismatch at position " << i;
      EXPECT_EQ(std::get<2>(range_iterators2[i]), std::get<2>(expected_data2[i]))
          << "trans_id mismatch at position " << i;
    }
  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
