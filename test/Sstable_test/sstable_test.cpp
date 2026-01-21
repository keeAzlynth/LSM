#include "../../include/memtable.h"
#include "../../include/BlockIterator.h"
#include "../../include/Blockcache.h"
#include <gtest/gtest.h>
#include <memory>
#include <filesystem>
#include <print>
#include <string>
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
 auto res= memtable->flushsync();
 for (auto i=res->begin();i!=res->end();++i) {
  auto kv=i.getValue();
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
  auto                                             checks_size = 10;
  std::vector<std::pair<std::string, std::string>> checks;
  checks.reserve(checks_size);
  for (int i = 0; i < checks_size; i++) {
    auto index = Global_::generateRandom(0, num_records - 1);
    checks.push_back({kvs[index].first, kvs[index].second});
    std::print("{}:{}\n", kvs[index].first, kvs[index].second);
  }

  for (const auto& pref : checks) {
    auto results = sst->get_prefix_range(pref.first, 0);
    ASSERT_FALSE(results.empty()) << "Prefix query returned empty for " << pref.first;

    // 检查每个结果确实以 pref 开头
    for (const auto& p : results) {
      ASSERT_TRUE(p.first.starts_with(pref.first))
          << "Key '" << p.first << "' does not start with prefix '" << pref.first << "'";
    }
  }

  // 验证某些具体键存在且值正确
  auto r     = sst->get_prefix_range("k12", 0);
  bool found = false;
  for (auto& p : r) {
    if (p.first.starts_with("k12")) {
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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}