#include <filesystem>
#include <format>
#include <print>
#include <string>
#include <vector>

#include "LSM.h"

static void section(const std::string& title) {
  std::println("\n══════════════════════════════════════");
  std::println("  {}", title);
  std::println("══════════════════════════════════════");
}

int main() {
  const std::string db_path = "./demo_db";

  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  auto lsm = std::make_shared<LSM>(db_path);

  // ──────────────────────────────────────────
  // 1. 基本读写
  // ──────────────────────────────────────────
  section("1. 基本读写 put / get");

  lsm->put("name", "Alice");
  lsm->put("city", "Tokyo");
  lsm->put("version", "1.0.0");

  for (const auto& key : {"name", "city", "version", "missing"}) {
    auto val = lsm->get(key);
    if (val) {
      std::println("  get({:10}) => \"{}\"", key, *val);
    } else {
      std::println("  get({:10}) => (not found)", key);
    }
  }

  // ──────────────────────────────────────────
  // 2. 覆盖写 & 删除
  // ──────────────────────────────────────────
  section("2. 覆盖写 & 删除 remove");

  lsm->put("version", "2.0.0");
  std::println("  overwrite version => \"{}\"", lsm->get("version").value_or("(not found)"));

  lsm->remove("city");
  std::println("  after remove city => \"{}\"", lsm->get("city").value_or("(not found)"));

  // ──────────────────────────────────────────
  // 3. 批量写入 & 批量查询
  // ──────────────────────────────────────────
  section("3. 批量读写 put_batch / get_batch");

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 5; ++i) {
    kvs.push_back({std::format("user:{:03d}", i), std::format("name_{}", i)});
  }
  lsm->put_batch(kvs);

  auto batch_results = lsm->get_batch({"user:000", "user:002", "user:004", "user:999"});
  for (const auto& [key, val] : batch_results) {
    std::println("  get_batch({:12}) => {}", key,
                 val ? std::format("\"{}\"", *val) : "(not found)");
  }

  // ──────────────────────────────────────────
  // 4. 前缀查询
  // ──────────────────────────────────────────
  section("4. 前缀查询 get_prefix_range");

  lsm->put("order:001", "item_A");
  lsm->put("order:002", "item_B");
  lsm->put("order:003", "item_C");

  for (const auto& prefix : {"user:", "order:"}) {
    std::println("  prefix \"{}\" matches:", prefix);
    for (const auto& [key, val, tranc_id] : lsm->get_prefix_range(prefix)) {
      std::println("    {} => \"{}\"", key, val);
    }
  }

  // ──────────────────────────────────────────
  // 5. flush 持久化
  // ──────────────────────────────────────────
  section("5. 手动刷盘 flush");

  lsm->put("persist_key", "I will survive flush");
  lsm->flush();
  std::println("  after flush => \"{}\"", lsm->get("persist_key").value_or("(not found)"));

  // ──────────────────────────────────────────
  // 6. 大量写入，触发自动压缩
  // ──────────────────────────────────────────
  section("6. 大量写入 + 压缩验证");

  const int N = 2000;
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("bulk:{:06d}", i), std::format("val_{:06d}", i));
    if ((i + 1) % 400 == 0) {
      lsm->flush();
      std::println("  flushed at i={}", i + 1);
    }
  }
  lsm->flush_all();

  bool all_ok = true;
  for (int i = 0; i < N; i += 200) {
    auto val = lsm->get(std::format("bulk:{:06d}", i));
    if (!val || *val != std::format("val_{:06d}", i)) {
      std::println("  [FAIL] bulk:{:06d} mismatch", i);
      all_ok = false;
    }
  }
  if (all_ok) {
    std::println("  All sampled keys verified OK after compaction.");
  }

  std::println("\nDemo finished.");
  return 0;
}