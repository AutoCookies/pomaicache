#include "pomai_cache/engine.hpp"
#include "pomai_cache/bloom_filter.hpp"
#include "pomai_cache/count_min_sketch.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <fstream>
#include <thread>

using namespace pomai_cache;

TEST_CASE("TTL with EX/PX semantics expires and bounded cleanup",
          "[engine][ttl]") {
  Engine e({1024 * 1024, 256, 1024, 8}, make_policy_by_name("lru"));
  REQUIRE(e.set("ex", std::vector<std::uint8_t>{'1'}, 1200, "default"));
  REQUIRE(e.set("px", std::vector<std::uint8_t>{'1'}, 100, "default"));
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  e.tick();
  CHECK_FALSE(e.get("px").has_value());
  CHECK(e.get("ex").has_value());

  for (int i = 0; i < 32; ++i) {
    REQUIRE(e.set("ttl" + std::to_string(i), std::vector<std::uint8_t>{'1'}, 1,
                  "default"));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  const auto before = e.stats().expirations;
  e.tick();
  CHECK((e.stats().expirations - before) <= 8);
}

TEST_CASE("Pomai policy evicts under pressure and owner quota enforced",
          "[engine][eviction]") {
  Engine e({96, 256, 1024, 16}, make_policy_by_name("pomai_cost"));
  REQUIRE(
      e.set("a", std::vector<std::uint8_t>(40, 1), std::nullopt, "default"));
  REQUIRE(
      e.set("b", std::vector<std::uint8_t>(40, 2), std::nullopt, "default"));
  REQUIRE(
      e.set("c", std::vector<std::uint8_t>(40, 3), std::nullopt, "default"));
  CHECK(e.memory_used() <= 96);
  CHECK(e.stats().evictions >= 1);

  const char *path = "policy_params_owner.json";
  std::ofstream out(path);
  out << R"({"owner_cap_bytes":16,"version":"quota-v1"})";
  out.close();
  REQUIRE(e.reload_params(path));
  std::string err;
  CHECK_FALSE(e.set("owner-limited", std::vector<std::uint8_t>(32, 0xFF),
                    std::nullopt, "default", &err));
  CHECK(err.find("owner quota") != std::string::npos);
}

TEST_CASE("INFO and top-k are deterministic and sorted",
          "[engine][determinism]") {
  Engine e({2048, 256, 1024, 16}, make_policy_by_name("lfu"));
  REQUIRE(e.set("x", std::vector<std::uint8_t>{'x'}, std::nullopt, "default"));
  REQUIRE(e.set("y", std::vector<std::uint8_t>{'y'}, std::nullopt, "default"));
  REQUIRE(e.set("z", std::vector<std::uint8_t>{'z'}, std::nullopt, "default"));
  e.get("y");
  e.get("z");
  e.get("z");

  auto i1 = e.info();
  auto i2 = e.info();
  CHECK(i1 == i2);
  CHECK(i1.find("topk_hits:z:2,y:1,x:0") != std::string::npos);
}

TEST_CASE("Param reload clamps and invalid schema rejected atomically",
          "[engine][config]") {
  Engine e({1024, 256, 1024, 16}, make_policy_by_name("pomai_cost"));
  const auto original = e.policy().params();

  const char *good = "policy_params_good.json";
  std::ofstream out(good);
  out << R"({"w_mem":-9999,"evict_pressure":99,"max_admissions_per_second":0,"version":"v-test"})";
  out.close();
  REQUIRE(e.reload_params(good));
  const auto &p = e.policy().params();
  CHECK(p.w_mem >= 0.0);
  CHECK(p.evict_pressure <= 1.0);
  CHECK(p.max_admissions_per_second >= 1);

  const char *bad = "policy_params_bad.json";
  std::ofstream bad_out(bad);
  bad_out << "not-json";
  bad_out.close();
  std::string err;
  CHECK_FALSE(e.reload_params(bad, &err));
  CHECK(e.policy().params().version == p.version);
}

TEST_CASE("Tiered SSD placement and recovery metadata in INFO",
          "[engine][tier]") {
  EngineConfig cfg;
  cfg.memory_limit_bytes = 1024 * 1024;
  cfg.max_key_len = 256;
  cfg.max_value_size = 256 * 1024;
  cfg.ttl_cleanup_per_tick = 16;
  cfg.data_dir = "test_tier_data";
  cfg.tier.ssd_enabled = true;
  cfg.tier.ssd_value_min_bytes = 64;
  cfg.tier.ram_max_bytes = 1024 * 1024;
  cfg.fsync_mode = FsyncMode::Always;

  Engine e(cfg, make_policy_by_name("lru"));
  REQUIRE(e.set("big", std::vector<std::uint8_t>(128, 'b'), std::nullopt,
                "default"));
  CHECK_FALSE(e.get("missing").has_value());
  auto v = e.get("big");
  REQUIRE(v.has_value());
  CHECK(v->size() == 128);

  auto i = e.info();
  CHECK(i.find("ssd_gets:") != std::string::npos);
  CHECK(i.find("ssd_index_rebuild_ms:") != std::string::npos);
}

TEST_CASE("BloomFilter basic add/query semantics", "[bloom]") {
  BloomFilter bf(1000, 0.01);
  CHECK(bf.add("hello"));
  CHECK(bf.add("world"));
  CHECK(bf.maybe_contains("hello"));
  CHECK(bf.maybe_contains("world"));
  CHECK_FALSE(bf.maybe_contains("missing"));

  CHECK_FALSE(bf.add("hello"));

  bf.reset();
  CHECK_FALSE(bf.maybe_contains("hello"));
}

TEST_CASE("BloomFilter false positive rate within bounds", "[bloom]") {
  BloomFilter bf(10000, 0.01);
  for (int i = 0; i < 5000; ++i)
    bf.add("key-" + std::to_string(i));

  int false_positives = 0;
  for (int i = 50000; i < 60000; ++i) {
    if (bf.maybe_contains("miss-" + std::to_string(i)))
      ++false_positives;
  }
  CHECK(false_positives < 300);
}

TEST_CASE("ScalableBloomFilter grows and queries across filters",
          "[bloom][scalable]") {
  ScalableBloomFilter sbf(100, 0.01);
  for (int i = 0; i < 500; ++i) {
    sbf.add("item-" + std::to_string(i));
  }
  for (int i = 0; i < 500; ++i) {
    CHECK(sbf.maybe_contains("item-" + std::to_string(i)));
  }
  CHECK(sbf.memory_bytes() > 0);
}

TEST_CASE("CountMinSketch increment and estimate", "[cms]") {
  CountMinSketch cms(0.001, 0.01);
  cms.increment(42, 5);
  cms.increment(42, 3);
  cms.increment(99, 1);
  CHECK(cms.estimate(42) >= 8);
  CHECK(cms.estimate(99) >= 1);
  CHECK(cms.estimate(1234) == 0);

  cms.reset();
  CHECK(cms.estimate(42) == 0);
}

TEST_CASE("CountMinSketch saturating add does not overflow", "[cms]") {
  CountMinSketch cms(0.001, 0.01);
  for (int i = 0; i < 70000; ++i)
    cms.increment(7);
  auto est = cms.estimate(7);
  CHECK(est == 65535);
}

TEST_CASE("Engine INFO includes frequency sketch and bloom stats",
          "[engine][ds]") {
  Engine e({2048, 256, 1024, 16}, make_policy_by_name("pomai_cost"));
  REQUIRE(e.set("k", std::vector<std::uint8_t>{'v'}, std::nullopt, "default"));
  e.get("k");
  auto info = e.info();
  CHECK(info.find("frequency_sketch_memory_bytes:") != std::string::npos);
  CHECK(info.find("negative_bloom_memory_bytes:") != std::string::npos);
}
