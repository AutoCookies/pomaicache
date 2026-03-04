#include <catch2/catch_test_macros.hpp>

#include "pomaicache.h"
#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"

#include <optional>
#include <string>
#include <vector>

TEST_CASE("integration: embedded core operations", "[integration]") {
  pomaicache::Config cfg;
  cfg.memory_limit_bytes = 16 * 1024 * 1024;
  cfg.data_dir = "./data_integration_core";

  pomaicache::PomaiCache cache(cfg);

  const std::string key = "a";
  const std::string value = "1";
  std::span<const std::byte> v{
      reinterpret_cast<const std::byte *>(value.data()), value.size()};

  REQUIRE(cache.Set(key, v, pomaicache::Ttl{0}));

  auto got = cache.Get(key);
  REQUIRE(got.has_value());
  std::string roundtrip(reinterpret_cast<const char *>(got->data()),
                        got->size());
  CHECK(roundtrip == value);

  // Overwrite and read again to ensure basic churn works.
  const std::string value2 = "2";
  std::span<const std::byte> v2{
      reinterpret_cast<const std::byte *>(value2.data()), value2.size()};
  REQUIRE(cache.Set(key, v2, pomaicache::Ttl{0}));
  auto got2 = cache.Get(key);
  REQUIRE(got2.has_value());
  std::string roundtrip2(reinterpret_cast<const char *>(got2->data()),
                         got2->size());
  CHECK(roundtrip2 == value2);
}

TEST_CASE("integration: embedded churn under load", "[integration][adversarial]") {
  pomaicache::Config cfg;
  cfg.memory_limit_bytes = 16 * 1024 * 1024;
  cfg.data_dir = "./data_integration_churn";

  pomaicache::PomaiCache cache(cfg);

  // Insert a bunch of small keys to exercise caps/churn behavior.
  for (int i = 0; i < 1000; ++i) {
    std::string key = "churn" + std::to_string(i);
    std::string value = "val" + std::to_string(i);
    std::span<const std::byte> v{
        reinterpret_cast<const std::byte *>(value.data()), value.size()};
    REQUIRE(cache.Set(key, v, pomaicache::Ttl{0}));
  }

  // Spot-check a few keys.
  for (int i = 0; i < 10; ++i) {
    std::string key = "churn" + std::to_string(i * 10);
    auto got = cache.Get(key);
    REQUIRE(got.has_value());
  }
}

TEST_CASE("integration: embedded AI artifact commands", "[integration][ai]") {
  pomai_cache::EngineConfig cfg;
  cfg.memory_limit_bytes = 16 * 1024 * 1024;
  cfg.data_dir = "./data_integration_ai";

  auto policy = pomai_cache::make_policy_by_name("pomai_cost");
  pomai_cache::Engine engine(cfg, std::move(policy));
  pomai_cache::AiArtifactCache ai(engine);

  const std::string key = "emb:m:h:3:float";
  const std::string type = "embedding";
  const std::string payload_str = "abc";
  std::vector<std::uint8_t> payload(payload_str.begin(), payload_str.end());

  std::string meta =
      R"({"artifact_type":"embedding","owner":"vector","schema_version":"v1","model_id":"m","snapshot_epoch":"ep9"})";

  std::string err;
  REQUIRE(ai.put(type, key, meta, payload, &err));

  auto got = ai.get(key);
  REQUIRE(got.has_value());
  std::string body(got->payload.begin(), got->payload.end());
  CHECK(body == payload_str);

  auto stats = ai.stats();
  CHECK(stats.find("dedup_hits") != std::string::npos);

  auto removed = ai.invalidate_epoch("ep9");
  CHECK(removed >= 1);

  auto miss = ai.get(key);
  CHECK_FALSE(miss.has_value());
}
