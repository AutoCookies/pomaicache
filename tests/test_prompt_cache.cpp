#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/prompt_cache.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <thread>

using namespace pomai_cache;

TEST_CASE("PromptCacheManager prefix matching and reuse", "[prompt_cache]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);
  PromptCacheConfig cfg;
  cfg.enabled = true;
  cfg.default_ttl_ms = 60'000;
  cfg.prefix_min_tokens = 2;
  PromptCacheManager pcm(e, ai, cfg);

  std::vector<std::uint8_t> prefix{'h', 'e', 'l', 'l', 'o'};
  REQUIRE(pcm.put_prefix("tok", "pfx1", prefix, 5));

  std::vector<std::uint8_t> full{'h', 'e', 'l', 'l', 'o', ' ', 'x'};
  auto reuse = pcm.reuse_for_query("tok", "full1", full);
  CHECK(reuse.hit);
  CHECK(reuse.cached_tokens == 5);
  CHECK(reuse.suffix_tokens >= 1);
  CHECK(reuse.savings_ratio > 0.0);
}

TEST_CASE("PromptCacheManager TTL expiration", "[prompt_cache][ttl]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);
  PromptCacheConfig cfg;
  cfg.enabled = true;
  cfg.default_ttl_ms = 10;
  cfg.prefix_min_tokens = 1;
  PromptCacheManager pcm(e, ai, cfg);

  std::vector<std::uint8_t> prefix{'a', 'b', 'c'};
  REQUIRE(pcm.put_prefix("tok", "short", prefix, 3));

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  pcm.tick();

  std::vector<std::uint8_t> full{'a', 'b', 'c', 'x'};
  auto reuse = pcm.reuse_for_query("tok", "full", full);
  CHECK_FALSE(reuse.hit);
}

