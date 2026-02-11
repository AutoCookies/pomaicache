#include "catch2/catch_test_macros.hpp"
#include "pomai_cache/engine.hpp"

#include <random>

TEST_CASE("chaos churn does not crash", "[chaos]") {
  pomai_cache::Engine e({1024 * 1024, 256, 1024, 32}, pomai_cache::make_policy_by_name("pomai_cost"));
  std::mt19937_64 rng(42);
  for (int i = 0; i < 20000; ++i) {
    const auto key = std::string("k") + std::to_string(rng() % 2000);
    if (rng() % 4 == 0) {
      std::vector<std::uint8_t> v(static_cast<std::size_t>(rng() % 128 + 1), 'a');
      e.set(key, v, std::nullopt, "default");
    } else if (rng() % 4 == 1) {
      e.get(key);
    } else if (rng() % 4 == 2) {
      e.expire(key, 1);
    } else {
      e.del({key});
    }
    e.tick();
  }
  REQUIRE(e.memory_used() <= 1024 * 1024);
}
