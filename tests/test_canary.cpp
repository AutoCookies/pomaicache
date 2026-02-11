#include "catch2/catch_test_macros.hpp"
#include "pomai_cache/engine.hpp"

TEST_CASE("canary config exposes info fields", "[canary]") {
  pomai_cache::Engine e({1024 * 1024, 256, 1024, 32}, pomai_cache::make_policy_by_name("pomai_cost"));
  e.set_canary_pct(10);
  std::string info = e.info();
  REQUIRE(info.find("canary_pct:10") != std::string::npos);
}
