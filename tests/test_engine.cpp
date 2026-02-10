#include "pomai_cache/engine.hpp"

#include <cassert>
#include <chrono>
#include <fstream>
#include <thread>

using namespace pomai_cache;

int main() {
  {
    Engine e({1024, 256, 1024, 16}, make_policy_by_name("lru"));
    e.set("a", std::vector<std::uint8_t>{'1'}, 1, "default");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    e.tick();
    assert(!e.get("a").has_value());
  }

  {
    Engine e({64, 256, 1024, 16}, make_policy_by_name("lru"));
    e.set("a", std::vector<std::uint8_t>(40, 1), std::nullopt, "default");
    e.set("b", std::vector<std::uint8_t>(40, 2), std::nullopt, "default");
    assert(e.memory_used() <= 64);
  }

  {
    Engine e({1024, 256, 1024, 16}, make_policy_by_name("lfu"));
    e.set("x", std::vector<std::uint8_t>{'x'}, std::nullopt, "default");
    auto i1 = e.info();
    auto i2 = e.info();
    assert(i1 == i2);
  }

  {
    const char* path = "tests/policy_params.json";
    std::ofstream out(path);
    out << R"({"weights":{"w_mem":-9999},"thresholds":{"evict_pressure":99},"guardrails":{"max_admissions_per_second":0},"version":"v-test"})";
    out.close();

    Engine e({1024, 256, 1024, 16}, make_policy_by_name("pomai_cost"));
    std::string err;
    e.reload_params(path, &err);
    const auto& p = e.policy().params();
    assert(p.w_mem >= 0.0);
    assert(p.evict_pressure <= 1.0);
    assert(p.max_admissions_per_second >= 1);
  }

  return 0;
}
