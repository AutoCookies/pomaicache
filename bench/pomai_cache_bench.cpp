#include "pomai_cache/engine.hpp"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>

using namespace pomai_cache;

int main() {
  const std::vector<std::string> policies = {"lru", "lfu", "pomai_cost"};
  const std::vector<std::string> presets = {"hotset", "uniform", "writeheavy",
                                            "mixed"};
  constexpr std::uint64_t seed = 424242;
  std::cout << "seed=" << seed << "\n";

  std::cout << "|workload|policy|ops/s|hit_rate|evictions|\n";
  std::cout << "|---|---:|---:|---:|---:|\n";

  for (const auto &preset : presets) {
    for (const auto &pname : policies) {
      Engine engine({8 * 1024 * 1024, 256, 4 * 1024, 256},
                    make_policy_by_name(pname));
      std::mt19937_64 rng(seed);
      std::uniform_int_distribution<int> u(0, 999);
      const int ops = 30000;
      auto start = std::chrono::steady_clock::now();
      int gets = 0;
      int hits = 0;
      for (int i = 0; i < ops; ++i) {
        int k = u(rng);
        if (preset == "hotset")
          k = static_cast<int>(std::pow((u(rng) % 100) + 1, 1.4));
        std::string key = "k" + std::to_string(k % 1000);
        const bool do_write =
            preset == "writeheavy" ? (i % 2 == 0) : (i % 5 == 0);
        if (do_write) {
          std::vector<std::uint8_t> v(64, static_cast<std::uint8_t>(i % 255));
          engine.set(key, v, std::nullopt, "default");
        } else {
          ++gets;
          if (engine.get(key).has_value())
            ++hits;
        }
      }
      auto end = std::chrono::steady_clock::now();
      double seconds = std::chrono::duration<double>(end - start).count();
      const double hit_rate =
          gets > 0 ? static_cast<double>(hits) / static_cast<double>(gets)
                   : 0.0;
      std::cout << "|" << preset << "|" << pname << "|" << std::fixed
                << std::setprecision(2) << (ops / seconds) << "|" << hit_rate
                << "|" << engine.stats().evictions << "|\n";
    }
  }
  return 0;
}
