#include "pomai_cache/engine.hpp"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>

using namespace pomai_cache;

int main() {
  const std::vector<std::string> policies = {"lru", "lfu", "pomai_cost"};
  const std::vector<std::string> presets = {"hotset", "uniform", "writeheavy", "mixed"};

  for (const auto& preset : presets) {
    std::cout << "workload=" << preset << "\n";
    for (const auto& pname : policies) {
      Engine engine({8 * 1024 * 1024, 256, 4 * 1024, 256}, make_policy_by_name(pname));
      std::mt19937_64 rng(42);
      std::uniform_int_distribution<int> u(0, 999);
      const int ops = 10000;
      auto start = std::chrono::steady_clock::now();
      int hits = 0;
      std::vector<double> lat;
      lat.reserve(ops);
      for (int i = 0; i < ops; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        int k = u(rng);
        if (preset == "hotset") k = static_cast<int>(std::pow((u(rng) % 100) + 1, 1.4));
        std::string key = "k" + std::to_string(k % 1000);
        const bool do_write = preset == "writeheavy" ? (i % 2 == 0) : (i % 5 == 0);
        if (do_write) {
          std::vector<std::uint8_t> v(64, static_cast<std::uint8_t>(i % 255));
          engine.set(key, v, std::nullopt, "default");
        } else {
          if (engine.get(key).has_value()) ++hits;
        }
        auto t1 = std::chrono::steady_clock::now();
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
      }
      auto end = std::chrono::steady_clock::now();
      std::sort(lat.begin(), lat.end());
      auto pct = [&](double p) { return lat[static_cast<std::size_t>(p * (lat.size() - 1))]; };
      double seconds = std::chrono::duration<double>(end - start).count();
      std::cout << "policy=" << pname
                << " ops/s=" << std::fixed << std::setprecision(2) << (ops / seconds)
                << " p50_us=" << pct(0.50)
                << " p95_us=" << pct(0.95)
                << " p99_us=" << pct(0.99)
                << " hit_rate=" << (static_cast<double>(hits) / ops)
                << " memory_used=" << engine.memory_used()
                << "\n";
    }
  }
  return 0;
}
