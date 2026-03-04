#include "pomai_cache/engine.hpp"
#include "pomaicache.h"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace pomai_cache;

struct LatencyStats {
  double p50_us{0};
  double p95_us{0};
  double p99_us{0};
};

static LatencyStats compute_stats(std::vector<double> &samples) {
  if (samples.empty())
    return {};
  std::sort(samples.begin(), samples.end());
  auto at = [&](double p) {
    return samples[static_cast<std::size_t>(p * (samples.size() - 1))];
  };
  return {at(0.50), at(0.95), at(0.99)};
}

int main() {
  const std::vector<std::string> policies = {"lru", "lfu", "pomai_cost"};
  const std::vector<std::string> presets = {"hotset", "uniform", "writeheavy",
                                            "mixed"};
  constexpr std::uint64_t seed = 424242;

  std::cout << "Embedded cache benchmark (in-process, no network)\n";
  std::cout << "seed=" << seed << "\n";

  std::cout << "\n|workload|policy|ops/s|hit_rate|evictions|\n";
  std::cout << "|---|---:|---:|---:|---:|\n";

  std::cout << std::fixed << std::setprecision(2);

  // Engine-only throughput and hit rate.
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
      std::cout << "|" << preset << "|" << pname << "|" << (ops / seconds)
                << "|" << hit_rate << "|" << engine.stats().evictions << "|\n";
    }
  }

  // Engine get-latency percentiles for pomai_cost.
  std::cout
      << "\nEngine GET latency (pomai_cost, microseconds, p50/p95/p99):\n";
  for (const auto &preset : presets) {
    Engine engine({8 * 1024 * 1024, 256, 4 * 1024, 256},
                  make_policy_by_name("pomai_cost"));
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> u(0, 999);
    const int ops = 30000;
    std::vector<double> get_lat;
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
        auto t0 = std::chrono::steady_clock::now();
        if (engine.get(key).has_value())
          ++hits;
        auto t1 = std::chrono::steady_clock::now();
        get_lat.push_back(
            std::chrono::duration<double, std::micro>(t1 - t0).count());
      }
    }
    auto stats = compute_stats(get_lat);
    double hit_rate =
        gets > 0 ? static_cast<double>(hits) / static_cast<double>(gets) : 0.0;
    std::cout << "  " << std::setw(10) << preset << "  p50=" << stats.p50_us
              << "  p95=" << stats.p95_us << "  p99=" << stats.p99_us
              << "  hit_rate=" << hit_rate << "\n";
  }

  // PomaiCache (embedded API) benchmark with same workloads.
  std::cout << "\nPomaiCache embedded API (Set/Get) latency, pomai_cost:\n";
  for (const auto &preset : presets) {
    pomaicache::Config cfg;
    cfg.memory_limit_bytes = 8 * 1024 * 1024;
    cfg.data_dir = "./data_pomai_embedded";
    pomaicache::PomaiCache cache(cfg);

    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> u(0, 999);
    const int ops = 30000;
    std::vector<double> get_lat;
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
        std::span<const std::byte> val(
            reinterpret_cast<const std::byte *>(v.data()), v.size());
        cache.Set(key, val, pomaicache::Ttl{0});
      } else {
        ++gets;
        auto t0 = std::chrono::steady_clock::now();
        auto got = cache.Get(key);
        if (got.has_value())
          ++hits;
        auto t1 = std::chrono::steady_clock::now();
        get_lat.push_back(
            std::chrono::duration<double, std::micro>(t1 - t0).count());
      }
    }

    auto stats = compute_stats(get_lat);
    double hit_rate =
        gets > 0 ? static_cast<double>(hits) / static_cast<double>(gets) : 0.0;
    std::cout << "  " << std::setw(10) << preset << "  p50=" << stats.p50_us
              << "  p95=" << stats.p95_us << "  p99=" << stats.p99_us
              << "  hit_rate=" << hit_rate << "\n";
  }

  return 0;
}
