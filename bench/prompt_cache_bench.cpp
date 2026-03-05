#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/prompt_cache.hpp"
#include "pomaicache.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace pomai_cache;
using hrc = std::chrono::high_resolution_clock;

struct LatencyStats {
  double p50_us{0};
  double p95_us{0};
  double p99_us{0};
  double p999_us{0};
  double mean_us{0};
  double min_us{0};
  double max_us{0};
};

static LatencyStats compute_stats(std::vector<double> &samples) {
  if (samples.empty())
    return {};
  std::sort(samples.begin(), samples.end());
  auto at = [&](double p) {
    return samples[static_cast<std::size_t>(p * (samples.size() - 1))];
  };
  double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
  return {at(0.50), at(0.95), at(0.99), at(0.999),
          sum / samples.size(), samples.front(), samples.back()};
}

struct PromptBenchResult {
  std::string name;
  double ops_s{0};
  LatencyStats lat;
  double hit_rate{0};
  double avg_savings_ratio{0};
};

// Serialize a token sequence into a byte vector (little-endian u64s).
static std::vector<std::uint8_t>
tokens_to_bytes(const std::vector<std::uint64_t> &tokens) {
  std::vector<std::uint8_t> bytes(tokens.size() * sizeof(std::uint64_t));
  std::memcpy(bytes.data(), tokens.data(), bytes.size());
  return bytes;
}

static PromptBenchResult
run_prompt_workload(const std::string &name, PromptCacheManager &pcm,
                    int ops, int hot_prefixes, int max_tokens,
                    double write_fraction) {
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<int> hot_dist(0, hot_prefixes - 1);
  std::uniform_int_distribution<int> len_dist(64, max_tokens);
  std::bernoulli_distribution write_dist(write_fraction);

  std::vector<double> latencies;
  latencies.reserve(static_cast<std::size_t>(ops));

  std::uint64_t gets = 0;
  std::uint64_t hits = 0;
  double savings_sum = 0.0;

  const std::string tokenizer_id = "tok";

  auto t_start = hrc::now();
  for (int i = 0; i < ops; ++i) {
    const int prefix_id = hot_dist(rng);
    const int total_tokens = len_dist(rng);
    const int prefix_tokens = std::max(32, total_tokens / 2);

    // Build a deterministic "token" sequence so that all prompts sharing a
    // prefix_id have a common prefix of prefix_tokens.
    std::vector<std::uint64_t> full_tokens(static_cast<std::size_t>(total_tokens));
    for (int t = 0; t < total_tokens; ++t) {
      if (t < prefix_tokens)
        full_tokens[static_cast<std::size_t>(t)] =
            static_cast<std::uint64_t>(prefix_id * 1000 + t);
      else
        full_tokens[static_cast<std::size_t>(t)] =
            static_cast<std::uint64_t>(prefix_id * 1000 + 100 + t);
    }

    std::vector<std::uint64_t> prefix_tokens_vec(
        full_tokens.begin(),
        full_tokens.begin() + static_cast<std::size_t>(prefix_tokens));

    std::vector<std::uint8_t> prefix_bytes = tokens_to_bytes(prefix_tokens_vec);
    std::vector<std::uint8_t> full_bytes = tokens_to_bytes(full_tokens);

    const std::string prefix_hash = "p" + std::to_string(prefix_id);

    auto t0 = hrc::now();

    if (write_dist(rng)) {
      // Store / refresh the prefix.
      pcm.put_prefix(tokenizer_id, prefix_hash, prefix_bytes,
                     static_cast<std::uint64_t>(prefix_tokens));
    } else {
      // Attempt reuse for a full query that shares the same prefix bytes.
      PromptReuseResult reuse =
          pcm.reuse_for_query(tokenizer_id, prefix_hash, full_bytes);
      ++gets;
      if (reuse.hit) {
        ++hits;
        savings_sum += reuse.savings_ratio;
      }
    }

    auto t1 = hrc::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());

    // Periodic maintenance.
    if ((i % 256) == 0)
      pcm.tick();
  }

  double elapsed_sec =
      std::chrono::duration<double>(hrc::now() - t_start).count();

  auto lat = compute_stats(latencies);
  double hit_rate = gets ? static_cast<double>(hits) / static_cast<double>(gets)
                         : 0.0;
  double avg_savings = gets ? (savings_sum / static_cast<double>(gets)) : 0.0;

  return {name,
          static_cast<double>(ops) / elapsed_sec,
          lat,
          hit_rate,
          avg_savings};
}

static PromptBenchResult
run_prompt_workload_embedded(const std::string &name,
                             pomaicache::PomaiCache &cache,
                             int ops, int hot_prefixes, int max_tokens,
                             double write_fraction) {
  std::mt19937_64 rng(1337);
  std::uniform_int_distribution<int> hot_dist(0, hot_prefixes - 1);
  std::uniform_int_distribution<int> len_dist(32, max_tokens);
  std::bernoulli_distribution write_dist(write_fraction);

  std::vector<double> latencies;
  latencies.reserve(static_cast<std::size_t>(ops));

  std::uint64_t gets = 0;
  std::uint64_t hits = 0;
  double savings_sum = 0.0;

  auto t_start = hrc::now();
  for (int i = 0; i < ops; ++i) {
    const int prefix_id = hot_dist(rng);
    const int total_tokens = len_dist(rng);

    std::vector<std::uint64_t> tokens(static_cast<std::size_t>(total_tokens));
    for (int t = 0; t < total_tokens; ++t) {
      tokens[static_cast<std::size_t>(t)] =
          static_cast<std::uint64_t>(prefix_id * 1000 + t);
    }

    std::vector<std::uint8_t> artifact_bytes(
        static_cast<std::size_t>(std::min(total_tokens * 2, 512)));
    std::fill(artifact_bytes.begin(), artifact_bytes.end(),
              static_cast<std::uint8_t>(prefix_id));

    auto t0 = hrc::now();

    if (write_dist(rng)) {
      std::span<const std::uint64_t> tok_span(tokens.data(), tokens.size());
      std::span<const std::byte> art_span(
          reinterpret_cast<const std::byte *>(artifact_bytes.data()),
          artifact_bytes.size());
      cache.PromptPut(tok_span, art_span, pomaicache::Ttl{300000});
    } else {
      std::span<const std::uint64_t> tok_span(tokens.data(), tokens.size());
      auto r = cache.PromptGet(tok_span);
      ++gets;
      if (r.hit) {
        ++hits;
        savings_sum += r.savings_ratio;
      }
    }

    auto t1 = hrc::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
  }

  double elapsed_sec =
      std::chrono::duration<double>(hrc::now() - t_start).count();

  auto lat = compute_stats(latencies);
  double hit_rate = gets ? static_cast<double>(hits) / static_cast<double>(gets)
                         : 0.0;
  double avg_savings = gets ? (savings_sum / static_cast<double>(gets)) : 0.0;

  return {name,
          static_cast<double>(ops) / elapsed_sec,
          lat,
          hit_rate,
          avg_savings};
}

int main(int argc, char **argv) {
  bool quick = false;
  std::string json_out = "prompt_cache_bench.json";
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--quick")
      quick = true;
    else
      json_out = argv[i];
  }
  const int ops_per_workload = quick ? 500 : 5000;

  EngineConfig cfg;
  cfg.memory_limit_bytes = 64 * 1024 * 1024;
  cfg.data_dir = "./data_prompt_bench";

  auto policy = make_policy_by_name("pomai_cost");
  Engine engine(cfg, std::move(policy));
  AiArtifactCache ai(engine);

  PromptCacheConfig pcfg;
  pcfg.enabled = true;
  pcfg.default_ttl_ms = 5 * 60 * 1000;
  pcfg.prefix_min_tokens = 32;
  pcfg.max_cached_prefix_bytes = 16u * 1024u * 1024u;

  PromptCacheManager pcm(engine, ai, pcfg);

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "Embedded token/prompt cache benchmark (in-process, no network)";
  if (quick)
    std::cout << " [--quick]";
  std::cout << "\n";

  std::vector<PromptBenchResult> results;
  results.push_back(run_prompt_workload("chatty_short_sessions",
                                        pcm,
                                        ops_per_workload,
                                        200,   // hot prefixes
                                        128,   // max tokens per prompt
                                        0.30   // writes
                                        ));
  results.push_back(run_prompt_workload("long_lived_system_prompts",
                                        pcm,
                                        ops_per_workload,
                                        50,    // fewer prefixes, more reuse
                                        256,   // max tokens per prompt
                                        0.10   // mostly reads
                                        ));

  // Embedded API workload using PomaiCache PromptPut/PromptGet.
  pomaicache::Config embedded_cfg;
  embedded_cfg.memory_limit_bytes = 64 * 1024 * 1024;
  embedded_cfg.data_dir = "./data_prompt_bench_embedded";
  pomaicache::PomaiCache cache(embedded_cfg);
  results.push_back(run_prompt_workload_embedded("embedded_api_hot_prompts",
                                                 cache,
                                                 ops_per_workload,
                                                 100,
                                                 256,
                                                 0.25));

  std::cout << "\n"
            << std::left << std::setw(28) << "workload"
            << std::right << std::setw(12) << "ops/s"
            << std::setw(12) << "p50_us"
            << std::setw(12) << "p95_us"
            << std::setw(12) << "hit_rate"
            << std::setw(16) << "avg_savings\n";
  std::cout << std::string(80, '-') << "\n";
  for (const auto &r : results) {
    std::cout << std::left << std::setw(28) << r.name << std::right
              << std::setw(12) << r.ops_s
              << std::setw(12) << r.lat.p50_us
              << std::setw(12) << r.lat.p95_us
              << std::setw(12) << r.hit_rate
              << std::setw(16) << r.avg_savings_ratio << "\n";
  }

  // JSON summary
  std::ofstream jf(json_out);
  jf << "{\n  \"prompt_cache_workloads\": [\n";
  for (std::size_t i = 0; i < results.size(); ++i) {
    const auto &r = results[i];
    jf << "    {\"name\":\"" << r.name << "\","
       << "\"ops_s\":" << r.ops_s << ","
       << "\"p50_us\":" << r.lat.p50_us << ","
       << "\"p95_us\":" << r.lat.p95_us << ","
       << "\"p99_us\":" << r.lat.p99_us << ","
       << "\"hit_rate\":" << r.hit_rate << ","
       << "\"avg_savings_ratio\":" << r.avg_savings_ratio << "}";
    if (i + 1 < results.size())
      jf << ",";
    jf << "\n";
  }
  jf << "  ]\n}\n";

  std::cout << "\nResults written to: " << json_out << "\n";
  return 0;
}

