#pragma once

#include "pomai_cache/policy.hpp"

#include <cstdint>
#include <functional>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace pomai_cache {

struct EngineConfig {
  std::size_t memory_limit_bytes{64 * 1024 * 1024};
  std::size_t max_key_len{256};
  std::size_t max_value_size{1024 * 1024};
  std::size_t ttl_cleanup_per_tick{128};
};

struct EngineStats {
  std::uint64_t hits{0};
  std::uint64_t misses{0};
  std::uint64_t evictions{0};
  std::uint64_t expirations{0};
  std::uint64_t admissions_rejected{0};
};

class Engine {
public:
  explicit Engine(EngineConfig cfg, std::unique_ptr<IEvictionPolicy> policy);

  bool set(const std::string &key, const std::vector<std::uint8_t> &value,
           std::optional<std::uint64_t> ttl_ms, std::string owner,
           std::string *err = nullptr);
  std::optional<std::vector<std::uint8_t>> get(const std::string &key);
  std::size_t del(const std::vector<std::string> &keys);
  bool expire(const std::string &key, std::uint64_t ttl_seconds);
  std::optional<std::int64_t> ttl(const std::string &key);
  std::vector<std::optional<std::vector<std::uint8_t>>>
  mget(const std::vector<std::string> &keys);

  void tick();

  std::string info() const;
  bool reload_params(const std::string &path, std::string *err = nullptr);
  void set_canary_pct(std::uint64_t pct);
  std::uint64_t canary_pct() const { return canary_pct_; }
  bool rollback_to_lkg(std::string *err = nullptr);
  bool dump_stats(const std::string &path, std::string *err = nullptr) const;

  const EngineStats &stats() const { return stats_; }
  std::size_t memory_used() const { return memory_used_; }
  std::size_t size() const { return entries_.size(); }
  std::size_t expiration_backlog() const { return expiration_backlog_; }
  double memory_overhead_ratio() const;
  const IEvictionPolicy &policy() const { return *policy_; }
  void set_policy(std::unique_ptr<IEvictionPolicy> policy);

private:
  struct ExpiryNode {
    TimePoint deadline;
    std::string key;
    std::uint64_t generation;
    bool operator>(const ExpiryNode &other) const {
      return deadline > other.deadline;
    }
  };

  bool exists_and_not_expired(const std::string &key);
  void erase_internal(const std::string &key, bool eviction, bool expiration);
  void evict_until_fit();
  double owner_miss_cost(const std::string &owner) const;
  std::size_t bucket_for(std::size_t size) const;
  bool is_canary_key(const std::string &key) const;
  void maybe_evaluate_canary();
  static std::uint64_t p99_from_samples(const std::deque<std::uint64_t> &samples);

  struct CohortStats {
    std::uint64_t gets{0};
    std::uint64_t hits{0};
    std::deque<std::uint64_t> latency_us;
  };

  EngineConfig cfg_;
  std::unique_ptr<IEvictionPolicy> policy_;
  std::unordered_map<std::string, Entry> entries_;
  std::unordered_map<std::string, std::uint64_t> expiry_generation_;
  std::priority_queue<ExpiryNode, std::vector<ExpiryNode>,
                      std::greater<ExpiryNode>>
      expiry_heap_;
  std::unordered_map<std::string, double> owner_miss_cost_default_;
  std::unordered_map<std::string, std::size_t> owner_usage_;
  EngineStats stats_;
  std::size_t memory_used_{0};
  std::size_t bucket_used_{0};
  std::size_t expiration_backlog_{0};
  std::uint64_t canary_pct_{0};
  bool canary_active_{false};
  PolicyParams control_params_{};
  PolicyParams canary_params_{};
  std::string lkg_path_{".pomai_lkg_params.json"};
  CohortStats control_stats_;
  CohortStats canary_stats_;
  TimePoint canary_start_{Clock::now()};
  TimePoint last_guardrail_eval_{Clock::now()};
  std::uint64_t baseline_evictions_{0};
  std::uint64_t rollback_events_{0};
  std::string last_canary_event_{"none"};
};

std::unique_ptr<IEvictionPolicy> make_policy_by_name(const std::string &mode);

} // namespace pomai_cache
