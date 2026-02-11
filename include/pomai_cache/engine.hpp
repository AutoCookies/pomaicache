#pragma once

#include "pomai_cache/policy.hpp"
#include "pomai_cache/ssd_store.hpp"

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace pomai_cache {

struct TierConfig {
  bool ssd_enabled{false};
  std::size_t ssd_value_min_bytes{32 * 1024};
  std::size_t ssd_max_bytes{2ULL * 1024 * 1024 * 1024};
  std::size_t ram_max_bytes{64 * 1024 * 1024};
  std::uint64_t promotion_hits{3};
  double demotion_pressure{0.90};
  std::size_t ssd_max_read_mb_s{256};
  std::size_t ssd_max_write_mb_s{256};
};

struct EngineConfig {
  std::size_t memory_limit_bytes{64 * 1024 * 1024};
  std::size_t max_key_len{256};
  std::size_t max_value_size{1024 * 1024};
  std::size_t ttl_cleanup_per_tick{128};
  std::size_t tier_work_per_tick{64};
  std::string data_dir{"./data"};
  TierConfig tier{};
  FsyncMode fsync_mode{FsyncMode::EverySec};
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
  void maybe_enqueue_demotion();

  EngineConfig cfg_;
  std::unique_ptr<IEvictionPolicy> policy_;
  std::unordered_map<std::string, Entry> entries_;
  std::unordered_map<std::string, std::uint64_t> expiry_generation_;
  std::priority_queue<ExpiryNode, std::vector<ExpiryNode>,
                      std::greater<ExpiryNode>>
      expiry_heap_;
  std::unordered_map<std::string, std::uint64_t> ssd_hit_count_;
  std::deque<std::string> promote_queue_;
  std::deque<std::string> demote_queue_;
  std::unordered_map<std::string, double> owner_miss_cost_default_;
  std::unordered_map<std::string, std::size_t> owner_usage_;
  EngineStats stats_;
  std::size_t memory_used_{0};
  std::size_t bucket_used_{0};
  std::size_t expiration_backlog_{0};
  std::uint64_t seq_{0};

  SsdStore ssd_;
};

std::unique_ptr<IEvictionPolicy> make_policy_by_name(const std::string &mode);

} // namespace pomai_cache
