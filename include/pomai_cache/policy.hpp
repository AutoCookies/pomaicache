#pragma once

#include "pomai_cache/types.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>

namespace pomai_cache {

struct PolicyParams {
  double w_miss{1.0};
  double w_reuse{1.0};
  double w_mem{1.0};
  double w_risk{1.0};
  double admit_threshold{0.0};
  double evict_pressure{0.8};
  std::uint64_t max_evictions_per_second{10000};
  std::uint64_t max_admissions_per_second{10000};
  std::size_t owner_cap_bytes{0};
  std::string version{"defaults-v1"};
};

struct CandidateView {
  std::string key;
  const Entry* entry;
  double miss_cost;
};

class IEvictionPolicy {
public:
  virtual ~IEvictionPolicy() = default;
  virtual std::string name() const = 0;
  virtual bool should_admit(const CandidateView& candidate) = 0;
  virtual void on_insert(const std::string& key, const Entry& entry) = 0;
  virtual void on_access(const std::string& key, const Entry& entry) = 0;
  virtual void on_erase(const std::string& key) = 0;
  virtual std::optional<std::string> pick_victim(
      const std::unordered_map<std::string, Entry>& entries,
      std::size_t memory_used,
      std::size_t memory_limit) = 0;
  virtual void set_params(const PolicyParams& params) = 0;
  virtual const PolicyParams& params() const = 0;
};

} // namespace pomai_cache
