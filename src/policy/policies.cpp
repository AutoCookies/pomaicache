#include "pomai_cache/policy.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <unordered_map>

namespace pomai_cache {
namespace {

class LruPolicy final : public IEvictionPolicy {
public:
  std::string name() const override { return "lru"; }
  bool should_admit(const CandidateView &) override { return true; }
  void on_insert(const std::string &, const Entry &) override {}
  void on_access(const std::string &, const Entry &) override {}
  void on_erase(const std::string &) override {}
  std::optional<std::string>
  pick_victim(const PomaiTable &entries,
              std::size_t, std::size_t) override {
    if (entries.size() == 0)
      return std::nullopt;
    
    std::string best_key;
    TimePoint oldest = TimePoint::max();
    
    entries.iterate([&](const std::string& key, const Entry& e) {
        if (e.last_access < oldest) {
            oldest = e.last_access;
            best_key = key;
        }
    });

    return best_key;
  }
  void set_params(const PolicyParams &params) override { params_ = params; }
  const PolicyParams &params() const override { return params_; }

private:
  PolicyParams params_{};
};

class LfuPolicy final : public IEvictionPolicy {
public:
  std::string name() const override { return "lfu"; }
  bool should_admit(const CandidateView &) override { return true; }
  void on_insert(const std::string &, const Entry &) override {}
  void on_access(const std::string &, const Entry &) override {}
  void on_erase(const std::string &) override {}
  std::optional<std::string>
  pick_victim(const PomaiTable &entries,
              std::size_t, std::size_t) override {
    if (entries.size() == 0)
      return std::nullopt;

    std::string best_key;
    std::uint64_t min_hits = std::numeric_limits<std::uint64_t>::max();
    TimePoint oldest = TimePoint::max();

    entries.iterate([&](const std::string& key, const Entry& e) {
        if (e.hit_count < min_hits || (e.hit_count == min_hits && e.last_access < oldest)) {
            min_hits = e.hit_count;
            oldest = e.last_access;
            best_key = key;
        }
    });

    return best_key;
  }
  void set_params(const PolicyParams &params) override { params_ = params; }
  const PolicyParams &params() const override { return params_; }

private:
  PolicyParams params_{};
};

class PomaiCostPolicy final : public IEvictionPolicy {
public:
  std::string name() const override { return "pomai_cost"; }

  bool should_admit(const CandidateView &candidate) override {
    refresh_window();
    if (admissions_this_window_ >= params_.max_admissions_per_second)
      return false;
    const double b = benefit(candidate.key, *candidate.entry,
                             candidate.miss_cost, candidate.estimated_frequency);
    if (b <= params_.admit_threshold)
      return false;
    ++admissions_this_window_;
    return true;
  }

  void on_insert(const std::string &, const Entry &) override {}
  void on_access(const std::string &, const Entry &) override {}
  void on_erase(const std::string &) override {}

  std::optional<std::string>
  pick_victim(const PomaiTable &entries,
              std::size_t memory_used, std::size_t memory_limit) override {
    refresh_window();
    if (evictions_this_window_ >= params_.max_evictions_per_second)
      return std::nullopt;
    if (entries.size() == 0)
      return std::nullopt;

    if (memory_limit > 0 &&
        memory_used <
            static_cast<std::size_t>(static_cast<double>(memory_limit) *
                                     params_.evict_pressure)) {
      return std::nullopt;
    }

    // Use PomaiTable::getRandomSlot for O(1) sampling (Dragonfly pattern)
    constexpr size_t kSampleSize = 16;
    std::string best_victim;
    double worst_score = std::numeric_limits<double>::infinity();

    for (size_t i = 0; i < kSampleSize; ++i) {
      auto* slot = entries.getRandomSlot();
      if (!slot) continue;
      
      const double score = benefit(std::string(slot->key.view()), slot->value, 1.0);
      if (score < worst_score) {
        worst_score = score;
        best_victim = std::string(slot->key.view());
      }
    }

    if (best_victim.empty()) return std::nullopt;

    ++evictions_this_window_;
    return best_victim;
  }

  void set_params(const PolicyParams &params) override { params_ = params; }
  const PolicyParams &params() const override { return params_; }

private:
  double benefit(const std::string &, const Entry &e, double miss_cost,
                 std::uint16_t cms_freq = 0) const {
    const auto now = Clock::now();
    const double age_s = std::max(
        1.0, std::chrono::duration<double>(now - e.last_access).count());
    const double freq_signal =
        cms_freq > 0 ? static_cast<double>(cms_freq) : 0.0;
    double p_reuse = std::min(
        1.0,
        (static_cast<double>(e.hit_count) + freq_signal + 1.0) / (age_s + 1.0));
    if (e.owner == "prompt" && params_.prompt_reuse_weight > 0.0) {
      p_reuse *= (1.0 + params_.prompt_reuse_weight);
    }
    const double mem_cost = static_cast<double>(e.size_bytes) / 1024.0 +
                            static_cast<double>(e.size_bytes % 64) * 0.01;
    const double risk =
        (e.size_bytes > (256 * 1024) ? 1.0 : 0.0) + (age_s < 1.0 ? 0.5 : 0.0);
    return params_.w_miss * miss_cost + params_.w_reuse * p_reuse -
           params_.w_mem * mem_cost - params_.w_risk * risk;
  }

  void refresh_window() {
    const auto now = Clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(now - window_start_)
            .count() >= 1) {
      window_start_ = now;
      admissions_this_window_ = 0;
      evictions_this_window_ = 0;
    }
  }

  PolicyParams params_{};
  TimePoint window_start_{Clock::now()};
  std::uint64_t admissions_this_window_{0};
  std::uint64_t evictions_this_window_{0};
};

} // namespace

std::unique_ptr<IEvictionPolicy> make_policy_by_name(const std::string &mode) {
  if (mode == "lru")
    return std::make_unique<LruPolicy>();
  if (mode == "lfu")
    return std::make_unique<LfuPolicy>();
  return std::make_unique<PomaiCostPolicy>();
}

} // namespace pomai_cache
