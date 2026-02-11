#include "pomai_cache/engine.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>

namespace pomai_cache {
namespace {
bool extract_double(const std::string &text, const std::string &key,
                    double &out) {
  std::regex re("\"" + key + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)");
  std::smatch m;
  if (!std::regex_search(text, m, re))
    return false;
  out = std::stod(m[1].str());
  return true;
}
bool extract_u64(const std::string &text, const std::string &key,
                 std::uint64_t &out) {
  std::regex re("\"" + key + "\"\\s*:\\s*([0-9]+)");
  std::smatch m;
  if (!std::regex_search(text, m, re))
    return false;
  out = static_cast<std::uint64_t>(std::stoull(m[1].str()));
  return true;
}
bool extract_string(const std::string &text, const std::string &key,
                    std::string &out) {
  std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
  std::smatch m;
  if (!std::regex_search(text, m, re))
    return false;
  out = m[1].str();
  return true;
}
} // namespace

Engine::Engine(EngineConfig cfg, std::unique_ptr<IEvictionPolicy> policy)
    : cfg_(cfg), policy_(std::move(policy)) {
  owner_miss_cost_default_["default"] = 1.0;
  owner_miss_cost_default_["premium"] = 2.0;
  control_params_ = policy_->params();
}

bool Engine::set(const std::string &key, const std::vector<std::uint8_t> &value,
                 std::optional<std::uint64_t> ttl_ms, std::string owner,
                 std::string *err) {
  const auto start = Clock::now();
  tick();
  if (key.empty() || key.size() > cfg_.max_key_len) {
    if (err)
      *err = "invalid key length";
    return false;
  }
  if (value.size() > cfg_.max_value_size) {
    if (err)
      *err = "value too large";
    return false;
  }

  const std::string normalized_owner = owner.empty() ? "default" : owner;
  const auto owner_cap = policy_->params().owner_cap_bytes;
  std::size_t owner_used = owner_usage_[normalized_owner];
  if (entries_.contains(key)) {
    owner_used -= entries_[key].size_bytes;
  }
  if (owner_cap > 0 && owner_used + value.size() > owner_cap) {
    if (err)
      *err = "owner quota exceeded";
    return false;
  }

  Entry candidate;
  candidate.value = value;
  candidate.size_bytes = value.size();
  candidate.created_at = Clock::now();
  candidate.last_access = candidate.created_at;
  candidate.hit_count = 0;
  candidate.owner = normalized_owner;
  if (ttl_ms.has_value()) {
    candidate.ttl_deadline = Clock::now() + std::chrono::milliseconds(*ttl_ms);
  }

  CandidateView cv{key, &candidate, owner_miss_cost(candidate.owner)};
  const auto original = policy_->params();
  if (is_canary_key(key) && canary_active_)
    policy_->set_params(canary_params_);
  else
    policy_->set_params(control_params_);
  if (!policy_->should_admit(cv)) {
    policy_->set_params(original);
    ++stats_.admissions_rejected;
    if (err)
      *err = "admission rejected";
    return false;
  }
  policy_->set_params(original);

  if (entries_.contains(key)) {
    owner_usage_[entries_[key].owner] -= entries_[key].size_bytes;
    memory_used_ -= entries_[key].size_bytes;
    bucket_used_ -= bucket_for(entries_[key].size_bytes);
    policy_->on_erase(key);
  }

  entries_[key] = std::move(candidate);
  owner_usage_[entries_[key].owner] += entries_[key].size_bytes;
  memory_used_ += entries_[key].size_bytes;
  bucket_used_ += bucket_for(entries_[key].size_bytes);
  policy_->on_insert(key, entries_[key]);

  if (entries_[key].ttl_deadline.has_value()) {
    const auto gen = ++expiry_generation_[key];
    expiry_heap_.push({*entries_[key].ttl_deadline, key, gen});
  }

  evict_until_fit();
  const auto dur = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
  auto &cohort = is_canary_key(key) ? canary_stats_ : control_stats_;
  cohort.latency_us.push_back(static_cast<std::uint64_t>(std::max<std::int64_t>(0, dur)));
  if (cohort.latency_us.size() > 2048)
    cohort.latency_us.pop_front();
  return true;
}

std::optional<std::vector<std::uint8_t>> Engine::get(const std::string &key) {
  const auto start = Clock::now();
  tick();
  auto &cohort = is_canary_key(key) ? canary_stats_ : control_stats_;
  ++cohort.gets;
  if (!exists_and_not_expired(key)) {
    ++stats_.misses;
    const auto dur = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
    cohort.latency_us.push_back(static_cast<std::uint64_t>(std::max<std::int64_t>(0, dur)));
    if (cohort.latency_us.size() > 2048)
      cohort.latency_us.pop_front();
    return std::nullopt;
  }
  auto &e = entries_[key];
  e.last_access = Clock::now();
  ++e.hit_count;
  ++stats_.hits;
  ++cohort.hits;
  const auto dur = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
  cohort.latency_us.push_back(static_cast<std::uint64_t>(std::max<std::int64_t>(0, dur)));
  if (cohort.latency_us.size() > 2048)
    cohort.latency_us.pop_front();
  policy_->on_access(key, e);
  return e.value;
}

std::size_t Engine::del(const std::vector<std::string> &keys) {
  tick();
  std::size_t removed = 0;
  for (const auto &k : keys) {
    if (entries_.contains(k)) {
      erase_internal(k, false, false);
      ++removed;
    }
  }
  return removed;
}

bool Engine::expire(const std::string &key, std::uint64_t ttl_seconds) {
  tick();
  if (!entries_.contains(key))
    return false;
  auto &e = entries_[key];
  e.ttl_deadline = Clock::now() + std::chrono::seconds(ttl_seconds);
  auto gen = ++expiry_generation_[key];
  expiry_heap_.push({*e.ttl_deadline, key, gen});
  return true;
}

std::optional<std::int64_t> Engine::ttl(const std::string &key) {
  tick();
  if (!entries_.contains(key))
    return std::nullopt;
  auto &e = entries_[key];
  if (!e.ttl_deadline.has_value())
    return -1;
  const auto now = Clock::now();
  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(*e.ttl_deadline - now)
          .count();
  return std::max<std::int64_t>(-2, secs);
}

std::vector<std::optional<std::vector<std::uint8_t>>>
Engine::mget(const std::vector<std::string> &keys) {
  std::vector<std::optional<std::vector<std::uint8_t>>> out;
  out.reserve(keys.size());
  for (const auto &k : keys)
    out.push_back(get(k));
  return out;
}

void Engine::tick() {
  const auto now = Clock::now();
  std::size_t cleaned = 0;
  while (!expiry_heap_.empty() && cleaned < cfg_.ttl_cleanup_per_tick) {
    const auto &node = expiry_heap_.top();
    if (node.deadline > now)
      break;
    const auto key = node.key;
    const auto gen = node.generation;
    expiry_heap_.pop();
    if (!entries_.contains(key))
      continue;
    if (expiry_generation_[key] != gen)
      continue;
    if (entries_[key].ttl_deadline.has_value() &&
        *entries_[key].ttl_deadline <= now) {
      erase_internal(key, false, true);
    }
    ++cleaned;
  }

  expiration_backlog_ = 0;
  auto snapshot = expiry_heap_;
  while (!snapshot.empty()) {
    if (snapshot.top().deadline > now)
      break;
    ++expiration_backlog_;
    snapshot.pop();
  }
  maybe_evaluate_canary();
}

std::string Engine::info() const {
  std::ostringstream os;
  os << "policy_mode:" << policy_->name() << "\n";
  os << "policy_params_version:" << policy_->params().version << "\n";
  os << "keys:" << entries_.size() << "\n";
  os << "memory_used_bytes:" << memory_used_ << "\n";
  os << "memory_limit_bytes:" << cfg_.memory_limit_bytes << "\n";
  os << "memory_overhead_ratio:" << memory_overhead_ratio() << "\n";
  os << "expiration_backlog:" << expiration_backlog_ << "\n";
  os << "hits:" << stats_.hits << "\n";
  os << "misses:" << stats_.misses << "\n";
  os << "evictions:" << stats_.evictions << "\n";
  os << "expirations:" << stats_.expirations << "\n";
  os << "admissions_rejected:" << stats_.admissions_rejected << "\n";
  os << "canary_enabled:" << (canary_active_ ? 1 : 0) << "\n";
  os << "canary_pct:" << canary_pct_ << "\n";
  const double control_hr = control_stats_.gets == 0 ? 0.0 : static_cast<double>(control_stats_.hits) / static_cast<double>(control_stats_.gets);
  const double canary_hr = canary_stats_.gets == 0 ? 0.0 : static_cast<double>(canary_stats_.hits) / static_cast<double>(canary_stats_.gets);
  os << "canary_control_hit_rate:" << control_hr << "\n";
  os << "canary_candidate_hit_rate:" << canary_hr << "\n";
  os << "canary_control_p99_us:" << p99_from_samples(control_stats_.latency_us) << "\n";
  os << "canary_candidate_p99_us:" << p99_from_samples(canary_stats_.latency_us) << "\n";
  os << "canary_rollback_events:" << rollback_events_ << "\n";
  os << "canary_last_event:" << last_canary_event_ << "\n";

  std::vector<std::pair<std::string, std::uint64_t>> counts;
  counts.reserve(entries_.size());
  for (const auto &[k, v] : entries_)
    counts.emplace_back(k, v.hit_count);
  std::sort(counts.begin(), counts.end(), [](const auto &a, const auto &b) {
    if (a.second == b.second)
      return a.first < b.first;
    return a.second > b.second;
  });
  os << "topk_hits:";
  for (std::size_t i = 0; i < std::min<std::size_t>(5, counts.size()); ++i) {
    if (i)
      os << ",";
    os << counts[i].first << ":" << counts[i].second;
  }
  os << "\n";
  return os.str();
}

bool Engine::reload_params(const std::string &path, std::string *err) {
  std::ifstream in(path);
  if (!in.is_open()) {
    if (err)
      *err = "params file not found";
    return false;
  }
  std::stringstream ss;
  ss << in.rdbuf();
  const std::string text = ss.str();
  if (text.find('{') == std::string::npos ||
      text.find('}') == std::string::npos) {
    if (err)
      *err = "invalid schema";
    return false;
  }

  PolicyParams p = policy_->params();
  auto clamp_d = [](double v, double lo, double hi) {
    return std::min(hi, std::max(lo, v));
  };

  double d;
  std::uint64_t u;
  std::string s;
  if (extract_double(text, "w_miss", d))
    p.w_miss = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_reuse", d))
    p.w_reuse = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_mem", d))
    p.w_mem = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_risk", d))
    p.w_risk = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "admit_threshold", d))
    p.admit_threshold = clamp_d(d, -1e9, 1e9);
  if (extract_double(text, "evict_pressure", d))
    p.evict_pressure = clamp_d(d, 0.1, 1.0);
  if (extract_u64(text, "max_evictions_per_second", u))
    p.max_evictions_per_second = std::clamp(
        u, static_cast<std::uint64_t>(1), static_cast<std::uint64_t>(1000000));
  if (extract_u64(text, "max_admissions_per_second", u))
    p.max_admissions_per_second = std::clamp(
        u, static_cast<std::uint64_t>(1), static_cast<std::uint64_t>(1000000));
  if (extract_u64(text, "owner_cap_bytes", u))
    p.owner_cap_bytes = static_cast<std::size_t>(
        std::clamp(u, static_cast<std::uint64_t>(0),
                   static_cast<std::uint64_t>(1ULL << 40)));
  if (extract_string(text, "version", s))
    p.version = s;

  if (canary_pct_ > 0) {
    canary_params_ = p;
    canary_active_ = true;
    canary_start_ = Clock::now();
    baseline_evictions_ = stats_.evictions;
    canary_stats_ = {};
    control_stats_ = {};
    last_canary_event_ = "canary_started:" + p.version;
  } else {
    control_params_ = p;
    policy_->set_params(control_params_);
    last_canary_event_ = "params_loaded:" + p.version;
    std::ofstream out(lkg_path_);
    if (out.is_open())
      out << text;
  }
  return true;
}

void Engine::set_policy(std::unique_ptr<IEvictionPolicy> policy) {
  PolicyParams p = control_params_;
  policy_ = std::move(policy);
  policy_->set_params(p);
}

bool Engine::exists_and_not_expired(const std::string &key) {
  if (!entries_.contains(key))
    return false;
  auto &e = entries_[key];
  if (e.ttl_deadline.has_value() && *e.ttl_deadline <= Clock::now()) {
    erase_internal(key, false, true);
    return false;
  }
  return true;
}

void Engine::erase_internal(const std::string &key, bool eviction,
                            bool expiration) {
  if (!entries_.contains(key))
    return;
  owner_usage_[entries_[key].owner] -= entries_[key].size_bytes;
  memory_used_ -= entries_[key].size_bytes;
  bucket_used_ -= bucket_for(entries_[key].size_bytes);
  policy_->on_erase(key);
  entries_.erase(key);
  expiry_generation_.erase(key);
  if (eviction)
    ++stats_.evictions;
  if (expiration)
    ++stats_.expirations;
}

void Engine::evict_until_fit() {
  std::size_t safety = entries_.size() + 1;
  while (memory_used_ > cfg_.memory_limit_bytes && safety-- > 0) {
    auto victim =
        policy_->pick_victim(entries_, memory_used_, cfg_.memory_limit_bytes);
    if (!victim.has_value())
      break;
    erase_internal(*victim, true, false);
  }
}

double Engine::owner_miss_cost(const std::string &owner) const {
  auto it = owner_miss_cost_default_.find(owner);
  if (it == owner_miss_cost_default_.end())
    return 1.0;
  return it->second;
}

std::size_t Engine::bucket_for(std::size_t size) const {
  if (size <= 64)
    return 64;
  if (size <= 128)
    return 128;
  if (size <= 256)
    return 256;
  if (size <= 512)
    return 512;
  if (size <= 1024)
    return 1024;
  if (size <= 4096)
    return ((size + 511) / 512) * 512;
  return ((size + 4095) / 4096) * 4096;
}

double Engine::memory_overhead_ratio() const {
  if (memory_used_ == 0)
    return 1.0;
  return static_cast<double>(bucket_used_) / static_cast<double>(memory_used_);
}

void Engine::set_canary_pct(std::uint64_t pct) {
  canary_pct_ = std::min<std::uint64_t>(100, pct);
  if (canary_pct_ == 0)
    canary_active_ = false;
}

bool Engine::rollback_to_lkg(std::string *err) {
  std::ifstream in(lkg_path_);
  if (!in.is_open()) {
    if (err)
      *err = "lkg file not found";
    return false;
  }
  std::stringstream ss;
  ss << in.rdbuf();
  const std::string text = ss.str();
  PolicyParams p = control_params_;
  double d;
  std::uint64_t u;
  std::string s;
  if (extract_double(text, "w_miss", d)) p.w_miss = d;
  if (extract_double(text, "w_reuse", d)) p.w_reuse = d;
  if (extract_double(text, "w_mem", d)) p.w_mem = d;
  if (extract_double(text, "w_risk", d)) p.w_risk = d;
  if (extract_double(text, "admit_threshold", d)) p.admit_threshold = d;
  if (extract_double(text, "evict_pressure", d)) p.evict_pressure = d;
  if (extract_u64(text, "max_evictions_per_second", u)) p.max_evictions_per_second = u;
  if (extract_u64(text, "max_admissions_per_second", u)) p.max_admissions_per_second = u;
  if (extract_u64(text, "owner_cap_bytes", u)) p.owner_cap_bytes = static_cast<std::size_t>(u);
  if (extract_string(text, "version", s)) p.version = s;
  control_params_ = p;
  policy_->set_params(control_params_);
  canary_active_ = false;
  ++rollback_events_;
  last_canary_event_ = "rollback_to_lkg:" + p.version;
  return true;
}

bool Engine::dump_stats(const std::string &path, std::string *err) const {
  std::ofstream out(path);
  if (!out.is_open()) {
    if (err)
      *err = "unable to open dump file";
    return false;
  }
  out << "config_hash:na\n";
  out << "policy_params_version:" << control_params_.version << "\n";
  out << "memory_used_bytes:" << memory_used_ << "\n";
  out << "memory_limit_bytes:" << cfg_.memory_limit_bytes << "\n";
  out << "owners:";
  std::vector<std::pair<std::string, std::size_t>> owners(owner_usage_.begin(), owner_usage_.end());
  std::sort(owners.begin(), owners.end(), [](const auto &a, const auto &b){ return a.first < b.first;});
  for (std::size_t i = 0; i < std::min<std::size_t>(5, owners.size()); ++i) {
    if (i) out << ",";
    out << owners[i].first << ":" << owners[i].second;
  }
  out << "\n";
  out << "eviction_reasons:memory_pressure=" << stats_.evictions << ",expiry=" << stats_.expirations << "\n";
  return true;
}

bool Engine::is_canary_key(const std::string &key) const {
  if (!canary_active_ || canary_pct_ == 0)
    return false;
  const auto h = std::hash<std::string>{}(key) % 100;
  return h < canary_pct_;
}

std::uint64_t Engine::p99_from_samples(const std::deque<std::uint64_t> &samples) {
  if (samples.empty())
    return 0;
  std::vector<std::uint64_t> sorted(samples.begin(), samples.end());
  std::sort(sorted.begin(), sorted.end());
  const std::size_t idx = (sorted.size() - 1) * 99 / 100;
  return sorted[idx];
}

void Engine::maybe_evaluate_canary() {
  if (!canary_active_)
    return;
  const auto now = Clock::now();
  if (std::chrono::duration_cast<std::chrono::seconds>(now - last_guardrail_eval_).count() < 1)
    return;
  last_guardrail_eval_ = now;
  if (std::chrono::duration_cast<std::chrono::seconds>(now - canary_start_).count() < 5)
    return;
  const double control_hr = control_stats_.gets == 0 ? 1.0 : static_cast<double>(control_stats_.hits) / static_cast<double>(control_stats_.gets);
  const double canary_hr = canary_stats_.gets == 0 ? control_hr : static_cast<double>(canary_stats_.hits) / static_cast<double>(canary_stats_.gets);
  const auto control_p99 = static_cast<double>(p99_from_samples(control_stats_.latency_us));
  const auto canary_p99 = static_cast<double>(p99_from_samples(canary_stats_.latency_us));
  const auto evictions_delta = stats_.evictions - baseline_evictions_;
  const bool latency_bad = control_p99 > 0 && canary_p99 > control_p99 * 1.5;
  const bool hit_bad = canary_hr + 0.05 < control_hr;
  const bool eviction_bad = evictions_delta > 1000;
  if (latency_bad || hit_bad || eviction_bad) {
    std::string ignored;
    rollback_to_lkg(&ignored);
    last_canary_event_ = "auto_rollback_guardrail";
  }
}

} // namespace pomai_cache
