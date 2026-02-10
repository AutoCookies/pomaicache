#include "pomai_cache/engine.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <regex>
#include <sstream>

namespace pomai_cache {
namespace {
bool extract_double(const std::string& text, const std::string& key, double& out) {
  std::regex re("\"" + key + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)");
  std::smatch m;
  if (!std::regex_search(text, m, re)) return false;
  out = std::stod(m[1].str());
  return true;
}
bool extract_u64(const std::string& text, const std::string& key, std::uint64_t& out) {
  std::regex re("\"" + key + "\"\\s*:\\s*([0-9]+)");
  std::smatch m;
  if (!std::regex_search(text, m, re)) return false;
  out = static_cast<std::uint64_t>(std::stoull(m[1].str()));
  return true;
}
bool extract_string(const std::string& text, const std::string& key, std::string& out) {
  std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
  std::smatch m;
  if (!std::regex_search(text, m, re)) return false;
  out = m[1].str();
  return true;
}
} // namespace

Engine::Engine(EngineConfig cfg, std::unique_ptr<IEvictionPolicy> policy)
    : cfg_(cfg), policy_(std::move(policy)) {
  owner_miss_cost_default_["default"] = 1.0;
  owner_miss_cost_default_["premium"] = 2.0;
}

bool Engine::set(const std::string& key, const std::vector<std::uint8_t>& value,
                 std::optional<std::uint64_t> ttl_seconds, std::string owner,
                 std::string* err) {
  tick();
  if (key.empty() || key.size() > cfg_.max_key_len) {
    if (err) *err = "invalid key length";
    return false;
  }
  if (value.size() > cfg_.max_value_size) {
    if (err) *err = "value too large";
    return false;
  }

  Entry candidate;
  candidate.value = value;
  candidate.size_bytes = value.size();
  candidate.created_at = Clock::now();
  candidate.last_access = candidate.created_at;
  candidate.hit_count = 0;
  candidate.owner = owner.empty() ? "default" : std::move(owner);
  if (ttl_seconds.has_value()) {
    candidate.ttl_deadline = Clock::now() + std::chrono::seconds(*ttl_seconds);
  }

  CandidateView cv{key, &candidate, owner_miss_cost(candidate.owner)};
  if (!policy_->should_admit(cv)) {
    ++stats_.admissions_rejected;
    if (err) *err = "admission rejected";
    return false;
  }

  if (entries_.contains(key)) {
    memory_used_ -= entries_[key].size_bytes;
    policy_->on_erase(key);
  }

  entries_[key] = std::move(candidate);
  memory_used_ += entries_[key].size_bytes;
  policy_->on_insert(key, entries_[key]);

  if (entries_[key].ttl_deadline.has_value()) {
    const auto gen = ++expiry_generation_[key];
    expiry_heap_.push({*entries_[key].ttl_deadline, key, gen});
  }

  evict_until_fit();
  return true;
}

std::optional<std::vector<std::uint8_t>> Engine::get(const std::string& key) {
  tick();
  if (!exists_and_not_expired(key)) {
    ++stats_.misses;
    return std::nullopt;
  }
  auto& e = entries_[key];
  e.last_access = Clock::now();
  ++e.hit_count;
  ++stats_.hits;
  policy_->on_access(key, e);
  return e.value;
}

std::size_t Engine::del(const std::vector<std::string>& keys) {
  tick();
  std::size_t removed = 0;
  for (const auto& k : keys) {
    if (entries_.contains(k)) {
      erase_internal(k, false, false);
      ++removed;
    }
  }
  return removed;
}

bool Engine::expire(const std::string& key, std::uint64_t ttl_seconds) {
  tick();
  if (!entries_.contains(key)) return false;
  auto& e = entries_[key];
  e.ttl_deadline = Clock::now() + std::chrono::seconds(ttl_seconds);
  auto gen = ++expiry_generation_[key];
  expiry_heap_.push({*e.ttl_deadline, key, gen});
  return true;
}

std::optional<std::int64_t> Engine::ttl(const std::string& key) {
  tick();
  if (!entries_.contains(key)) return std::nullopt;
  auto& e = entries_[key];
  if (!e.ttl_deadline.has_value()) return -1;
  const auto now = Clock::now();
  const auto secs = std::chrono::duration_cast<std::chrono::seconds>(*e.ttl_deadline - now).count();
  return std::max<std::int64_t>(-2, secs);
}

std::vector<std::optional<std::vector<std::uint8_t>>> Engine::mget(const std::vector<std::string>& keys) {
  std::vector<std::optional<std::vector<std::uint8_t>>> out;
  out.reserve(keys.size());
  for (const auto& k : keys) out.push_back(get(k));
  return out;
}

void Engine::tick() {
  const auto now = Clock::now();
  std::size_t cleaned = 0;
  while (!expiry_heap_.empty() && cleaned < cfg_.ttl_cleanup_per_tick) {
    const auto& node = expiry_heap_.top();
    if (node.deadline > now) break;
    const auto key = node.key;
    const auto gen = node.generation;
    expiry_heap_.pop();
    if (!entries_.contains(key)) continue;
    if (expiry_generation_[key] != gen) continue;
    if (entries_[key].ttl_deadline.has_value() && *entries_[key].ttl_deadline <= now) {
      erase_internal(key, false, true);
    }
    ++cleaned;
  }
}

std::string Engine::info() const {
  std::ostringstream os;
  os << "policy_mode:" << policy_->name() << "\n";
  os << "policy_params_version:" << policy_->params().version << "\n";
  os << "keys:" << entries_.size() << "\n";
  os << "memory_used_bytes:" << memory_used_ << "\n";
  os << "memory_limit_bytes:" << cfg_.memory_limit_bytes << "\n";
  os << "hits:" << stats_.hits << "\n";
  os << "misses:" << stats_.misses << "\n";
  os << "evictions:" << stats_.evictions << "\n";
  os << "expirations:" << stats_.expirations << "\n";
  os << "admissions_rejected:" << stats_.admissions_rejected << "\n";

  std::vector<std::pair<std::string, std::uint64_t>> counts;
  counts.reserve(entries_.size());
  for (const auto& [k, v] : entries_) counts.emplace_back(k, v.hit_count);
  std::sort(counts.begin(), counts.end(), [](const auto& a, const auto& b) {
    if (a.second == b.second) return a.first < b.first;
    return a.second > b.second;
  });
  os << "topk_hits:";
  for (std::size_t i = 0; i < std::min<std::size_t>(5, counts.size()); ++i) {
    if (i) os << ",";
    os << counts[i].first << ":" << counts[i].second;
  }
  os << "\n";
  return os.str();
}

bool Engine::reload_params(const std::string& path, std::string* err) {
  std::ifstream in(path);
  if (!in.is_open()) {
    if (err) *err = "params file not found, using defaults";
    return false;
  }
  std::stringstream ss;
  ss << in.rdbuf();
  const std::string text = ss.str();

  PolicyParams p = policy_->params();
  auto clamp_d = [](double v, double lo, double hi) { return std::min(hi, std::max(lo, v)); };

  double d;
  std::uint64_t u;
  std::string s;
  if (extract_double(text, "w_miss", d)) p.w_miss = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_reuse", d)) p.w_reuse = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_mem", d)) p.w_mem = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "w_risk", d)) p.w_risk = clamp_d(d, 0.0, 1000.0);
  if (extract_double(text, "admit_threshold", d)) p.admit_threshold = clamp_d(d, -1e9, 1e9);
  if (extract_double(text, "evict_pressure", d)) p.evict_pressure = clamp_d(d, 0.1, 1.0);
  if (extract_u64(text, "max_evictions_per_second", u)) p.max_evictions_per_second = std::clamp(u, static_cast<std::uint64_t>(1), static_cast<std::uint64_t>(1000000));
  if (extract_u64(text, "max_admissions_per_second", u)) p.max_admissions_per_second = std::clamp(u, static_cast<std::uint64_t>(1), static_cast<std::uint64_t>(1000000));
  if (extract_string(text, "version", s)) p.version = s;

  policy_->set_params(p);
  return true;
}

void Engine::set_policy(std::unique_ptr<IEvictionPolicy> policy) {
  PolicyParams p = policy_->params();
  policy_ = std::move(policy);
  policy_->set_params(p);
}

bool Engine::exists_and_not_expired(const std::string& key) {
  if (!entries_.contains(key)) return false;
  auto& e = entries_[key];
  if (e.ttl_deadline.has_value() && *e.ttl_deadline <= Clock::now()) {
    erase_internal(key, false, true);
    return false;
  }
  return true;
}

void Engine::erase_internal(const std::string& key, bool eviction, bool expiration) {
  if (!entries_.contains(key)) return;
  memory_used_ -= entries_[key].size_bytes;
  policy_->on_erase(key);
  entries_.erase(key);
  expiry_generation_.erase(key);
  if (eviction) ++stats_.evictions;
  if (expiration) ++stats_.expirations;
}

void Engine::evict_until_fit() {
  std::size_t safety = entries_.size() + 1;
  while (memory_used_ > cfg_.memory_limit_bytes && safety-- > 0) {
    auto victim = policy_->pick_victim(entries_, memory_used_, cfg_.memory_limit_bytes);
    if (!victim.has_value()) break;
    erase_internal(*victim, true, false);
  }
}

double Engine::owner_miss_cost(const std::string& owner) const {
  auto it = owner_miss_cost_default_.find(owner);
  if (it == owner_miss_cost_default_.end()) return 1.0;
  return it->second;
}

} // namespace pomai_cache
