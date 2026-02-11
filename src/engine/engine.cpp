#include "pomai_cache/engine.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
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
    : cfg_(std::move(cfg)), policy_(std::move(policy)),
      ssd_({cfg_.tier.ssd_enabled, cfg_.data_dir, cfg_.tier.ssd_value_min_bytes,
            cfg_.tier.ssd_max_bytes, cfg_.tier.ssd_max_read_mb_s,
            cfg_.tier.ssd_max_write_mb_s, 512, 0.25, cfg_.fsync_mode}) {
  owner_miss_cost_default_["default"] = 1.0;
  owner_miss_cost_default_["premium"] = 2.0;
  owner_miss_cost_default_["vector"] = 8.0;
  owner_miss_cost_default_["prompt"] = 2.0;
  owner_miss_cost_default_["rag"] = 3.0;
  owner_miss_cost_default_["rerank"] = 4.0;
  owner_miss_cost_default_["response"] = 5.0;
  if (cfg_.tier.ssd_enabled)
    cfg_.memory_limit_bytes = cfg_.tier.ram_max_bytes;
  ssd_.init();
}

bool Engine::set(const std::string &key, const std::vector<std::uint8_t> &value,
                 std::optional<std::uint64_t> ttl_ms, std::string owner,
                 std::string *err) {
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
  if (entries_.contains(key))
    owner_used -= entries_[key].size_bytes;
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
  if (ttl_ms.has_value())
    candidate.ttl_deadline = Clock::now() + std::chrono::milliseconds(*ttl_ms);

  CandidateView cv{key, &candidate, owner_miss_cost(candidate.owner)};
  if (!policy_->should_admit(cv)) {
    ++stats_.admissions_rejected;
    if (err)
      *err = "admission rejected";
    return false;
  }

  ++seq_;
  const bool to_ssd =
      cfg_.tier.ssd_enabled && value.size() >= cfg_.tier.ssd_value_min_bytes;
  if (to_ssd) {
    if (!ssd_.put(key, value, candidate.ttl_deadline, seq_, err))
      return false;
    if (entries_.contains(key))
      erase_internal(key, false, false);
    ssd_hit_count_[key] = 0;
    return true;
  }

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
  return true;
}

std::optional<std::vector<std::uint8_t>> Engine::get(const std::string &key) {
  tick();
  if (exists_and_not_expired(key)) {
    auto &e = entries_[key];
    e.last_access = Clock::now();
    ++e.hit_count;
    ++stats_.hits;
    policy_->on_access(key, e);
    return e.value;
  }

  if (!cfg_.tier.ssd_enabled) {
    ++stats_.misses;
    return std::nullopt;
  }
  SsdMeta m;
  auto v = ssd_.get(key, &m);
  if (!v.has_value()) {
    ++stats_.misses;
    return std::nullopt;
  }
  ++stats_.hits;

  auto &hits = ssd_hit_count_[key];
  hits++;
  if (hits >= cfg_.tier.promotion_hits &&
      v->size() < cfg_.tier.ssd_value_min_bytes) {
    promote_queue_.push_back(key);
    hits = 0;
  }
  return v;
}

std::size_t Engine::del(const std::vector<std::string> &keys) {
  tick();
  std::size_t removed = 0;
  for (const auto &k : keys) {
    bool deleted = false;
    if (entries_.contains(k)) {
      erase_internal(k, false, false);
      deleted = true;
    }
    if (cfg_.tier.ssd_enabled && ssd_.contains(k)) {
      ++seq_;
      ssd_.del(k, seq_);
      deleted = true;
    }
    if (deleted)
      ++removed;
  }
  return removed;
}

bool Engine::expire(const std::string &key, std::uint64_t ttl_seconds) {
  tick();
  auto deadline = Clock::now() + std::chrono::seconds(ttl_seconds);
  if (entries_.contains(key)) {
    auto &e = entries_[key];
    e.ttl_deadline = deadline;
    auto gen = ++expiry_generation_[key];
    expiry_heap_.push({*e.ttl_deadline, key, gen});
    return true;
  }
  if (cfg_.tier.ssd_enabled) {
    auto v = ssd_.get(key);
    if (v.has_value()) {
      ++seq_;
      return ssd_.put(key, *v, deadline, seq_);
    }
  }
  return false;
}

std::optional<std::int64_t> Engine::ttl(const std::string &key) {
  tick();
  if (entries_.contains(key)) {
    auto &e = entries_[key];
    if (!e.ttl_deadline.has_value())
      return -1;
    const auto now = Clock::now();
    const auto secs =
        std::chrono::duration_cast<std::chrono::seconds>(*e.ttl_deadline - now)
            .count();
    return std::max<std::int64_t>(-2, secs);
  }
  if (cfg_.tier.ssd_enabled) {
    SsdMeta m;
    auto v = ssd_.get(key, &m);
    if (!v.has_value())
      return std::nullopt;
    if (m.ttl_epoch_ms < 0)
      return -1;
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      Clock::now().time_since_epoch())
                      .count();
    auto remain = (m.ttl_epoch_ms - now_ms) / 1000;
    return std::max<std::int64_t>(-2, remain);
  }
  return std::nullopt;
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
        *entries_[key].ttl_deadline <= now)
      erase_internal(key, false, true);
    ++cleaned;
  }
  if (cfg_.tier.ssd_enabled)
    ssd_.erase_expired(cfg_.ttl_cleanup_per_tick, now);

  std::size_t tier_work = 0;
  while (!promote_queue_.empty() && tier_work < cfg_.tier_work_per_tick) {
    std::string key = promote_queue_.front();
    promote_queue_.pop_front();
    if (!entries_.contains(key)) {
      SsdMeta m;
      auto v = ssd_.get(key, &m);
      if (v.has_value() && v->size() < cfg_.tier.ssd_value_min_bytes) {
        std::optional<std::uint64_t> ttl_ms;
        if (m.ttl_epoch_ms >= 0) {
          auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            Clock::now().time_since_epoch())
                            .count();
          if (m.ttl_epoch_ms > now_ms)
            ttl_ms = static_cast<std::uint64_t>(m.ttl_epoch_ms - now_ms);
        }
        set(key, *v, ttl_ms, "default");
        ++seq_;
        ssd_.del(key, seq_);
      }
    }
    ++tier_work;
  }

  maybe_enqueue_demotion();
  while (!demote_queue_.empty() && tier_work < cfg_.tier_work_per_tick) {
    auto key = demote_queue_.front();
    demote_queue_.pop_front();
    if (!entries_.contains(key)) {
      ++tier_work;
      continue;
    }
    ++seq_;
    ssd_.put(key, entries_[key].value, entries_[key].ttl_deadline, seq_);
    erase_internal(key, true, false);
    ++tier_work;
  }
  if (cfg_.tier.ssd_enabled)
    ssd_.maybe_compact();

  expiration_backlog_ = 0;
  auto snapshot = expiry_heap_;
  while (!snapshot.empty()) {
    if (snapshot.top().deadline > now)
      break;
    ++expiration_backlog_;
    snapshot.pop();
  }
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
  os << "ram_bytes:" << memory_used_ << "\n";
  os << "ssd_bytes:" << ssd_.stats().bytes << "\n";
  os << "ssd_gets:" << ssd_.stats().gets << "\n";
  os << "ssd_hits:" << ssd_.stats().hits << "\n";
  os << "ssd_misses:" << ssd_.stats().misses << "\n";
  os << "promotions:" << ssd_.stats().promotions << "\n";
  os << "demotions:" << ssd_.stats().demotions << "\n";
  os << "ssd_read_mb:" << ssd_.stats().read_mb << "\n";
  os << "ssd_write_mb:" << ssd_.stats().write_mb << "\n";
  os << "tier_backlog:" << (promote_queue_.size() + demote_queue_.size())
     << "\n";
  os << "ssd_gc_runs:" << ssd_.stats().gc_runs << "\n";
  os << "ssd_gc_bytes_reclaimed:" << ssd_.stats().gc_bytes_reclaimed << "\n";
  os << "ssd_gc_time_ms:" << ssd_.stats().gc_time_ms << "\n";
  os << "fragmentation_estimate:" << ssd_.stats().fragmentation_estimate
     << "\n";
  os << "ssd_index_rebuild_ms:" << ssd_.stats().index_rebuild_ms << "\n";

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

  policy_->set_params(p);
  return true;
}

void Engine::set_policy(std::unique_ptr<IEvictionPolicy> policy) {
  PolicyParams p = policy_->params();
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
    if (cfg_.tier.ssd_enabled) {
      demote_queue_.push_back(*victim);
      break;
    }
    erase_internal(*victim, true, false);
  }
}

void Engine::maybe_enqueue_demotion() {
  if (!cfg_.tier.ssd_enabled)
    return;
  const double pressure = static_cast<double>(memory_used_) /
                          std::max<std::size_t>(1, cfg_.memory_limit_bytes);
  if (pressure < cfg_.tier.demotion_pressure)
    return;
  auto victim =
      policy_->pick_victim(entries_, memory_used_, cfg_.memory_limit_bytes);
  if (victim.has_value())
    demote_queue_.push_back(*victim);
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

} // namespace pomai_cache
