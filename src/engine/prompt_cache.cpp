#include "pomai_cache/prompt_cache.hpp"

#include <algorithm>
#include <chrono>
#include <sstream>

namespace pomai_cache {
namespace {

std::uint64_t to_ms(TimePoint tp) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          tp.time_since_epoch())
          .count());
}

} // namespace

PromptCacheManager::PromptCacheManager(Engine &engine, AiArtifactCache &ai_cache,
                                       PromptCacheConfig config)
    : engine_(engine), ai_cache_(ai_cache), cfg_(std::move(config)) {}

bool PromptCacheManager::put_prefix(
    const std::string &tokenizer_id, const std::string &prompt_prefix_hash,
    const std::vector<std::uint8_t> &serialized_tokens,
    std::uint64_t cached_tokens, std::optional<std::uint64_t> ttl_ms,
    std::string *err) {
  if (!cfg_.enabled)
    return false;
  if (serialized_tokens.empty() || cached_tokens == 0)
    return false;

  const auto now = Clock::now();
  const auto ttl = ttl_ms.value_or(cfg_.default_ttl_ms);
  const auto expiry = to_ms(now) + ttl;

  const auto canonical_key =
      canonical_prompt_key(tokenizer_id, prompt_prefix_hash);

  // Prepare AI metadata for the prefix payload. We model cached prompt
  // prefixes as regular `prompt` artifacts so they participate in the same
  // eviction and TTL behaviour as other prompt entries.
  ArtifactMeta meta;
  meta.artifact_type = "prompt";
  meta.owner = "prompt";
  meta.schema_version = "v1";
  meta.tokenizer_id = tokenizer_id;
  meta.created_at_ms = to_ms(now);
  meta.ttl_ms = ttl;
  meta.size_bytes = serialized_tokens.size();
  meta.content_hash = AiArtifactCache::fast_hash_hex(serialized_tokens);

  // Treat cached prefix tokens as 10x cheaper than re-generating them from the
  // model. This is exposed through AI.STATS and cost_report.
  meta.inference_tokens = cached_tokens;
  meta.miss_cost = 2.0; // logical miss cost for a prompt
  meta.dollar_cost = (meta.miss_cost * static_cast<double>(cached_tokens) *
                      0.001) /
                     10.0;

  const auto meta_json = AiArtifactCache::meta_to_json(meta);

  std::string local_err;
  std::string *put_err = err ? err : &local_err;
  if (!ai_cache_.put("prompt", canonical_key, meta_json, serialized_tokens,
                     put_err)) {
    return false;
  }

  auto &entry = entries_[canonical_key];
  entry.canonical_key = canonical_key;
  entry.tokenizer_id = tokenizer_id;
  entry.prompt_prefix_hash = prompt_prefix_hash;
  entry.cached_tokens = cached_tokens;
  entry.size_bytes = serialized_tokens.size();
  entry.expiry_epoch_ms = expiry;

  auto &vec = by_tokenizer_[tokenizer_id];
  if (std::find(vec.begin(), vec.end(), canonical_key) == vec.end())
    vec.push_back(canonical_key);

  const auto gen = ++expiry_generation_[canonical_key];
  expiry_heap_.push({expiry, canonical_key, gen});

  stats_.cached_prefix_bytes += serialized_tokens.size();
  stats_.entry_count = entries_.size();
  return true;
}

PromptReuseResult PromptCacheManager::reuse_for_query(
    const std::string &tokenizer_id, const std::string &prompt_full_hash,
    const std::vector<std::uint8_t> &serialized_query,
    std::optional<std::size_t> prefix_min_tokens_override) {
  PromptReuseResult result;
  if (!cfg_.enabled || serialized_query.empty())
    return result;

  maybe_expire();

  ++stats_.total_queries;

  auto it_vec = by_tokenizer_.find(tokenizer_id);
  if (it_vec == by_tokenizer_.end()) {
    ++stats_.misses;
    return result;
  }

  const std::size_t min_tokens = std::max<std::size_t>(
      cfg_.prefix_min_tokens, prefix_min_tokens_override.value_or(0));

  std::size_t best_tokens = 0;
  std::string best_key;

  for (const auto &key : it_vec->second) {
    auto it = entries_.find(key);
    if (it == entries_.end())
      continue;
    auto &e = it->second;
    if (e.cached_tokens < min_tokens)
      continue;

    auto val = ai_cache_.get(key);
    if (!val.has_value())
      continue;
    const auto &prefix_bytes = val->payload;
    if (prefix_bytes.size() > serialized_query.size())
      continue;

    // Byte-wise prefix check against the cached payload. Callers guarantee that
    // the serialized representation preserves prompt-prefix relationships at
    // the byte level, so this is sufficient to validate reuse.
    bool is_prefix = std::equal(prefix_bytes.begin(), prefix_bytes.end(),
                                serialized_query.begin());
    if (!is_prefix)
      continue;
    if (e.cached_tokens > best_tokens) {
      best_tokens = static_cast<std::size_t>(e.cached_tokens);
      best_key = key;
    }
  }

  if (best_key.empty()) {
    ++stats_.misses;
    return result;
  }

  auto &entry = entries_[best_key];
  entry.reuse_count += 1;

  const std::size_t suffix_tokens =
      best_tokens >= serialized_query.size()
          ? 0
          : static_cast<std::size_t>(serialized_query.size() - best_tokens);

  result.hit = true;
  result.prompt_prefix_hash = entry.prompt_prefix_hash;
  result.cached_tokens = best_tokens;
  result.suffix_tokens = suffix_tokens;
  const auto denom =
      static_cast<double>(best_tokens + std::max<std::size_t>(1, suffix_tokens));
  result.savings_ratio = static_cast<double>(best_tokens) / denom;

  ++stats_.hits;
  // Track cached token reuse as discounted cost in savings ratio.
  const double total_q =
      static_cast<double>(std::max<std::uint64_t>(1, stats_.total_queries));
  const double prev_sum =
      stats_.average_savings_ratio * (total_q - 1.0);
  stats_.average_savings_ratio = (prev_sum + result.savings_ratio) / total_q;

  // Touch underlying engine entry so eviction policy sees reuse.
  engine_.get(best_key);

  return result;
}

std::size_t PromptCacheManager::invalidate_prefix(
    const std::string &tokenizer_id, const std::string &prompt_prefix_hash) {
  const auto canonical_key =
      canonical_prompt_key(tokenizer_id, prompt_prefix_hash);

  auto it = entries_.find(canonical_key);
  if (it == entries_.end())
    return 0;

  auto &entry = it->second;
  if (stats_.cached_prefix_bytes >= entry.size_bytes)
    stats_.cached_prefix_bytes -= entry.size_bytes;

  auto vec_it = by_tokenizer_.find(tokenizer_id);
  if (vec_it != by_tokenizer_.end()) {
    auto &v = vec_it->second;
    v.erase(std::remove(v.begin(), v.end(), canonical_key), v.end());
    if (v.empty())
      by_tokenizer_.erase(vec_it);
  }

  entries_.erase(it);
  stats_.entry_count = entries_.size();

  // Best-effort invalidation of the backing AI artifact.
  ai_cache_.invalidate_prefix(canonical_key);
  return 1;
}

void PromptCacheManager::tick() {
  if (!cfg_.enabled)
    return;
  maybe_expire();
}

PromptCacheStats PromptCacheManager::stats() const { return stats_; }

void PromptCacheManager::maybe_expire() {
  const auto now = now_ms();
  std::size_t cleaned = 0;
  constexpr std::size_t kMaxPerTick = 256;

  while (!expiry_heap_.empty() && cleaned < kMaxPerTick) {
    const auto &node = expiry_heap_.top();
    if (node.expiry_epoch_ms > now)
      break;
    const auto key = node.canonical_key;
    const auto gen = node.generation;
    expiry_heap_.pop();

    auto it_gen = expiry_generation_.find(key);
    if (it_gen == expiry_generation_.end() || it_gen->second != gen)
      continue;

    auto it = entries_.find(key);
    if (it == entries_.end())
      continue;

    auto &entry = it->second;
    if (stats_.cached_prefix_bytes >= entry.size_bytes)
      stats_.cached_prefix_bytes -= entry.size_bytes;

    auto vec_it = by_tokenizer_.find(entry.tokenizer_id);
    if (vec_it != by_tokenizer_.end()) {
      auto &v = vec_it->second;
      v.erase(std::remove(v.begin(), v.end(), key), v.end());
      if (v.empty())
        by_tokenizer_.erase(vec_it);
    }

    entries_.erase(it);
    expiry_generation_.erase(key);
    ++cleaned;
  }
  stats_.entry_count = entries_.size();
}

std::uint64_t PromptCacheManager::now_ms() const {
  return to_ms(Clock::now());
}

std::size_t
PromptCacheManager::key_count_for_tokenizer(const std::string &tokenizer_id) const {
  auto it = by_tokenizer_.find(tokenizer_id);
  if (it == by_tokenizer_.end())
    return 0;
  return it->second.size();
}

} // namespace pomai_cache

