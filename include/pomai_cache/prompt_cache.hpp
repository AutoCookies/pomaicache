#pragma once

#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace pomai_cache {

// Configuration for prompt prefix caching. Tuned for edge / AI workloads.
struct PromptCacheConfig {
  bool enabled{true};
  std::uint64_t default_ttl_ms{300'000};     // 5 minutes
  std::size_t prefix_min_tokens{50};         // minimum tokens for reuse
  std::size_t max_cached_prefix_bytes{16u * 1024u * 1024u}; // hard cap for RAM index
};

struct PromptReuseResult {
  bool hit{false};
  std::string prompt_prefix_hash;
  std::size_t cached_tokens{0};
  std::size_t suffix_tokens{0};
  double savings_ratio{0.0};
};

struct PromptCacheStats {
  std::uint64_t hits{0};
  std::uint64_t misses{0};
  std::uint64_t total_queries{0};
  std::uint64_t cached_prefix_bytes{0};
  double average_savings_ratio{0.0};
  std::uint64_t entry_count{0};
};

// Manages prefix-based prompt caching for pre-tokenized prompts.
//
// Prompts are modeled as opaque byte sequences (e.g., serialized token ID
// arrays or partial embeddings). Callers are responsible for ensuring that
// the serialized representation obeys a prefix property: if a prefix P of a
// prompt Q is cached, then P's byte sequence is a prefix of Q's byte sequence.
//
// This manager uses AiArtifactCache + Engine for durable storage and SSD
// demotion, and keeps a compact in-memory index for fast longest-prefix
// lookup. It assumes single-threaded access per EngineShard.
class PromptCacheManager {
public:
  PromptCacheManager(Engine &engine, AiArtifactCache &ai_cache,
                     PromptCacheConfig config);

  // Store a prompt prefix identified by (tokenizer_id, prompt_prefix_hash).
  // `serialized_tokens` is the pre-tokenized representation, and
  // `cached_tokens` is the logical token count (for metrics and thresholds).
  //
  // When ttl_ms is not provided, default_ttl_ms from config is used.
  bool put_prefix(const std::string &tokenizer_id,
                  const std::string &prompt_prefix_hash,
                  const std::vector<std::uint8_t> &serialized_tokens,
                  std::uint64_t cached_tokens,
                  std::optional<std::uint64_t> ttl_ms = std::nullopt,
                  std::string *err = nullptr);

  // Attempt to reuse a cached prefix for a new query prompt identified by
  // (tokenizer_id, prompt_full_hash) and its serialized token sequence.
  //
  // The manager scans cached prefixes for the same tokenizer and selects the
  // longest prefix whose serialized bytes are a prefix of serialized_query.
  // The effective minimum length is max(config.prefix_min_tokens,
  // prefix_min_tokens_override.value_or(0)), compared against cached_tokens.
  PromptReuseResult
  reuse_for_query(const std::string &tokenizer_id,
                  const std::string &prompt_full_hash,
                  const std::vector<std::uint8_t> &serialized_query,
                  std::optional<std::size_t> prefix_min_tokens_override =
                      std::nullopt);

  // Invalidate a single cached prefix by its tokenizer_id + prefix hash.
  std::size_t invalidate_prefix(const std::string &tokenizer_id,
                                const std::string &prompt_prefix_hash);

  // Periodic maintenance for TTL expiration and resource caps.
  void tick();

  PromptCacheStats stats() const;

private:
  struct PrefixEntry {
    std::string canonical_key;
    std::string tokenizer_id;
    std::string prompt_prefix_hash;
    std::uint64_t cached_tokens{0};
    std::uint64_t reuse_count{0};
    std::uint64_t size_bytes{0};
    std::uint64_t expiry_epoch_ms{0};
  };

  struct ExpiryNode {
    std::uint64_t expiry_epoch_ms;
    std::string canonical_key;
    std::uint64_t generation;
    bool operator>(const ExpiryNode &other) const {
      return expiry_epoch_ms > other.expiry_epoch_ms;
    }
  };

  void maybe_expire();
  std::uint64_t now_ms() const;
  std::size_t key_count_for_tokenizer(const std::string &tokenizer_id) const;

  Engine &engine_;
  AiArtifactCache &ai_cache_;
  PromptCacheConfig cfg_;

  std::unordered_map<std::string, PrefixEntry> entries_;
  std::unordered_map<std::string, std::vector<std::string>> by_tokenizer_;
  std::priority_queue<ExpiryNode, std::vector<ExpiryNode>, std::greater<ExpiryNode>>
      expiry_heap_;
  std::unordered_map<std::string, std::uint64_t> expiry_generation_;

  PromptCacheStats stats_{};
};

} // namespace pomai_cache

