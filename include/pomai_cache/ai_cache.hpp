#pragma once

#include "pomai_cache/bloom_filter.hpp"
#include "pomai_cache/compression.hpp"
#include "pomai_cache/dep_graph.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/vector_index.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pomai_cache {

struct ArtifactMeta {
  std::string artifact_type;
  std::string owner{"default"};
  std::string schema_version{"v1"};
  std::string model_id;
  std::string tokenizer_id;
  std::string dataset_id;
  std::string source_id;
  std::string chunk_id;
  std::string source_rev;
  std::string snapshot_epoch;
  std::uint64_t created_at_ms{0};
  std::uint64_t ttl_ms{0};
  std::size_t size_bytes{0};
  std::string content_hash;
  std::string tags_json{"{}"};
  double miss_cost{1.0};

  // Token-economics fields
  std::uint64_t inference_tokens{0};
  std::uint64_t inference_latency_ms{0};
  double dollar_cost{0.0};

  // Pipeline dependency parents
  std::vector<std::string> depends_on;
};

struct ArtifactValue {
  ArtifactMeta meta;
  std::vector<std::uint8_t> payload;
};

struct AiStats {
  std::uint64_t puts{0};
  std::uint64_t gets{0};
  std::uint64_t hits{0};
  std::uint64_t misses{0};
  std::uint64_t dedup_hits{0};
  std::uint64_t dedup_blobs{0};

  // Token economics
  double total_dollar_saved{0.0};
  std::uint64_t total_tokens_saved{0};
  std::uint64_t total_latency_saved_ms{0};

  // Similarity
  std::uint64_t sim_queries{0};
  std::uint64_t sim_hits{0};

  // Cascade
  std::uint64_t cascade_invalidations{0};

  // Streaming
  std::uint64_t stream_begins{0};
  std::uint64_t stream_completions{0};

  // Compression
  double avg_compression_ratio{1.0};
};

struct CostReport {
  double total_dollar_saved{0.0};
  std::uint64_t total_tokens_saved{0};
  std::uint64_t total_latency_saved_ms{0};
  std::uint64_t total_hits{0};
  double dollars_per_hour{0.0};
};

struct StreamEntry {
  ArtifactMeta meta;
  std::vector<std::vector<std::uint8_t>> chunks;
  bool finalized{false};
};

std::string canonical_embedding_key(const std::string &model_id,
                                    const std::string &input_hash, int dim,
                                    const std::string &dtype);
std::string canonical_prompt_key(const std::string &tokenizer_id,
                                 const std::string &prompt_hash);
std::string canonical_rag_chunk_key(const std::string &source_id,
                                    const std::string &chunk_id,
                                    const std::string &rev);
std::string canonical_rerank_key(const std::string &query_hash,
                                 const std::string &index_epoch, int topk,
                                 const std::string &params_hash);
std::string canonical_response_key(const std::string &prompt_hash,
                                   const std::string &params_hash,
                                   const std::string &model_id);

class AiArtifactCache {
public:
  explicit AiArtifactCache(Engine &engine);

  // --- Original artifact ops ---
  bool put(const std::string &type, const std::string &key,
           const std::string &meta_json,
           const std::vector<std::uint8_t> &payload,
           std::string *err = nullptr);
  std::optional<ArtifactValue> get(const std::string &key);
  std::vector<std::optional<ArtifactValue>>
  mget(const std::vector<std::string> &keys);

  // --- Invalidation ---
  std::size_t invalidate_epoch(const std::string &epoch);
  std::size_t invalidate_model(const std::string &model_id);
  std::size_t invalidate_prefix(const std::string &prefix);
  std::size_t invalidate_cascade(const std::string &key);

  // --- Similarity search (Feature 1) ---
  bool sim_put(const std::string &key, const std::vector<float> &vector,
               const std::vector<std::uint8_t> &payload,
               const std::string &meta_json, std::string *err = nullptr);
  struct SimSearchResult {
    std::string key;
    float score;
    ArtifactValue value;
  };
  std::vector<SimSearchResult> sim_get(const std::vector<float> &query,
                                       std::size_t top_k = 1,
                                       float threshold = 0.9f);

  // --- Token economics (Feature 2) ---
  CostReport cost_report() const;
  void set_budget(double max_dollar_per_hour);
  double budget() const { return budget_dollar_per_hour_; }

  // --- Pipeline cascade (Feature 3) ---
  bool put_with_deps(const std::string &type, const std::string &key,
                     const std::string &meta_json,
                     const std::vector<std::uint8_t> &payload,
                     const std::vector<std::string> &depends_on,
                     std::string *err = nullptr);
  std::vector<std::string> get_dependents(const std::string &key) const;

  // --- Streaming (Feature 6) ---
  bool stream_begin(const std::string &key, const std::string &meta_json,
                    std::string *err = nullptr);
  bool stream_append(const std::string &key,
                     const std::vector<std::uint8_t> &chunk,
                     std::string *err = nullptr);
  bool stream_end(const std::string &key, std::string *err = nullptr);
  std::optional<ArtifactValue> stream_get(const std::string &key);

  // --- Introspection ---
  std::string stats() const;
  std::string top_hot(std::size_t n) const;
  std::string top_costly(std::size_t n) const;
  std::string explain(const std::string &key) const;

  static bool parse_meta_json(const std::string &json, ArtifactMeta &out,
                              std::string *err = nullptr);
  static std::string meta_to_json(const ArtifactMeta &meta);
  static std::string fast_hash_hex(const std::vector<std::uint8_t> &payload);

private:
  struct BlobInfo {
    std::size_t refcount{0};
    std::size_t size_bytes{0};
  };
  struct KeyInfo {
    ArtifactMeta meta;
    std::string blob_hash;
    std::uint64_t hits{0};
    std::string explain;
  };

  std::uint64_t ttl_default_ms(const std::string &owner) const;
  void index_key(const std::string &key, const ArtifactMeta &meta);
  void deindex_key(const std::string &key, const KeyInfo &ki);
  std::size_t invalidate_keys(const std::unordered_set<std::string> &keys);
  bool check_budget() const;

  Engine &engine_;
  mutable AiStats stats_{};
  BloomFilter blob_bloom_{65536, 0.01};
  std::unordered_map<std::string, BlobInfo> blob_index_;
  std::unordered_map<std::string, KeyInfo> key_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> epoch_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> model_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>>
      prefix_index_;
  std::unordered_map<std::string, std::uint64_t> owner_ttl_defaults_;
  std::size_t prefix_index_cap_{4096};

  // Feature 1: Vector similarity
  VectorIndex vector_index_{768, DistanceMetric::Cosine};

  // Feature 2: Token economics
  double budget_dollar_per_hour_{0.0};
  mutable TimePoint budget_window_start_{Clock::now()};
  mutable double budget_spent_this_hour_{0.0};

  // Feature 3: Pipeline dependency graph
  DepGraph dep_graph_;

  // Feature 6: Streaming entries
  std::unordered_map<std::string, StreamEntry> streams_;

  // Feature 4: Compression stats
  mutable double total_compression_ratio_sum_{0.0};
  mutable std::uint64_t compression_count_{0};
};

} // namespace pomai_cache
