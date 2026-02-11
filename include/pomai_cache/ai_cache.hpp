#pragma once

#include "pomai_cache/engine.hpp"

#include <cstdint>
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

  bool put(const std::string &type, const std::string &key,
           const std::string &meta_json,
           const std::vector<std::uint8_t> &payload,
           std::string *err = nullptr);
  std::optional<ArtifactValue> get(const std::string &key);
  std::vector<std::optional<ArtifactValue>>
  mget(const std::vector<std::string> &keys);

  std::size_t invalidate_epoch(const std::string &epoch);
  std::size_t invalidate_model(const std::string &model_id);
  std::size_t invalidate_prefix(const std::string &prefix);

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

  Engine &engine_;
  mutable AiStats stats_{};
  std::unordered_map<std::string, BlobInfo> blob_index_;
  std::unordered_map<std::string, KeyInfo> key_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> epoch_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> model_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> prefix_index_;
  std::unordered_map<std::string, std::uint64_t> owner_ttl_defaults_;
  std::size_t prefix_index_cap_{4096};
};

} // namespace pomai_cache
