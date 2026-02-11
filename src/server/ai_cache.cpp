#include "pomai_cache/ai_cache.hpp"

#include <algorithm>
#include <chrono>
#include <regex>
#include <sstream>
#include <tuple>

namespace pomai_cache {
namespace {
std::optional<std::string> find_string(const std::string &json,
                                       const std::string &key) {
  std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
  std::smatch m;
  if (!std::regex_search(json, m, re))
    return std::nullopt;
  return m[1].str();
}
std::optional<std::uint64_t> find_u64(const std::string &json,
                                      const std::string &key) {
  std::regex re("\"" + key + "\"\\s*:\\s*([0-9]+)");
  std::smatch m;
  if (!std::regex_search(json, m, re))
    return std::nullopt;
  return static_cast<std::uint64_t>(std::stoull(m[1].str()));
}
double default_miss_cost(const std::string &type) {
  if (type == "embedding")
    return 8.0;
  if (type == "rerank_buffer")
    return 3.0;
  if (type == "response")
    return 4.0;
  if (type == "prompt")
    return 2.0;
  if (type == "rag_chunk")
    return 2.5;
  return 1.0;
}
} // namespace

std::string canonical_embedding_key(const std::string &model_id,
                                    const std::string &input_hash, int dim,
                                    const std::string &dtype) {
  return "emb:" + model_id + ":" + input_hash + ":" + std::to_string(dim) +
         ":" + dtype;
}
std::string canonical_prompt_key(const std::string &tokenizer_id,
                                 const std::string &prompt_hash) {
  return "prm:" + tokenizer_id + ":" + prompt_hash;
}
std::string canonical_rag_chunk_key(const std::string &source_id,
                                    const std::string &chunk_id,
                                    const std::string &rev) {
  return "rag:" + source_id + ":" + chunk_id + ":" + rev;
}
std::string canonical_rerank_key(const std::string &query_hash,
                                 const std::string &index_epoch, int topk,
                                 const std::string &params_hash) {
  return "rrk:" + query_hash + ":" + index_epoch + ":" + std::to_string(topk) +
         ":" + params_hash;
}
std::string canonical_response_key(const std::string &prompt_hash,
                                   const std::string &params_hash,
                                   const std::string &model_id) {
  return "rsp:" + prompt_hash + ":" + params_hash + ":" + model_id;
}

AiArtifactCache::AiArtifactCache(Engine &engine) : engine_(engine) {
  owner_ttl_defaults_["rerank"] = 5 * 60 * 1000ULL;
  owner_ttl_defaults_["response"] = 60 * 60 * 1000ULL;
  owner_ttl_defaults_["prompt"] = 24 * 60 * 60 * 1000ULL;
  owner_ttl_defaults_["vector"] = 7 * 24 * 60 * 60 * 1000ULL;
  owner_ttl_defaults_["rag"] = 6 * 60 * 60 * 1000ULL;
}

bool AiArtifactCache::parse_meta_json(const std::string &json,
                                      ArtifactMeta &out, std::string *err) {
  auto type = find_string(json, "artifact_type");
  auto owner = find_string(json, "owner");
  auto schema = find_string(json, "schema_version");
  if (!type || !owner || !schema) {
    if (err)
      *err = "meta_json missing required fields";
    return false;
  }
  out.artifact_type = *type;
  out.owner = *owner;
  out.schema_version = *schema;
  if (auto v = find_string(json, "model_id"))
    out.model_id = *v;
  if (auto v = find_string(json, "tokenizer_id"))
    out.tokenizer_id = *v;
  if (auto v = find_string(json, "dataset_id"))
    out.dataset_id = *v;
  if (auto v = find_string(json, "snapshot_epoch"))
    out.snapshot_epoch = *v;
  if (auto v = find_string(json, "source_rev"))
    out.source_rev = *v;
  if (auto v = find_string(json, "source_id"))
    out.source_id = *v;
  if (auto v = find_string(json, "chunk_id"))
    out.chunk_id = *v;
  if (auto v = find_u64(json, "created_at"))
    out.created_at_ms = *v;
  if (auto v = find_u64(json, "ttl_deadline"))
    out.ttl_ms = *v;
  if (auto v = find_u64(json, "size_bytes"))
    out.size_bytes = static_cast<std::size_t>(*v);
  if (auto v = find_string(json, "content_hash"))
    out.content_hash = *v;
  if (auto v = find_u64(json, "miss_cost"))
    out.miss_cost = static_cast<double>(*v);
  return true;
}

std::string AiArtifactCache::fast_hash_hex(const std::vector<std::uint8_t> &p) {
  std::uint64_t h = 1469598103934665603ULL;
  for (auto b : p) {
    h ^= static_cast<std::uint64_t>(b);
    h *= 1099511628211ULL;
  }
  std::ostringstream os;
  os << std::hex << h;
  return os.str();
}

std::string AiArtifactCache::meta_to_json(const ArtifactMeta &m) {
  std::ostringstream os;
  os << "{\"artifact_type\":\"" << m.artifact_type << "\",\"owner\":\""
     << m.owner << "\",\"schema_version\":\"" << m.schema_version
     << "\",\"model_id\":\"" << m.model_id
     << "\",\"created_at\":" << m.created_at_ms
     << ",\"ttl_deadline\":" << m.ttl_ms << ",\"size_bytes\":" << m.size_bytes
     << ",\"content_hash\":\"" << m.content_hash
     << "\",\"tenant\":\"local\",\"snapshot_epoch\":\"" << m.snapshot_epoch
     << "\",\"source_rev\":\"" << m.source_rev << "\"}";
  return os.str();
}

std::uint64_t AiArtifactCache::ttl_default_ms(const std::string &owner) const {
  auto it = owner_ttl_defaults_.find(owner);
  if (it == owner_ttl_defaults_.end())
    return 60 * 60 * 1000ULL;
  return it->second;
}

bool AiArtifactCache::put(const std::string &type, const std::string &key,
                          const std::string &meta_json,
                          const std::vector<std::uint8_t> &payload,
                          std::string *err) {
  ArtifactMeta meta;
  if (!parse_meta_json(meta_json, meta, err))
    return false;
  if (meta.artifact_type != type) {
    if (err)
      *err = "artifact type mismatch";
    return false;
  }
  const auto now_ms = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          Clock::now().time_since_epoch())
          .count());
  if (meta.created_at_ms == 0)
    meta.created_at_ms = now_ms;
  if (meta.ttl_ms == 0)
    meta.ttl_ms = ttl_default_ms(meta.owner);
  meta.size_bytes = payload.size();
  if (meta.content_hash.empty())
    meta.content_hash = fast_hash_hex(payload);
  if (meta.miss_cost <= 0)
    meta.miss_cost = default_miss_cost(type);

  const auto blob_key = "blob:" + meta.content_hash;
  std::optional<std::uint64_t> ttl_ms = meta.ttl_ms;

  if (key_index_.contains(key)) {
    auto prev = key_index_[key];
    deindex_key(key, prev);
    auto itb = blob_index_.find(prev.blob_hash);
    if (itb != blob_index_.end() && itb->second.refcount > 0)
      --itb->second.refcount;
  }

  std::string set_err;
  if (!engine_.set(blob_key, payload, ttl_ms, "vector", &set_err)) {
    if (err)
      *err = "blob put failed: " + set_err;
    return false;
  }
  std::vector<std::uint8_t> blob_ref(meta.content_hash.begin(),
                                     meta.content_hash.end());
  if (!engine_.set(key, blob_ref, ttl_ms, meta.owner, &set_err)) {
    if (err)
      *err = "key put failed: " + set_err;
    return false;
  }

  auto &bi = blob_index_[meta.content_hash];
  if (bi.refcount > 0)
    ++stats_.dedup_hits;
  bi.refcount += 1;
  bi.size_bytes = payload.size();

  auto &ki = key_index_[key];
  ki.meta = meta;
  ki.blob_hash = meta.content_hash;
  ki.explain = "admit:score>threshold owner=" + meta.owner +
               " type=" + meta.artifact_type;
  index_key(key, meta);

  ++stats_.puts;
  stats_.dedup_blobs = blob_index_.size();
  return true;
}

std::optional<ArtifactValue> AiArtifactCache::get(const std::string &key) {
  ++stats_.gets;
  auto it = key_index_.find(key);
  if (it == key_index_.end()) {
    ++stats_.misses;
    return std::nullopt;
  }
  auto ref = engine_.get(key);
  if (!ref.has_value()) {
    ++stats_.misses;
    return std::nullopt;
  }
  auto blob = engine_.get("blob:" + it->second.blob_hash);
  if (!blob.has_value()) {
    ++stats_.misses;
    return std::nullopt;
  }
  ++stats_.hits;
  ++it->second.hits;
  return ArtifactValue{it->second.meta, *blob};
}

std::vector<std::optional<ArtifactValue>>
AiArtifactCache::mget(const std::vector<std::string> &keys) {
  std::vector<std::optional<ArtifactValue>> out;
  out.reserve(keys.size());
  for (const auto &k : keys)
    out.push_back(get(k));
  return out;
}

void AiArtifactCache::index_key(const std::string &key,
                                const ArtifactMeta &meta) {
  if (!meta.snapshot_epoch.empty())
    epoch_index_[meta.snapshot_epoch].insert(key);
  if (!meta.model_id.empty())
    model_index_[meta.model_id].insert(key);
  for (std::size_t i = 1; i <= key.size() && i <= 32; ++i) {
    auto prefix = key.substr(0, i);
    auto &bucket = prefix_index_[prefix];
    if (bucket.size() < prefix_index_cap_)
      bucket.insert(key);
  }
}

void AiArtifactCache::deindex_key(const std::string &key, const KeyInfo &ki) {
  if (!ki.meta.snapshot_epoch.empty() &&
      epoch_index_.contains(ki.meta.snapshot_epoch))
    epoch_index_[ki.meta.snapshot_epoch].erase(key);
  if (!ki.meta.model_id.empty() && model_index_.contains(ki.meta.model_id))
    model_index_[ki.meta.model_id].erase(key);
  for (std::size_t i = 1; i <= key.size() && i <= 32; ++i) {
    auto prefix = key.substr(0, i);
    if (prefix_index_.contains(prefix))
      prefix_index_[prefix].erase(key);
  }
}

std::size_t
AiArtifactCache::invalidate_keys(const std::unordered_set<std::string> &keys) {
  std::size_t removed = 0;
  for (const auto &k : keys) {
    auto it = key_index_.find(k);
    if (it == key_index_.end())
      continue;
    auto old = it->second;
    deindex_key(k, old);
    auto bit = blob_index_.find(old.blob_hash);
    if (bit != blob_index_.end() && bit->second.refcount > 0) {
      --bit->second.refcount;
      if (bit->second.refcount == 0) {
        engine_.del({"blob:" + old.blob_hash});
        blob_index_.erase(bit);
      }
    }
    engine_.del({k});
    key_index_.erase(it);
    ++removed;
  }
  stats_.dedup_blobs = blob_index_.size();
  return removed;
}

std::size_t AiArtifactCache::invalidate_epoch(const std::string &epoch) {
  if (!epoch_index_.contains(epoch))
    return 0;
  auto keys = epoch_index_[epoch];
  epoch_index_.erase(epoch);
  return invalidate_keys(keys);
}

std::size_t AiArtifactCache::invalidate_model(const std::string &model_id) {
  if (!model_index_.contains(model_id))
    return 0;
  auto keys = model_index_[model_id];
  model_index_.erase(model_id);
  return invalidate_keys(keys);
}

std::size_t AiArtifactCache::invalidate_prefix(const std::string &prefix) {
  if (!prefix_index_.contains(prefix))
    return 0;
  auto keys = prefix_index_[prefix];
  prefix_index_.erase(prefix);
  return invalidate_keys(keys);
}

std::string AiArtifactCache::stats() const {
  std::ostringstream os;
  os << "puts:" << stats_.puts << "\n";
  os << "gets:" << stats_.gets << "\n";
  os << "hits:" << stats_.hits << "\n";
  os << "misses:" << stats_.misses << "\n";
  os << "dedup_hits:" << stats_.dedup_hits << "\n";
  os << "blob_count:" << blob_index_.size() << "\n";
  std::vector<std::tuple<std::string, std::uint64_t, std::uint64_t>> by_type;
  std::unordered_map<std::string, std::uint64_t> cnt;
  for (const auto &[k, v] : key_index_)
    cnt[v.meta.artifact_type]++;
  for (const auto &[k, v] : cnt)
    by_type.emplace_back(k, v, 0);
  std::sort(by_type.begin(), by_type.end());
  for (const auto &[t, c, _] : by_type)
    os << "type." << t << ":" << c << "\n";
  return os.str();
}

std::string AiArtifactCache::top_hot(std::size_t n) const {
  std::vector<std::pair<std::string, std::uint64_t>> rows;
  rows.reserve(key_index_.size());
  for (const auto &[k, v] : key_index_)
    rows.emplace_back(k, v.hits);
  std::sort(rows.begin(), rows.end(), [](const auto &a, const auto &b) {
    if (a.second == b.second)
      return a.first < b.first;
    return a.second > b.second;
  });
  std::ostringstream os;
  for (std::size_t i = 0; i < std::min(n, rows.size()); ++i)
    os << rows[i].first << ":" << rows[i].second << "\n";
  return os.str();
}

std::string AiArtifactCache::top_costly(std::size_t n) const {
  std::vector<std::pair<std::string, double>> rows;
  rows.reserve(key_index_.size());
  for (const auto &[k, v] : key_index_)
    rows.emplace_back(k, v.meta.miss_cost);
  std::sort(rows.begin(), rows.end(), [](const auto &a, const auto &b) {
    if (a.second == b.second)
      return a.first < b.first;
    return a.second > b.second;
  });
  std::ostringstream os;
  for (std::size_t i = 0; i < std::min(n, rows.size()); ++i)
    os << rows[i].first << ":" << rows[i].second << "\n";
  return os.str();
}

std::string AiArtifactCache::explain(const std::string &key) const {
  auto it = key_index_.find(key);
  if (it == key_index_.end())
    return "MISS:no metadata";
  return it->second.explain;
}

} // namespace pomai_cache
