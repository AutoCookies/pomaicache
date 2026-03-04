#include "pomaicache.h"

#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/prompt_cache.hpp"

#include <cstring>

namespace pomaicache {

class PomaiCacheImpl {
public:
  explicit PomaiCacheImpl(const Config &cfg)
      : engine_cfg_(),
        policy_(pomai_cache::make_policy_by_name("pomai_cost")),
        engine_(engine_cfg_, std::move(policy_)),
        ai_cache_(engine_),
        prompt_cfg_(),
        prompt_cache_(engine_, ai_cache_, prompt_cfg_) {
    engine_cfg_.memory_limit_bytes = cfg.memory_limit_bytes;
    engine_cfg_.data_dir = cfg.data_dir;
  }

  bool set(std::string_view key,
           std::span<const std::byte> value,
           Ttl ttl) {
    std::vector<std::uint8_t> v(value.size());
    std::memcpy(v.data(), value.data(), value.size());
    std::optional<std::uint64_t> ttl_ms;
    if (ttl.ms > 0)
      ttl_ms = ttl.ms;
    std::string err;
    return engine_.set(std::string(key), v, ttl_ms, "default", &err);
  }

  std::optional<std::vector<std::byte>> get(std::string_view key) {
    auto v = engine_.get(std::string(key));
    if (!v.has_value())
      return std::nullopt;
    std::vector<std::byte> out(v->size());
    std::memcpy(out.data(), v->data(), v->size());
    return out;
  }

  bool prompt_put(std::span<const std::uint64_t> tokens,
                  std::span<const std::byte> artifact,
                  Ttl ttl) {
    if (tokens.empty())
      return false;
    std::vector<std::uint8_t> serialized(artifact.size());
    std::memcpy(serialized.data(), artifact.data(), artifact.size());
    std::vector<std::uint8_t> token_bytes(tokens.size() * sizeof(std::uint64_t));
    std::memcpy(token_bytes.data(), tokens.data(), token_bytes.size());
    const auto hash = pomai_cache::AiArtifactCache::fast_hash_hex(token_bytes);
    std::optional<std::uint64_t> ttl_ms;
    if (ttl.ms > 0)
      ttl_ms = ttl.ms;
    std::string err;
    return prompt_cache_.put_prefix("tok", hash, serialized,
                                    static_cast<std::uint64_t>(tokens.size()),
                                    ttl_ms, &err);
  }

  PromptResult prompt_get(std::span<const std::uint64_t> tokens) {
    PromptResult r;
    if (tokens.empty())
      return r;
    std::vector<std::uint8_t> token_bytes(tokens.size() * sizeof(std::uint64_t));
    std::memcpy(token_bytes.data(), tokens.data(), token_bytes.size());
    const auto hash = pomai_cache::AiArtifactCache::fast_hash_hex(token_bytes);
    auto reuse = prompt_cache_.reuse_for_query("tok", hash, token_bytes);
    r.hit = reuse.hit;
    r.cached_tokens = reuse.cached_tokens;
    r.suffix_tokens = reuse.suffix_tokens;
    r.savings_ratio = reuse.savings_ratio;
    return r;
  }

private:
  pomai_cache::EngineConfig engine_cfg_;
  std::unique_ptr<pomai_cache::IEvictionPolicy> policy_;
  pomai_cache::Engine engine_;
  pomai_cache::AiArtifactCache ai_cache_;
  pomai_cache::PromptCacheConfig prompt_cfg_;
  pomai_cache::PromptCacheManager prompt_cache_;
};

PomaiCache::PomaiCache(const Config &cfg)
    : impl_(std::make_unique<PomaiCacheImpl>(cfg)) {}

PomaiCache::~PomaiCache() = default;

PomaiCache::PomaiCache(PomaiCache &&) noexcept = default;
PomaiCache &PomaiCache::operator=(PomaiCache &&) noexcept = default;

bool PomaiCache::Set(std::string_view key,
                     std::span<const std::byte> value,
                     Ttl ttl) {
  return impl_->set(key, value, ttl);
}

std::optional<std::vector<std::byte>> PomaiCache::Get(std::string_view key) {
  return impl_->get(key);
}

bool PomaiCache::PromptPut(std::span<const std::uint64_t> tokens,
                           std::span<const std::byte> artifact,
                           Ttl ttl) {
  return impl_->prompt_put(tokens, artifact, ttl);
}

PromptResult PomaiCache::PromptGet(std::span<const std::uint64_t> tokens) {
  return impl_->prompt_get(tokens);
}

} // namespace pomaicache

