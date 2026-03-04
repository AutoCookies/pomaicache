#pragma once

#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/journal.hpp"
#include "pomai_cache/prompt_cache.hpp"
#include <memory>
#include <vector>

/**
 * Inspired by DragonflyDB's `dfly::EngineShard` (src/server/engine_shard.h).
 * 
 * Performance Analysis:
 * 1. Cacheline Alignment: The alignas(64) ensures each shard starts on a new cacheline,
 *    preventing "False Sharing" where multiple cores fight over the same cacheline.
 * 2. Thread-Local Storage: Using `thread_local` pointers allows each core to access
 *    its own shard with zero global locking, achieving a "Share-Nothing" architecture.
 * 3. Zero-Lock Expiration: Since each shard is owned by exactly one thread, we can
 *    perform TTL cleanup and eviction without any mutexes.
 */

#include <mutex>

namespace pomai_cache {

class alignas(64) EngineShard {
public:
  EngineShard(std::uint32_t id, EngineConfig cfg,
              std::unique_ptr<IEvictionPolicy> policy,
              PromptCacheConfig prompt_cfg = {})
      : id_(id), engine_(std::move(cfg), std::move(policy)),
        ai_cache_(engine_), prompt_cache_(engine_, ai_cache_, prompt_cfg) {}

  // Forbidden copy/assignment to ensure memory stability
  EngineShard(const EngineShard&) = delete;
  EngineShard& operator=(const EngineShard&) = delete;

  std::uint32_t id() const { return id_; }
  Engine& engine() { return engine_; }
  Journal& journal() { return journal_; }
  AiArtifactCache& ai_cache() { return ai_cache_; }
  PromptCacheManager& prompt_cache() { return prompt_cache_; }

  static void InitThreadLocal(std::uint32_t id, EngineConfig cfg,
                              std::unique_ptr<IEvictionPolicy> policy,
                              PromptCacheConfig prompt_cfg = {}) {
    tlocal_shard_ =
        new EngineShard(id, std::move(cfg), std::move(policy), prompt_cfg);
  }

  static void DestroyThreadLocal() {
    delete tlocal_shard_;
    tlocal_shard_ = nullptr;
  }

  static EngineShard* tlocal() { return tlocal_shard_; }

private:
  std::uint32_t id_;
  Engine engine_;
  AiArtifactCache ai_cache_;
  PromptCacheManager prompt_cache_;
  Journal journal_;

  static inline thread_local EngineShard* tlocal_shard_{nullptr};
};

// Global shard set for multi-threaded access
class ShardSet {
public:
  static ShardSet& instance() {
    static ShardSet inst;
    return inst;
  }

  void add_shard(EngineShard* shard) {
    std::lock_guard lock(mu_);
    shards_.push_back(shard);
  }

  EngineShard* get_shard(const std::string& key) {
    std::lock_guard lock(mu_);
    if (shards_.empty()) return nullptr;
    // Basic consistent hashing to map keys to shards
    std::size_t hash = std::hash<std::string>{}(key);
    return shards_[hash % shards_.size()];
  }

  std::vector<EngineShard*> all_shards() {
    std::lock_guard lock(mu_);
    return shards_;
  }

private:
  std::mutex mu_;
  std::vector<EngineShard*> shards_;
};

} // namespace pomai_cache
