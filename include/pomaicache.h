#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace pomaicache {

struct Config {
  std::size_t memory_limit_bytes{128 * 1024 * 1024};
  std::string data_dir{"./data"};
};

struct Ttl {
  std::uint64_t ms{0};
};

struct PromptResult {
  bool hit{false};
  std::uint64_t cached_tokens{0};
  std::uint64_t suffix_tokens{0};
  double savings_ratio{0.0};
};

class PomaiCacheImpl;

class PomaiCache {
public:
  explicit PomaiCache(const Config &cfg);
  ~PomaiCache();

  PomaiCache(const PomaiCache &) = delete;
  PomaiCache &operator=(const PomaiCache &) = delete;
  PomaiCache(PomaiCache &&) noexcept;
  PomaiCache &operator=(PomaiCache &&) noexcept;

  // Core K/V
  bool Set(std::string_view key,
           std::span<const std::byte> value,
           Ttl ttl);

  std::optional<std::vector<std::byte>> Get(std::string_view key);

  // AI Prompt Caching
  bool PromptPut(std::span<const std::uint64_t> tokens,
                 std::span<const std::byte> artifact,
                 Ttl ttl);

  PromptResult PromptGet(std::span<const std::uint64_t> tokens);

private:
  std::unique_ptr<PomaiCacheImpl> impl_;
};

} // namespace pomaicache

