#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace pomai_cache {

using Clock = std::chrono::system_clock;
using TimePoint = Clock::time_point;

struct Entry {
  std::vector<std::uint8_t> value;
  std::size_t size_bytes{0};
  TimePoint created_at{};
  TimePoint last_access{};
  std::uint64_t hit_count{0};
  std::optional<TimePoint> ttl_deadline;
  std::string owner{"default"};
};

} // namespace pomai_cache
