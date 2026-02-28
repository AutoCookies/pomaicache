#include "pomai_cache/count_min_sketch.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>

namespace pomai_cache {

namespace {
constexpr auto kMaxCount = std::numeric_limits<CountMinSketch::CountT>::max();

std::uint64_t fnv1a_u64(const void *data, std::size_t len,
                        std::uint64_t seed) {
  std::uint64_t h = 14695981039346656037ULL ^ seed;
  auto bytes = static_cast<const unsigned char *>(data);
  for (std::size_t i = 0; i < len; ++i) {
    h ^= bytes[i];
    h *= 1099511628211ULL;
  }
  return h;
}

std::uint64_t decay_exponential(std::uint64_t value, std::int64_t dt_ms) {
  static constexpr double kHalfLifeConst = 0.000138629; // ln(2)/5000
  return static_cast<std::uint64_t>(
      static_cast<double>(value) * std::exp(-dt_ms * kHalfLifeConst));
}

std::uint64_t decay_linear(std::uint64_t value, std::int64_t dt_ms) {
  static constexpr double kRate = 0.001; // -1 per 1000ms
  double dec = dt_ms * kRate;
  return static_cast<std::uint64_t>(
      static_cast<double>(value) -
      std::min(static_cast<double>(value), dec));
}

} // namespace

CountMinSketch::CountMinSketch(double epsilon, double delta) {
  width_ = static_cast<std::size_t>(std::ceil(std::exp(1.0) / epsilon));
  depth_ = static_cast<std::size_t>(std::ceil(std::log(1.0 / delta)));
  if (width_ < 8)
    width_ = 8;
  if (depth_ < 2)
    depth_ = 2;
  counters_.reserve(depth_);
  for (std::size_t i = 0; i < depth_; ++i)
    counters_.emplace_back(width_, CountT{0});
}

std::uint64_t CountMinSketch::hash(std::uint64_t key,
                                   std::uint64_t seed) const {
  return fnv1a_u64(&key, sizeof(key), seed) % width_;
}

void CountMinSketch::increment(std::uint64_t key, CountT amount) {
  for (std::size_t i = 0; i < depth_; ++i) {
    auto idx = hash(key, i);
    auto &cell = counters_[i][idx];
    CountT next = cell + amount;
    cell = (next < cell) ? kMaxCount : next; // saturating add
  }
}

CountMinSketch::CountT CountMinSketch::estimate(std::uint64_t key) const {
  CountT min_val = kMaxCount;
  for (std::size_t i = 0; i < depth_; ++i) {
    min_val = std::min(min_val, counters_[i][hash(key, i)]);
  }
  return min_val;
}

void CountMinSketch::reset() {
  for (auto &row : counters_)
    std::fill(row.begin(), row.end(), CountT{0});
}

std::size_t CountMinSketch::memory_bytes() const {
  return sizeof(*this) + depth_ * width_ * sizeof(CountT);
}

// --- MultiWindowSketch ---

std::uint64_t MultiWindowSketch::now_ms() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

MultiWindowSketch::MultiWindowSketch(std::uint64_t rollover_ms, double epsilon,
                                     double delta, DecayMode decay)
    : rollover_ms_(rollover_ms), decay_(decay) {
  auto t = now_ms();
  for (auto &w : windows_) {
    w.sketch = CountMinSketch{epsilon, delta};
    w.start_time = t;
  }
}

void MultiWindowSketch::increment(std::uint64_t key,
                                  CountMinSketch::CountT amount) {
  if (++check_counter_ >= kCheckInterval) {
    maybe_rollover();
    check_counter_ = 0;
  }
  windows_[current_].sketch.increment(key, amount);
}

CountMinSketch::CountT
MultiWindowSketch::estimate(std::uint64_t key) const {
  auto t = now_ms();
  std::uint64_t total = 0;
  for (const auto &w : windows_) {
    auto e = w.sketch.estimate(key);
    auto dt = static_cast<std::int64_t>(t - w.start_time);
    switch (decay_) {
    case DecayMode::Exponential:
      total += decay_exponential(e, dt);
      break;
    case DecayMode::Linear:
      total += decay_linear(e, dt);
      break;
    case DecayMode::SlidingWindow:
      total += e;
      break;
    }
  }
  return static_cast<CountMinSketch::CountT>(
      std::min<std::uint64_t>(total, kMaxCount));
}

void MultiWindowSketch::maybe_rollover() {
  auto t = now_ms();
  auto oldest = (current_ + 1) % windows_.size();
  if (t - windows_[oldest].start_time > rollover_ms_) {
    windows_[oldest].sketch.reset();
    windows_[oldest].start_time = t;
    current_ = oldest;
  }
}

} // namespace pomai_cache
