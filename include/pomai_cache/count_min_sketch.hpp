#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

namespace pomai_cache {

/// Count-Min Sketch for space-efficient frequency estimation.
/// Inspired by Dragonfly's CountMinSketch, rewritten as a
/// zero-dependency Pomai Cache native data structure.
class CountMinSketch {
public:
  using CountT = std::uint16_t;

  /// epsilon: error factor per element.  f_est <= f_actual + epsilon * N
  /// delta:   failure probability (all rows disagree)
  explicit CountMinSketch(double epsilon = 0.0001, double delta = 0.0001);

  CountMinSketch(const CountMinSketch &) = delete;
  CountMinSketch &operator=(const CountMinSketch &) = delete;
  CountMinSketch(CountMinSketch &&) noexcept = default;
  CountMinSketch &operator=(CountMinSketch &&) noexcept = default;

  void increment(std::uint64_t key, CountT amount = 1);
  CountT estimate(std::uint64_t key) const;
  void reset();

  std::size_t width() const { return width_; }
  std::size_t depth() const { return depth_; }
  std::size_t memory_bytes() const;

private:
  std::uint64_t hash(std::uint64_t key, std::uint64_t seed) const;

  std::vector<std::vector<CountT>> counters_;
  std::size_t width_;
  std::size_t depth_;
};

/// Multi-window sketch: maintains 3 sketches with time-based decay.
/// Estimates naturally decay as old sketches roll over.
class MultiWindowSketch {
public:
  enum class DecayMode : std::uint8_t { Exponential, Linear, SlidingWindow };

  explicit MultiWindowSketch(std::uint64_t rollover_ms = 1000,
                             double epsilon = 0.0001, double delta = 0.0001,
                             DecayMode decay = DecayMode::Linear);

  void increment(std::uint64_t key, CountMinSketch::CountT amount = 1);
  CountMinSketch::CountT estimate(std::uint64_t key) const;

private:
  static std::uint64_t now_ms();
  void maybe_rollover();

  struct Window {
    CountMinSketch sketch;
    std::uint64_t start_time{0};
  };

  std::array<Window, 3> windows_;
  std::uint64_t rollover_ms_;
  std::size_t current_{2};
  std::uint64_t check_counter_{0};
  static constexpr std::uint64_t kCheckInterval = 512;
  DecayMode decay_;
};

} // namespace pomai_cache
