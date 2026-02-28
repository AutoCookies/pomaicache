#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

namespace pomai_cache {

/// Bloom filter for probabilistic membership testing.
/// Inspired by Dragonfly's Bloom implementation, rewritten as a
/// zero-dependency Pomai Cache native data structure.
/// Uses double-hashing via FNV-1a to avoid external hash library deps.
class BloomFilter {
public:
  BloomFilter() = default;

  /// Construct a bloom filter sized for `expected_entries` with target
  /// false-positive probability `fp_prob` (must be in (0, 1)).
  BloomFilter(std::size_t expected_entries, double fp_prob);

  BloomFilter(const BloomFilter &) = default;
  BloomFilter &operator=(const BloomFilter &) = default;
  BloomFilter(BloomFilter &&) noexcept = default;
  BloomFilter &operator=(BloomFilter &&) noexcept = default;

  bool add(std::string_view item);
  bool maybe_contains(std::string_view item) const;

  std::size_t bit_count() const { return bits_.size() * 8; }
  std::size_t memory_bytes() const { return bits_.size(); }
  unsigned hash_count() const { return hash_cnt_; }
  void reset();

private:
  static constexpr double kLn2Sq = 0.4804530139182015; // ln(2)^2

  struct HashPair {
    std::uint64_t h1;
    std::uint64_t h2;
  };

  static HashPair compute_hash(std::string_view item);
  std::uint64_t nth_hash(std::uint64_t h1, std::uint64_t h2, unsigned i) const;

  std::vector<std::uint8_t> bits_;
  std::size_t num_bits_{0};
  unsigned hash_cnt_{0};
};

/// Scalable Bloom Filter: automatically grows when capacity is reached.
/// Based on the SBF paper (Almeida et al. 2007).
class ScalableBloomFilter {
public:
  ScalableBloomFilter(std::size_t initial_capacity, double fp_prob,
                      double grow_factor = 2.0);

  bool add(std::string_view item);
  bool maybe_contains(std::string_view item) const;
  std::size_t memory_bytes() const;
  std::size_t size() const { return current_size_; }

private:
  static constexpr double kErrorTighten = 0.5;
  std::vector<BloomFilter> filters_;
  double fp_prob_;
  double grow_factor_;
  std::size_t current_size_{0};
  std::size_t current_capacity_;
};

} // namespace pomai_cache
