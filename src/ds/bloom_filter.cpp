#include "pomai_cache/bloom_filter.hpp"

#include <algorithm>
#include <bit>

namespace pomai_cache {

namespace {

std::uint64_t fnv1a_64(const char *data, std::size_t len, std::uint64_t seed) {
  std::uint64_t h = 14695981039346656037ULL ^ seed;
  for (std::size_t i = 0; i < len; ++i) {
    h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(data[i]));
    h *= 1099511628211ULL;
  }
  return h;
}

std::size_t next_power_of_two(std::size_t v) {
  if (v == 0)
    return 1;
  return std::bit_ceil(v);
}

} // namespace

BloomFilter::HashPair BloomFilter::compute_hash(std::string_view item) {
  return {fnv1a_64(item.data(), item.size(), 0xc6a4a7935bd1e995ULL),
          fnv1a_64(item.data(), item.size(), 0x9ae16a3b2f90404fULL)};
}

std::uint64_t BloomFilter::nth_hash(std::uint64_t h1, std::uint64_t h2,
                                    unsigned i) const {
  return (h1 + h2 * i) % num_bits_;
}

BloomFilter::BloomFilter(std::size_t expected_entries, double fp_prob) {
  if (fp_prob <= 0.0 || fp_prob >= 1.0)
    fp_prob = 0.01;
  if (fp_prob > 0.5)
    fp_prob = 0.5;

  double bpe = -std::log(fp_prob) / kLn2Sq;
  hash_cnt_ = static_cast<unsigned>(std::ceil(std::log(2.0) * bpe));
  if (hash_cnt_ < 1)
    hash_cnt_ = 1;

  std::size_t raw_bits = static_cast<std::size_t>(
      std::ceil(static_cast<double>(expected_entries) * bpe));
  if (raw_bits < 64)
    raw_bits = 64;
  num_bits_ = next_power_of_two(raw_bits);

  bits_.assign(num_bits_ / 8, 0);
}

bool BloomFilter::add(std::string_view item) {
  auto [h1, h2] = compute_hash(item);
  bool changed = false;
  for (unsigned i = 0; i < hash_cnt_; ++i) {
    auto idx = nth_hash(h1, h2, i);
    auto byte_idx = idx / 8;
    auto bit_idx = idx % 8;
    auto old = bits_[byte_idx];
    bits_[byte_idx] |= (1u << bit_idx);
    if (bits_[byte_idx] != old)
      changed = true;
  }
  return changed;
}

bool BloomFilter::maybe_contains(std::string_view item) const {
  if (num_bits_ == 0)
    return false;
  auto [h1, h2] = compute_hash(item);
  for (unsigned i = 0; i < hash_cnt_; ++i) {
    auto idx = nth_hash(h1, h2, i);
    auto byte_idx = idx / 8;
    auto bit_idx = idx % 8;
    if ((bits_[byte_idx] & (1u << bit_idx)) == 0)
      return false;
  }
  return true;
}

void BloomFilter::reset() { std::fill(bits_.begin(), bits_.end(), 0); }

// --- ScalableBloomFilter ---

ScalableBloomFilter::ScalableBloomFilter(std::size_t initial_capacity,
                                         double fp_prob, double grow_factor)
    : fp_prob_(fp_prob * kErrorTighten), grow_factor_(grow_factor),
      current_capacity_(initial_capacity) {
  filters_.emplace_back(initial_capacity, fp_prob_);
}

bool ScalableBloomFilter::add(std::string_view item) {
  for (auto it = filters_.rbegin(); it != filters_.rend(); ++it) {
    if (it->maybe_contains(item))
      return false;
  }

  if (!filters_.back().add(item))
    return false;

  ++current_size_;

  std::size_t cap = current_capacity_;
  if (current_size_ >= cap) {
    fp_prob_ *= kErrorTighten;
    current_capacity_ =
        static_cast<std::size_t>(static_cast<double>(cap) * grow_factor_);
    filters_.emplace_back(current_capacity_, fp_prob_);
    current_size_ = 0;
  }

  return true;
}

bool ScalableBloomFilter::maybe_contains(std::string_view item) const {
  for (auto it = filters_.rbegin(); it != filters_.rend(); ++it) {
    if (it->maybe_contains(item))
      return true;
  }
  return false;
}

std::size_t ScalableBloomFilter::memory_bytes() const {
  std::size_t total = sizeof(*this);
  for (const auto &f : filters_)
    total += f.memory_bytes();
  return total;
}

} // namespace pomai_cache
