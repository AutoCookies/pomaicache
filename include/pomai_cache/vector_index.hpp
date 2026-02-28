#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#ifdef __SSE2__
#include <immintrin.h>
#endif

namespace pomai_cache {

enum class DistanceMetric : std::uint8_t { Cosine, L2, DotProduct };

enum class QuantLevel : std::uint8_t { Float32, Float16, Int8 };

struct VectorEntry {
  std::string key;
  std::vector<float> vec_f32;
  std::vector<std::int8_t> vec_i8;
  float norm{0.0f};
  float scale{1.0f};
  float zero_point{0.0f};
};

struct SimResult {
  std::string key;
  float score;
};

class VectorIndex {
public:
  explicit VectorIndex(std::uint32_t dim, DistanceMetric metric = DistanceMetric::Cosine,
                       QuantLevel quant = QuantLevel::Float32);

  bool insert(const std::string &key, const float *data, std::uint32_t len);
  bool remove(const std::string &key);

  std::vector<SimResult> search(const float *query, std::uint32_t len,
                                std::size_t top_k, float threshold) const;

  std::size_t size() const { return entries_.size(); }
  std::uint32_t dim() const { return dim_; }
  std::size_t memory_bytes() const;
  QuantLevel quant_level() const { return quant_; }

  static float cosine_similarity(const float *a, const float *b, std::uint32_t n);
  static float l2_distance(const float *a, const float *b, std::uint32_t n);
  static float dot_product(const float *a, const float *b, std::uint32_t n);

  static void quantize_int8(const float *src, std::int8_t *dst, std::uint32_t n,
                            float &scale, float &zero_point);
  static float dot_product_int8(const std::int8_t *a, const std::int8_t *b,
                                std::uint32_t n);

private:
  float compute_score(const VectorEntry &entry, const float *query,
                      float query_norm) const;
  static float compute_norm(const float *v, std::uint32_t n);

  std::uint32_t dim_;
  DistanceMetric metric_;
  QuantLevel quant_;
  std::vector<VectorEntry> entries_;
};

} // namespace pomai_cache
