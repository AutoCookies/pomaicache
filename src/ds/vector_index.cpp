#include "pomai_cache/vector_index.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>

namespace pomai_cache {

VectorIndex::VectorIndex(std::uint32_t dim, DistanceMetric metric,
                         QuantLevel quant)
    : dim_(dim), metric_(metric), quant_(quant) {}

float VectorIndex::compute_norm(const float *v, std::uint32_t n) {
  return std::sqrt(dot_product(v, v, n));
}

float VectorIndex::dot_product(const float *a, const float *b, std::uint32_t n) {
  float sum = 0.0f;
  std::uint32_t i = 0;

#ifdef __SSE2__
  __m128 acc = _mm_setzero_ps();
  for (; i + 4 <= n; i += 4) {
    __m128 va = _mm_loadu_ps(a + i);
    __m128 vb = _mm_loadu_ps(b + i);
    acc = _mm_add_ps(acc, _mm_mul_ps(va, vb));
  }
  alignas(16) float tmp[4];
  _mm_store_ps(tmp, acc);
  sum = tmp[0] + tmp[1] + tmp[2] + tmp[3];
#endif

  for (; i < n; ++i)
    sum += a[i] * b[i];
  return sum;
}

float VectorIndex::cosine_similarity(const float *a, const float *b,
                                     std::uint32_t n) {
  float dot = dot_product(a, b, n);
  float na = std::sqrt(dot_product(a, a, n));
  float nb = std::sqrt(dot_product(b, b, n));
  if (na < 1e-12f || nb < 1e-12f)
    return 0.0f;
  return dot / (na * nb);
}

float VectorIndex::l2_distance(const float *a, const float *b, std::uint32_t n) {
  float sum = 0.0f;
  std::uint32_t i = 0;

#ifdef __SSE2__
  __m128 acc = _mm_setzero_ps();
  for (; i + 4 <= n; i += 4) {
    __m128 va = _mm_loadu_ps(a + i);
    __m128 vb = _mm_loadu_ps(b + i);
    __m128 diff = _mm_sub_ps(va, vb);
    acc = _mm_add_ps(acc, _mm_mul_ps(diff, diff));
  }
  alignas(16) float tmp[4];
  _mm_store_ps(tmp, acc);
  sum = tmp[0] + tmp[1] + tmp[2] + tmp[3];
#endif

  for (; i < n; ++i) {
    float d = a[i] - b[i];
    sum += d * d;
  }
  return std::sqrt(sum);
}

void VectorIndex::quantize_int8(const float *src, std::int8_t *dst,
                                std::uint32_t n, float &scale,
                                float &zero_point) {
  float vmin = *std::min_element(src, src + n);
  float vmax = *std::max_element(src, src + n);
  float range = vmax - vmin;
  if (range < 1e-12f)
    range = 1.0f;
  scale = range / 254.0f;
  zero_point = vmin;
  for (std::uint32_t i = 0; i < n; ++i) {
    float normalized = (src[i] - zero_point) / scale;
    int v = static_cast<int>(std::round(normalized)) - 127;
    dst[i] = static_cast<std::int8_t>(std::clamp(v, -128, 127));
  }
}

float VectorIndex::dot_product_int8(const std::int8_t *a, const std::int8_t *b,
                                    std::uint32_t n) {
  std::int32_t sum = 0;
  for (std::uint32_t i = 0; i < n; ++i)
    sum += static_cast<std::int32_t>(a[i]) * static_cast<std::int32_t>(b[i]);
  return static_cast<float>(sum);
}

bool VectorIndex::insert(const std::string &key, const float *data,
                         std::uint32_t len) {
  if (len != dim_)
    return false;

  auto it = std::find_if(entries_.begin(), entries_.end(),
                         [&](const VectorEntry &e) { return e.key == key; });
  if (it != entries_.end())
    entries_.erase(it);

  VectorEntry entry;
  entry.key = key;
  entry.norm = compute_norm(data, len);

  if (quant_ == QuantLevel::Int8) {
    entry.vec_i8.resize(len);
    quantize_int8(data, entry.vec_i8.data(), len, entry.scale, entry.zero_point);
    entry.vec_f32.assign(data, data + len);
  } else {
    entry.vec_f32.assign(data, data + len);
  }

  entries_.push_back(std::move(entry));
  return true;
}

bool VectorIndex::remove(const std::string &key) {
  auto it = std::find_if(entries_.begin(), entries_.end(),
                         [&](const VectorEntry &e) { return e.key == key; });
  if (it == entries_.end())
    return false;
  entries_.erase(it);
  return true;
}

float VectorIndex::compute_score(const VectorEntry &entry, const float *query,
                                 float query_norm) const {
  switch (metric_) {
  case DistanceMetric::Cosine: {
    float dot = dot_product(entry.vec_f32.data(), query, dim_);
    if (entry.norm < 1e-12f || query_norm < 1e-12f)
      return 0.0f;
    return dot / (entry.norm * query_norm);
  }
  case DistanceMetric::L2:
    return -l2_distance(entry.vec_f32.data(), query, dim_);
  case DistanceMetric::DotProduct:
    return dot_product(entry.vec_f32.data(), query, dim_);
  }
  return 0.0f;
}

std::vector<SimResult> VectorIndex::search(const float *query, std::uint32_t len,
                                           std::size_t top_k,
                                           float threshold) const {
  if (len != dim_ || entries_.empty())
    return {};

  float query_norm = compute_norm(query, len);

  std::vector<SimResult> candidates;
  candidates.reserve(entries_.size());

  for (const auto &entry : entries_) {
    float score = compute_score(entry, query, query_norm);
    if (metric_ == DistanceMetric::L2) {
      if (-score <= threshold)
        candidates.push_back({entry.key, score});
    } else {
      if (score >= threshold)
        candidates.push_back({entry.key, score});
    }
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const SimResult &a, const SimResult &b) {
              return a.score > b.score;
            });

  if (candidates.size() > top_k)
    candidates.resize(top_k);

  return candidates;
}

std::size_t VectorIndex::memory_bytes() const {
  std::size_t total = sizeof(*this);
  for (const auto &e : entries_) {
    total += sizeof(VectorEntry) + e.key.size();
    total += e.vec_f32.size() * sizeof(float);
    total += e.vec_i8.size() * sizeof(std::int8_t);
  }
  return total;
}

} // namespace pomai_cache
