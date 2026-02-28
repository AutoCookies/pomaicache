#include "pomai_cache/compression.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace pomai_cache {

namespace {

std::uint16_t float_to_half(float f) {
  std::uint32_t bits;
  std::memcpy(&bits, &f, 4);
  std::uint32_t sign = (bits >> 31) & 1;
  std::int32_t exp = static_cast<std::int32_t>((bits >> 23) & 0xFF) - 127;
  std::uint32_t mantissa = bits & 0x7FFFFF;

  if (exp > 15) {
    return static_cast<std::uint16_t>((sign << 15) | 0x7C00);
  }
  if (exp < -14) {
    return static_cast<std::uint16_t>(sign << 15);
  }
  std::uint16_t h_exp = static_cast<std::uint16_t>((exp + 15) & 0x1F);
  std::uint16_t h_man = static_cast<std::uint16_t>(mantissa >> 13);
  return static_cast<std::uint16_t>((sign << 15) | (h_exp << 10) | h_man);
}

float half_to_float(std::uint16_t h) {
  std::uint32_t sign = (h >> 15) & 1;
  std::uint32_t exp = (h >> 10) & 0x1F;
  std::uint32_t mantissa = h & 0x3FF;

  if (exp == 0 && mantissa == 0) {
    std::uint32_t bits = sign << 31;
    float f;
    std::memcpy(&f, &bits, 4);
    return f;
  }
  if (exp == 0x1F) {
    std::uint32_t bits = (sign << 31) | 0x7F800000 | (mantissa << 13);
    float f;
    std::memcpy(&f, &bits, 4);
    return f;
  }

  std::uint32_t f_exp = static_cast<std::uint32_t>(static_cast<int>(exp) - 15 + 127);
  std::uint32_t bits = (sign << 31) | (f_exp << 23) | (mantissa << 13);
  float f;
  std::memcpy(&f, &bits, 4);
  return f;
}

} // namespace

CompressedBlob
CompressionEngine::rle_compress(const std::vector<std::uint8_t> &input) {
  CompressedBlob blob;
  blob.type = CompressionType::RunLength;
  blob.original_size = input.size();
  if (input.empty())
    return blob;

  blob.data.reserve(input.size());
  std::size_t i = 0;
  while (i < input.size()) {
    std::uint8_t val = input[i];
    std::size_t run = 1;
    while (i + run < input.size() && input[i + run] == val && run < 255)
      ++run;
    blob.data.push_back(static_cast<std::uint8_t>(run));
    blob.data.push_back(val);
    i += run;
  }
  return blob;
}

std::vector<std::uint8_t>
CompressionEngine::rle_decompress(const CompressedBlob &blob) {
  std::vector<std::uint8_t> out;
  out.reserve(blob.original_size);
  for (std::size_t i = 0; i + 1 < blob.data.size(); i += 2) {
    std::uint8_t count = blob.data[i];
    std::uint8_t val = blob.data[i + 1];
    for (std::uint8_t j = 0; j < count; ++j)
      out.push_back(val);
  }
  return out;
}

CompressedBlob
CompressionEngine::delta_compress(const std::vector<std::uint8_t> &input) {
  CompressedBlob blob;
  blob.type = CompressionType::Delta;
  blob.original_size = input.size();
  if (input.empty())
    return blob;

  blob.data.reserve(input.size());
  blob.data.push_back(input[0]);
  for (std::size_t i = 1; i < input.size(); ++i)
    blob.data.push_back(
        static_cast<std::uint8_t>(input[i] - input[i - 1]));
  return blob;
}

std::vector<std::uint8_t>
CompressionEngine::delta_decompress(const CompressedBlob &blob) {
  std::vector<std::uint8_t> out;
  out.reserve(blob.original_size);
  if (blob.data.empty())
    return out;

  out.push_back(blob.data[0]);
  for (std::size_t i = 1; i < blob.data.size(); ++i)
    out.push_back(
        static_cast<std::uint8_t>(out.back() + blob.data[i]));
  return out;
}

CompressedBlob
CompressionEngine::compress(const std::vector<std::uint8_t> &input) {
  if (input.size() < 16) {
    CompressedBlob blob;
    blob.type = CompressionType::None;
    blob.original_size = input.size();
    blob.data = input;
    return blob;
  }

  auto rle = rle_compress(input);
  auto delta = delta_compress(input);

  if (rle.data.size() < delta.data.size() && rle.data.size() < input.size())
    return rle;
  if (delta.data.size() < input.size())
    return delta;

  CompressedBlob blob;
  blob.type = CompressionType::None;
  blob.original_size = input.size();
  blob.data = input;
  return blob;
}

std::vector<std::uint8_t>
CompressionEngine::decompress(const CompressedBlob &blob) {
  switch (blob.type) {
  case CompressionType::RunLength:
    return rle_decompress(blob);
  case CompressionType::Delta:
    return delta_decompress(blob);
  case CompressionType::None:
  case CompressionType::DictPrefix:
    return blob.data;
  }
  return blob.data;
}

double CompressionEngine::compression_ratio(const CompressedBlob &blob) {
  if (blob.original_size == 0)
    return 1.0;
  return static_cast<double>(blob.original_size) /
         static_cast<double>(blob.data.size());
}

std::vector<std::uint8_t>
CompressionEngine::quantize_f32_to_f16(const float *src, std::uint32_t count) {
  std::vector<std::uint8_t> out(count * 2);
  auto *dst = reinterpret_cast<std::uint16_t *>(out.data());
  for (std::uint32_t i = 0; i < count; ++i)
    dst[i] = float_to_half(src[i]);
  return out;
}

std::vector<float>
CompressionEngine::dequantize_f16_to_f32(const std::uint8_t *src,
                                          std::uint32_t count) {
  const auto *halves = reinterpret_cast<const std::uint16_t *>(src);
  std::vector<float> out(count);
  for (std::uint32_t i = 0; i < count; ++i)
    out[i] = half_to_float(halves[i]);
  return out;
}

std::vector<std::uint8_t>
CompressionEngine::quantize_f32_to_i8(const float *src, std::uint32_t count,
                                       float &scale, float &zero_point) {
  float vmin = *std::min_element(src, src + count);
  float vmax = *std::max_element(src, src + count);
  float range = vmax - vmin;
  if (range < 1e-12f)
    range = 1.0f;
  scale = range / 254.0f;
  zero_point = vmin;

  std::vector<std::uint8_t> out(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    float normalized = (src[i] - zero_point) / scale;
    int v = static_cast<int>(std::round(normalized));
    out[i] = static_cast<std::uint8_t>(std::clamp(v, 0, 255));
  }
  return out;
}

std::vector<float>
CompressionEngine::dequantize_i8_to_f32(const std::uint8_t *src,
                                         std::uint32_t count, float scale,
                                         float zero_point) {
  std::vector<float> out(count);
  for (std::uint32_t i = 0; i < count; ++i)
    out[i] = static_cast<float>(src[i]) * scale + zero_point;
  return out;
}

std::size_t CompressionEngine::shared_prefix_len(const std::string &a,
                                                  const std::string &b) {
  std::size_t n = std::min(a.size(), b.size());
  for (std::size_t i = 0; i < n; ++i) {
    if (a[i] != b[i])
      return i;
  }
  return n;
}

} // namespace pomai_cache
