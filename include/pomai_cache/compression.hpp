#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace pomai_cache {

enum class CompressionType : std::uint8_t { None, RunLength, Delta, DictPrefix };

struct CompressedBlob {
  CompressionType type{CompressionType::None};
  std::vector<std::uint8_t> data;
  std::size_t original_size{0};
};

class CompressionEngine {
public:
  static CompressedBlob compress(const std::vector<std::uint8_t> &input);
  static std::vector<std::uint8_t> decompress(const CompressedBlob &blob);

  static CompressedBlob rle_compress(const std::vector<std::uint8_t> &input);
  static std::vector<std::uint8_t> rle_decompress(const CompressedBlob &blob);

  static CompressedBlob delta_compress(const std::vector<std::uint8_t> &input);
  static std::vector<std::uint8_t> delta_decompress(const CompressedBlob &blob);

  static double compression_ratio(const CompressedBlob &blob);

  /// Quantize float32 embeddings to float16 (stored as uint16).
  static std::vector<std::uint8_t>
  quantize_f32_to_f16(const float *src, std::uint32_t count);

  /// Dequantize float16 back to float32.
  static std::vector<float> dequantize_f16_to_f32(const std::uint8_t *src,
                                                   std::uint32_t count);

  /// Quantize float32 embeddings to int8.
  static std::vector<std::uint8_t>
  quantize_f32_to_i8(const float *src, std::uint32_t count, float &scale,
                     float &zero_point);

  /// Dequantize int8 back to float32.
  static std::vector<float> dequantize_i8_to_f32(const std::uint8_t *src,
                                                  std::uint32_t count,
                                                  float scale,
                                                  float zero_point);

  /// Compute prefix-shared storage for a set of prompts.
  static std::size_t shared_prefix_len(const std::string &a,
                                       const std::string &b);
};

} // namespace pomai_cache
