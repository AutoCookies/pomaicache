#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/compression.hpp"
#include "pomai_cache/dep_graph.hpp"
#include "pomai_cache/vector_index.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cmath>

using namespace pomai_cache;

TEST_CASE("AI canonical keys deterministic", "[ai][keys]") {
  REQUIRE(canonical_embedding_key("m1", "h1", 768, "float16") ==
          "emb:m1:h1:768:float16");
  REQUIRE(canonical_prompt_key("tok", "p") == "prm:tok:p");
  REQUIRE(canonical_rag_chunk_key("src", "c1", "r2") == "rag:src:c1:r2");
  REQUIRE(canonical_rerank_key("q", "e", 20, "ph") == "rrk:q:e:20:ph");
  REQUIRE(canonical_response_key("p", "par", "m") == "rsp:p:par:m");
}

TEST_CASE("AI PUT/GET roundtrip and dedup", "[ai][roundtrip]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string meta =
      "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_"
      "version\":\"v1\",\"model_id\":\"m\",\"snapshot_epoch\":\"ep1\"}";
  std::vector<std::uint8_t> payload{1, 2, 3, 4};
  REQUIRE(ai.put("embedding", "k1", meta, payload));
  REQUIRE(ai.put("embedding", "k2", meta, payload));

  auto got = ai.get("k1");
  REQUIRE(got.has_value());
  REQUIRE(got->payload == payload);

  auto stats = ai.stats();
  REQUIRE(stats.find("dedup_hits:1") != std::string::npos);
}

TEST_CASE("AI invalidation by epoch and model", "[ai][invalidate]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<std::uint8_t> payload{9, 8, 7};
  REQUIRE(
      ai.put("response", "rsp:1",
             "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_"
             "version\":\"v1\",\"model_id\":\"m2\",\"snapshot_epoch\":\"e2\"}",
             payload));
  REQUIRE(
      ai.put("response", "rsp:2",
             "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_"
             "version\":\"v1\",\"model_id\":\"m3\",\"snapshot_epoch\":\"e3\"}",
             payload));

  REQUIRE(ai.invalidate_epoch("e2") == 1);
  REQUIRE(!ai.get("rsp:1").has_value());
  REQUIRE(ai.get("rsp:2").has_value());
  REQUIRE(ai.invalidate_model("m3") == 1);
  REQUIRE(!ai.get("rsp:2").has_value());
}

TEST_CASE("AI meta and caps validation", "[ai][adversarial]") {
  Engine e({1024 * 1024, 256, 8}, make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<std::uint8_t> payload(32, 1);
  std::string err;
  REQUIRE(!ai.put("embedding", "k", "{\"owner\":\"vector\"}", payload, &err));
  REQUIRE(err.find("missing") != std::string::npos);

  REQUIRE(!ai.put("embedding", "k",
                  "{\"artifact_type\":\"embedding\",\"owner\":\"vector\","
                  "\"schema_version\":\"v1\"}",
                  payload, &err));
  REQUIRE(err.find("blob put failed") != std::string::npos);
}

// ===== Feature 1: Semantic Similarity =====

TEST_CASE("VectorIndex cosine similarity search", "[vector][sim]") {
  VectorIndex idx(3, DistanceMetric::Cosine);
  std::vector<float> v1{1.0f, 0.0f, 0.0f};
  std::vector<float> v2{0.0f, 1.0f, 0.0f};
  std::vector<float> v3{0.9f, 0.1f, 0.0f};

  REQUIRE(idx.insert("a", v1.data(), 3));
  REQUIRE(idx.insert("b", v2.data(), 3));
  REQUIRE(idx.insert("c", v3.data(), 3));
  CHECK(idx.size() == 3);

  auto results = idx.search(v1.data(), 3, 2, 0.5f);
  REQUIRE(results.size() >= 1);
  CHECK(results[0].key == "a");
  CHECK(results[0].score > 0.99f);
  if (results.size() > 1)
    CHECK(results[1].key == "c");

  REQUIRE(idx.remove("a"));
  CHECK(idx.size() == 2);
}

TEST_CASE("VectorIndex L2 distance search", "[vector][l2]") {
  VectorIndex idx(2, DistanceMetric::L2);
  std::vector<float> v1{0.0f, 0.0f};
  std::vector<float> v2{1.0f, 1.0f};
  REQUIRE(idx.insert("origin", v1.data(), 2));
  REQUIRE(idx.insert("far", v2.data(), 2));

  auto results = idx.search(v1.data(), 2, 2, 2.0f);
  REQUIRE(results.size() == 2);
  CHECK(results[0].key == "origin");
}

TEST_CASE("VectorIndex quantization int8", "[vector][quant]") {
  std::vector<float> src{0.1f, 0.5f, 0.9f, -0.3f};
  float scale, zp;
  std::vector<std::int8_t> dst(4);
  VectorIndex::quantize_int8(src.data(), dst.data(), 4, scale, zp);

  for (std::size_t i = 0; i < 4; ++i) {
    float approx = static_cast<float>(dst[i] + 127) * scale + zp;
    CHECK(std::abs(approx - src[i]) < 0.02f);
  }
}

TEST_CASE("AI.SIM.PUT and AI.SIM.GET integration", "[ai][sim]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<float> v1{1.0f, 0.0f, 0.0f};
  std::vector<float> v2{0.9f, 0.1f, 0.0f};
  std::vector<uint8_t> p1{1, 2, 3};
  std::vector<uint8_t> p2{4, 5, 6};

  std::string meta_json =
      "{\"artifact_type\":\"embedding\",\"owner\":\"vector\","
      "\"schema_version\":\"v1\",\"model_id\":\"m1\"}";

  REQUIRE(ai.sim_put("sim:a", v1, p1, meta_json));
  REQUIRE(ai.sim_put("sim:b", v2, p2, meta_json));

  auto results = ai.sim_get(v1, 2, 0.8f);
  REQUIRE(results.size() >= 1);
  CHECK(results[0].key == "sim:a");
  CHECK(results[0].score > 0.99f);
}

// ===== Feature 2: Token Economics =====

TEST_CASE("Token economics tracks dollar cost on hits", "[ai][cost]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string meta =
      "{\"artifact_type\":\"response\",\"owner\":\"response\","
      "\"schema_version\":\"v1\",\"model_id\":\"gpt-4\","
      "\"inference_tokens\":500,\"dollar_cost\":0.05,"
      "\"inference_latency_ms\":1200}";
  std::vector<uint8_t> payload{1, 2, 3};
  REQUIRE(ai.put("response", "rsp:cost1", meta, payload));

  ai.get("rsp:cost1");
  ai.get("rsp:cost1");

  auto report = ai.cost_report();
  CHECK(report.total_dollar_saved > 0.09);
  CHECK(report.total_tokens_saved == 1000);
  CHECK(report.total_latency_saved_ms == 2400);
}

TEST_CASE("Budget setting and report", "[ai][budget]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  ai.set_budget(10.0);
  CHECK(ai.budget() > 9.9);

  auto stats = ai.stats();
  CHECK(stats.find("budget_dollar_per_hour:") != std::string::npos);
}

// ===== Feature 3: Pipeline Cascade =====

TEST_CASE("DepGraph cascade invalidation", "[depgraph]") {
  DepGraph g;
  g.add_edge("embed", "retrieve");
  g.add_edge("retrieve", "rerank");
  g.add_edge("rerank", "response");

  auto desc = g.descendants("embed");
  CHECK(desc.size() == 3);
  CHECK(desc.count("retrieve") == 1);
  CHECK(desc.count("rerank") == 1);
  CHECK(desc.count("response") == 1);

  auto parents = g.parents("response");
  CHECK(parents.size() == 1);

  g.remove_node("rerank");
  auto desc2 = g.descendants("embed");
  CHECK(desc2.count("rerank") == 0);
  CHECK(desc2.count("response") == 0);
}

TEST_CASE("AI put_with_deps and cascade invalidation", "[ai][cascade]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<uint8_t> payload{1, 2, 3};
  std::string meta_emb =
      "{\"artifact_type\":\"embedding\",\"owner\":\"vector\","
      "\"schema_version\":\"v1\",\"model_id\":\"m1\",\"snapshot_epoch\":\"ep1\"}";
  std::string meta_rsp =
      "{\"artifact_type\":\"response\",\"owner\":\"response\","
      "\"schema_version\":\"v1\",\"model_id\":\"m1\"}";

  REQUIRE(ai.put("embedding", "emb:1", meta_emb, payload));
  REQUIRE(ai.put_with_deps("response", "rsp:1", meta_rsp, payload, {"emb:1"}));
  REQUIRE(ai.get("rsp:1").has_value());

  auto removed = ai.invalidate_cascade("emb:1");
  CHECK(removed >= 2);
  CHECK_FALSE(ai.get("emb:1").has_value());
  CHECK_FALSE(ai.get("rsp:1").has_value());
}

// ===== Feature 4: Compression =====

TEST_CASE("RLE compression roundtrip", "[compression][rle]") {
  std::vector<uint8_t> input(100, 42);
  auto compressed = CompressionEngine::rle_compress(input);
  auto decompressed = CompressionEngine::rle_decompress(compressed);
  CHECK(decompressed == input);
  CHECK(CompressionEngine::compression_ratio(compressed) > 1.0);
}

TEST_CASE("Delta compression roundtrip", "[compression][delta]") {
  std::vector<uint8_t> input{10, 11, 12, 13, 14, 15, 16};
  auto compressed = CompressionEngine::delta_compress(input);
  auto decompressed = CompressionEngine::delta_decompress(compressed);
  CHECK(decompressed == input);
}

TEST_CASE("Auto compression picks best method", "[compression][auto]") {
  std::vector<uint8_t> repetitive(200, 0xAA);
  auto blob = CompressionEngine::compress(repetitive);
  CHECK(blob.data.size() < repetitive.size());

  auto recovered = CompressionEngine::decompress(blob);
  CHECK(recovered == repetitive);
}

TEST_CASE("Float32 to float16 quantization roundtrip", "[compression][f16]") {
  std::vector<float> src{1.0f, -0.5f, 0.25f, 100.0f};
  auto packed = CompressionEngine::quantize_f32_to_f16(src.data(), 4);
  CHECK(packed.size() == 8);

  auto recovered = CompressionEngine::dequantize_f16_to_f32(packed.data(), 4);
  for (std::size_t i = 0; i < 4; ++i) {
    CHECK(std::abs(recovered[i] - src[i]) < 0.1f * std::abs(src[i]) + 0.01f);
  }
}

TEST_CASE("Float32 to int8 quantization roundtrip", "[compression][i8]") {
  std::vector<float> src{0.0f, 0.5f, 1.0f, -1.0f};
  float scale, zp;
  auto packed = CompressionEngine::quantize_f32_to_i8(src.data(), 4, scale, zp);
  CHECK(packed.size() == 4);

  auto recovered =
      CompressionEngine::dequantize_i8_to_f32(packed.data(), 4, scale, zp);
  for (std::size_t i = 0; i < 4; ++i) {
    CHECK(std::abs(recovered[i] - src[i]) < 0.05f);
  }
}

TEST_CASE("AI stats include compression ratio", "[ai][compression]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string meta =
      "{\"artifact_type\":\"embedding\",\"owner\":\"vector\","
      "\"schema_version\":\"v1\",\"model_id\":\"m1\"}";
  std::vector<uint8_t> payload(200, 42);
  REQUIRE(ai.put("embedding", "comp:1", meta, payload));

  auto stats = ai.stats();
  CHECK(stats.find("avg_compression_ratio:") != std::string::npos);
}

// ===== Feature 6: Streaming =====

TEST_CASE("Stream begin, append, end roundtrip", "[ai][streaming]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string meta =
      "{\"artifact_type\":\"response\",\"owner\":\"response\","
      "\"schema_version\":\"v1\",\"model_id\":\"gpt-4\"}";

  REQUIRE(ai.stream_begin("stream:1", meta));

  std::vector<uint8_t> c1{'H', 'e', 'l', 'l', 'o'};
  std::vector<uint8_t> c2{' ', 'w', 'o', 'r', 'l', 'd'};
  REQUIRE(ai.stream_append("stream:1", c1));
  REQUIRE(ai.stream_append("stream:1", c2));

  auto partial = ai.stream_get("stream:1");
  REQUIRE(partial.has_value());
  CHECK(partial->payload.size() == 11);

  REQUIRE(ai.stream_end("stream:1"));

  auto final_val = ai.get("stream:1");
  REQUIRE(final_val.has_value());
  CHECK(final_val->payload.size() == 11);
  CHECK(std::string(final_val->payload.begin(), final_val->payload.end()) ==
        "Hello world");
}

TEST_CASE("Stream error handling", "[ai][streaming][errors]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string err;
  CHECK_FALSE(ai.stream_append("nonexistent", {1}, &err));
  CHECK(err.find("no active stream") != std::string::npos);

  CHECK_FALSE(ai.stream_end("nonexistent", &err));

  std::string meta =
      "{\"artifact_type\":\"response\",\"owner\":\"response\","
      "\"schema_version\":\"v1\"}";
  REQUIRE(ai.stream_begin("dup", meta));
  CHECK_FALSE(ai.stream_begin("dup", meta, &err));
  CHECK(err.find("already in progress") != std::string::npos);
}

TEST_CASE("AI stats include all feature metrics", "[ai][stats]") {
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024},
           make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  auto stats = ai.stats();
  CHECK(stats.find("total_dollar_saved:") != std::string::npos);
  CHECK(stats.find("sim_queries:") != std::string::npos);
  CHECK(stats.find("cascade_invalidations:") != std::string::npos);
  CHECK(stats.find("stream_begins:") != std::string::npos);
  CHECK(stats.find("vector_index_size:") != std::string::npos);
  CHECK(stats.find("dep_graph_edges:") != std::string::npos);
  CHECK(stats.find("active_streams:") != std::string::npos);
}
