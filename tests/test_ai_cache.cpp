#include "pomai_cache/ai_cache.hpp"

#include <catch2/catch_test_macros.hpp>

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
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024}, make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::string meta =
      "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\",\"model_id\":\"m\",\"snapshot_epoch\":\"ep1\"}";
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
  Engine e({4 * 1024 * 1024, 256, 1024 * 1024}, make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<std::uint8_t> payload{9, 8, 7};
  REQUIRE(ai.put("response", "rsp:1", "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_version\":\"v1\",\"model_id\":\"m2\",\"snapshot_epoch\":\"e2\"}", payload));
  REQUIRE(ai.put("response", "rsp:2", "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_version\":\"v1\",\"model_id\":\"m3\",\"snapshot_epoch\":\"e3\"}", payload));

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

  REQUIRE(!ai.put("embedding", "k", "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\"}", payload, &err));
  REQUIRE(err.find("blob put failed") != std::string::npos);
}
