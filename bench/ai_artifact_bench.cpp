#include "pomai_cache/ai_cache.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace pomai_cache;

namespace {
struct WorkloadResult {
  std::string name;
  double ops_s{0};
  double p50{0};
  double p95{0};
  double p99{0};
  double p999{0};
  double hit_rate{0};
};

double pct(std::vector<double> v, double p) {
  if (v.empty())
    return 0.0;
  std::sort(v.begin(), v.end());
  std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(v.size() - 1));
  return v[idx];
}

WorkloadResult run_workload(AiArtifactCache &ai, const std::string &name, int ops,
                            const std::function<void(int, std::mt19937_64 &, int &, int &, std::vector<double> &)> &fn) {
  std::mt19937_64 rng(7);
  int gets = 0, hits = 0;
  std::vector<double> lat;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < ops; ++i)
    fn(i, rng, gets, hits, lat);
  auto sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  return {name, static_cast<double>(ops) / sec, pct(lat, 0.50), pct(lat, 0.95), pct(lat, 0.99), pct(lat, 0.999), gets ? static_cast<double>(hits) / gets : 0.0};
}

} // namespace

int main(int argc, char **argv) {
  std::string out = "ai_bench_summary.json";
  if (argc > 1)
    out = argv[1];

  Engine e({64 * 1024 * 1024, 256, 4 * 1024 * 1024}, make_policy_by_name("pomai_cost"));
  AiArtifactCache ai(e);

  std::vector<WorkloadResult> results;
  results.push_back(run_workload(ai, "embedding_zipf", 2000,
                                 [&](int i, std::mt19937_64 &rng, int &gets, int &hits, std::vector<double> &lat) {
                                   std::uniform_real_distribution<double> u(0.0, 1.0);
                                   int key_idx = static_cast<int>(std::pow(u(rng), 2.2) * 300.0);
                                   std::string k = canonical_embedding_key("e5", "h" + std::to_string(key_idx), key_idx % 2 ? 768 : 1536, "float16");
                                   auto s = std::chrono::steady_clock::now();
                                   auto g = ai.get(k);
                                   if (!g) {
                                     std::vector<std::uint8_t> payload(static_cast<std::size_t>(1024 + (key_idx % 16) * 512), 1);
                                     ai.put("embedding", k, "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\",\"model_id\":\"e5\",\"snapshot_epoch\":\"ix1\"}", payload);
                                   } else {
                                     ++hits;
                                   }
                                   ++gets;
                                   lat.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - s).count());
                                 }));

  results.push_back(run_workload(ai, "prompt_response_churn", 1500,
                                 [&](int i, std::mt19937_64 &, int &gets, int &hits, std::vector<double> &lat) {
                                   std::string pk = canonical_prompt_key("tok", "p" + std::to_string(i % 400));
                                   std::string rk = canonical_response_key("p" + std::to_string(i % 400), "paramsA", "m");
                                   auto s = std::chrono::steady_clock::now();
                                   auto p = ai.get(pk);
                                   if (!p)
                                     ai.put("prompt", pk, "{\"artifact_type\":\"prompt\",\"owner\":\"prompt\",\"schema_version\":\"v1\"}", {'p'});
                                   else
                                     ++hits;
                                   ++gets;
                                   if (!ai.get(rk))
                                     ai.put("response", rk, "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_version\":\"v1\",\"model_id\":\"m\"}", {'r'});
                                   lat.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - s).count());
                                 }));

  results.push_back(run_workload(ai, "rerank_ttl_storm", 1200,
                                 [&](int i, std::mt19937_64 &, int &gets, int &hits, std::vector<double> &lat) {
                                   std::string k = canonical_rerank_key("q" + std::to_string(i % 300), "ix1", 50, "ph");
                                   auto s = std::chrono::steady_clock::now();
                                   auto g = ai.get(k);
                                   if (!g)
                                     ai.put("rerank_buffer", k, "{\"artifact_type\":\"rerank_buffer\",\"owner\":\"rerank\",\"schema_version\":\"v1\",\"ttl_deadline\":120000}", std::vector<std::uint8_t>(512, 4));
                                   else
                                     ++hits;
                                   ++gets;
                                   lat.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - s).count());
                                 }));

  results.push_back(run_workload(ai, "mixed_rag_pipeline", 1000,
                                 [&](int i, std::mt19937_64 &, int &gets, int &hits, std::vector<double> &lat) {
                                   auto s = std::chrono::steady_clock::now();
                                   std::string p = canonical_prompt_key("tok", "qh" + std::to_string(i % 200));
                                   std::string ekey = canonical_embedding_key("e5", "qh" + std::to_string(i % 200), 768, "float16");
                                   std::string rag = canonical_rag_chunk_key("src", std::to_string(i % 500), "r1");
                                   std::string rrk = canonical_rerank_key("qh" + std::to_string(i % 200), "ix1", 20, "p");
                                   std::string rsp = canonical_response_key("qh" + std::to_string(i % 200), "p", "m");
                                   if (!ai.get(p)) ai.put("prompt", p, "{\"artifact_type\":\"prompt\",\"owner\":\"prompt\",\"schema_version\":\"v1\"}", {'x'}); else ++hits;
                                   if (!ai.get(ekey)) ai.put("embedding", ekey, "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\",\"model_id\":\"e5\"}", std::vector<std::uint8_t>(1024, 1)); else ++hits;
                                   if (!ai.get(rag)) ai.put("rag_chunk", rag, "{\"artifact_type\":\"rag_chunk\",\"owner\":\"rag\",\"schema_version\":\"v1\",\"snapshot_epoch\":\"ix1\"}", std::vector<std::uint8_t>(256, 2)); else ++hits;
                                   if (!ai.get(rrk)) ai.put("rerank_buffer", rrk, "{\"artifact_type\":\"rerank_buffer\",\"owner\":\"rerank\",\"schema_version\":\"v1\"}", std::vector<std::uint8_t>(256, 3)); else ++hits;
                                   if (!ai.get(rsp)) ai.put("response", rsp, "{\"artifact_type\":\"response\",\"owner\":\"response\",\"schema_version\":\"v1\",\"model_id\":\"m\"}", std::vector<std::uint8_t>(128, 4)); else ++hits;
                                   gets += 5;
                                   lat.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - s).count());
                                 }));

  auto r0 = std::chrono::steady_clock::now();
  Engine warm({64 * 1024 * 1024, 256, 4 * 1024 * 1024}, make_policy_by_name("pomai_cost"));
  auto warm_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - r0).count();

  std::ofstream os(out);
  os << "{\n  \"workloads\": [\n";
  for (std::size_t i = 0; i < results.size(); ++i) {
    const auto &r = results[i];
    os << "    {\"name\":\"" << r.name << "\",\"ops_s\":" << std::fixed << std::setprecision(2)
       << r.ops_s << ",\"p50_us\":" << r.p50 << ",\"p95_us\":" << r.p95
       << ",\"p99_us\":" << r.p99 << ",\"p999_us\":" << r.p999
       << ",\"hit_rate\":" << r.hit_rate << "}";
    if (i + 1 != results.size())
      os << ",";
    os << "\n";
  }
  os << "  ],\n";
  os << "  \"ssd_mb_s\": 0.0,\n";
  os << "  \"warm_restart_ms\": " << warm_ms << ",\n";
  os << "  \"dedup_ratio\": 0.0\n";
  os << "}\n";

  std::cout << "wrote " << out << "\n";
  return 0;
}
