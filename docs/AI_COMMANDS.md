# AI Commands (embedded)

The AI artifact cache is exposed as a C++ library (`pomai_cache::AiArtifactCache`) and via the higher-level `pomaicache::PomaiCache` / C / Python bindings.

Below are examples using the C++ API directly.

## Store / fetch

```cpp
#include <pomai_cache/ai_cache.hpp>
#include <pomai_cache/engine.hpp>

pomai_cache::EngineConfig cfg;
cfg.memory_limit_bytes = 128 * 1024 * 1024;
cfg.data_dir = "./data";

pomai_cache::Engine engine(cfg, pomai_cache::make_policy_by_name("pomai_cost"));
pomai_cache::AiArtifactCache ai(engine);

std::vector<std::uint8_t> payload{/* bytes */};
std::string meta = R"({"artifact_type":"embedding","owner":"vector","schema_version":"v1","model_id":"modelX","snapshot_epoch":"ix42"})";

ai.put("embedding", "emb:modelX:ih:768:float16", meta, payload);

auto got = ai.get("emb:modelX:ih:768:float16");
if (got) {
  // use got->meta / got->payload
}
```

## Invalidation

```cpp
ai.invalidate_epoch("ix42");
ai.invalidate_model("modelX");
ai.invalidate_prefix("emb:modelX:");
```

## Introspection

```cpp
std::string stats = ai.stats();
std::string hot = ai.top_hot(20);
std::string costly = ai.top_costly(20);
std::string explain = ai.explain("emb:modelX:ih:768:float16");
```

