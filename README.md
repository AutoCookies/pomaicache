# Pomai Cache v1

<img src="./assets/logo.png"/>

Embedded local cache core with RAM+SSD tiering, bounded TTL cleanup, crash-safe append-only SSD segments, selectable eviction policy (`lru`, `lfu`, `pomai_cost`), and an AI artifact cache layer for embeddings/prompts/RAG/rerank/response reuse.

## Repo structure

- `src/engine/` KV store, TTL heap, memory limit enforcement
- `src/policy/` LRU, LFU, PomaiCostPolicy
- `src/metrics/` INFO metrics module
- `bindings/` C and Python embedded bindings
- `bench/` embedded benchmarks
- `tests/` correctness tests
- `tuner/` offline python tuner

## Quickstart (embedded library)

### Build library

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

This produces the shared library `libpomaicache` and optional Python module.

### C++ usage

```cpp
#include <pomaicache.h>

int main() {
  pomaicache::Config cfg;
  cfg.memory_limit_bytes = 128 * 1024 * 1024;
  cfg.data_dir = "./data";

  pomaicache::PomaiCache cache(cfg);
  const std::string key = "demo";
  const std::string value = "hello";

  cache.Set(key, std::as_bytes(std::span(value.data(), value.size())),
            pomaicache::Ttl{300000});

  auto got = cache.Get(key);
  if (got) {
    // use *got
  }
}
```

### C API usage

```c
#include <pomaicache_c.h>

int main() {
  pomai_config_t cfg = { .memory_limit_bytes = 128 * 1024 * 1024,
                         .data_dir = "./data" };
  pomai_t* db = pomai_create(&cfg);
  const char* key = "demo";
  const char* val = "hello";
  pomai_set(db, key, strlen(key), val, strlen(val), 300000);
  void* out = NULL;
  size_t out_len = 0;
  if (pomai_get(db, key, strlen(key), &out, &out_len) == 0) {
    // use out / out_len
    pomai_free(out);
  }
  pomai_destroy(db);
}
```

### Python usage

```python
import pomaicache

cache = pomaicache.Cache(data_dir="./data", memory_limit_bytes=128*1024*1024)
# prompt_put / prompt_get APIs are available for prompt prefix caching
```

## Policy tuning

Generate params from offline stats snapshot:

```bash
python3 tuner/tune_policy.py --input stats_snapshot.json --output config/policy_params.json
```

Your application is responsible for loading updated params and calling the appropriate reload functions in the embedded API.

## Benchmarks

Benchmarks under `bench/` exercise the embedded library in-process (no network).

## Security/stability constraints

- max key length enforced
- max value size enforced
- bounded per-tick TTL cleanup

## SSD tier defaults (laptop-safe)

Recommended defaults:

- `--memory 67108864` (64 MiB RAM tier)
- `--ssd-enabled --data-dir ./data`
- `--ssd-value-min-bytes 2048`
- `--ssd-read-mb-s 256 --ssd-write-mb-s 256`
- `--fsync never`

Data files are stored under `--data-dir`:

- `manifest.txt`
- `segment_<id>.log`

See: `docs/TIERING.md`, `docs/SSD_FORMAT.md`, `docs/CRASH_SEMANTICS.md`, and `docs/BENCH_TIERING.md`.


## AI workload recommended config

- use SSD tier for large embeddings: `--ssd-enabled --ssd-value-min-bytes 2048`
- keep fsync disabled for cache semantics: `--fsync never`
- prefer `pomai_cost` policy for mixed AI artifact owners
- use short TTL for rerank/response and longer TTL for embeddings

See: `docs/AI_CACHE.md`, `docs/AI_COMMANDS.md`, `docs/INVALIDATION.md`, `docs/BLOB_DEDUP.md`.
