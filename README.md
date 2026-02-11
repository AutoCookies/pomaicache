# Pomai Cache v1

Redis-compatible (subset) local cache core with RAM+SSD tiering, bounded TTL cleanup, crash-safe append-only SSD segments, selectable eviction policy (`lru`, `lfu`, `pomai_cost`), and an AI artifact cache layer for embeddings/prompts/RAG/rerank/response reuse.

## Repo structure

- `src/server/` RESP parser + connection loop
- `src/engine/` KV store, TTL heap, memory limit enforcement
- `src/policy/` LRU, LFU, PomaiCostPolicy
- `src/metrics/` INFO metrics module
- `apps/cli/` simple CLI helper
- `bench/` benchmark tool
- `tests/` correctness tests
- `tuner/` offline python tuner
- `docker/` container artifacts

## Quickstart

### Build + run locally

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/pomai_cache_server --port 6379 --policy pomai_cost --params config/policy_params.json --ssd-enabled --data-dir ./data --ssd-value-min-bytes 2048 --fsync everysec
```

or:

```bash
make dev
```

### Run with Docker

```bash
make docker-build
make docker-run
```

### Example redis-cli session

```bash
redis-cli -p 6379 SET demo hello EX 30
redis-cli -p 6379 GET demo
redis-cli -p 6379 INFO
redis-cli -p 6379 CONFIG GET POLICY
redis-cli -p 6379 CONFIG SET POLICY lru
redis-cli -p 6379 CONFIG SET PARAMS /app/config/policy_params.json
```


### AI artifact quickstart (redis-cli)

```bash
redis-cli -p 6379 AI.PUT embedding emb:modelA:hashA:768:float16 '{"artifact_type":"embedding","owner":"vector","schema_version":"v1","model_id":"modelA","snapshot_epoch":"ix1"}' "abc"
redis-cli -p 6379 AI.GET emb:modelA:hashA:768:float16
redis-cli -p 6379 AI.STATS
redis-cli -p 6379 AI.INVALIDATE EPOCH ix1
```

## Make targets

- `make dev` debug build + run server
- `make release` release build
- `make test` tests
- `make bench` benchmarks
- `make crash-suite` short crash/recovery harness
- `make fmt` clang-format
- `make docker-build`
- `make docker-run`

## Supported commands

- `GET`
- `SET key value [EX seconds] [OWNER owner_name]`
- `DEL key [key ...]`
- `EXPIRE key seconds`
- `TTL key`
- `MGET key [key ...]`
- `INFO`
- `CONFIG GET POLICY`
- `CONFIG SET POLICY <lru|lfu|pomai_cost>`
- `CONFIG SET PARAMS <path>`

## Policy tuning

Generate params from offline stats snapshot:

```bash
python3 tuner/tune_policy.py --input stats_snapshot.json --output config/policy_params.json
```

The server loads params on startup and can reload at runtime via:

```bash
redis-cli -p 6379 CONFIG SET PARAMS /app/config/policy_params.json
```

Invalid/missing param files are handled safely with existing/default values.

## Benchmarks

Run:

```bash
make bench
```

Bench reports per workload and policy:

- ops/s
- p50/p95/p99 latency
- hit rate
- memory used

## Security/stability constraints

- max key length enforced
- max value size enforced
- max concurrent connections enforced
- slow-client protection via bounded output buffer
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
