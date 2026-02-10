# Pomai Cache v1

Redis-compatible (subset) in-memory cache with deterministic INFO output, bounded TTL cleanup, and selectable eviction policy (`lru`, `lfu`, `pomai_cost`).

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
./build/pomai_cache_server --port 6379 --policy pomai_cost --params config/policy_params.json
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

## Make targets

- `make dev` debug build + run server
- `make release` release build
- `make test` tests
- `make bench` benchmarks
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
