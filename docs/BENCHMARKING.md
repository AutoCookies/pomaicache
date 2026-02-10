# Benchmarking

## Engine benchmark

Run deterministic policy comparison:

```bash
make bench
```

The benchmark prints the fixed RNG seed and a markdown table for `lru`, `lfu`, and `pomai_cost` across `hotset`, `uniform`, `writeheavy`, and `mixed` workloads.

## Network benchmark

Start server (separate shell):

```bash
./build-release/pomai_cache_server --port 6379 --params config/policy_params.json
```

Run netbench:

```bash
make netbench
# or
./build-release/pomai_cache_netbench --workload hotset --duration 10 --warmup 2 --pipeline 8 --json out/hotset.json
```

Metrics include `ops/s`, `p50/p95/p99/p999`, hit rate, memory usage, and eviction/admission counters from `INFO`.

## Repro harness

Use the full reproducible harness:

```bash
make bench-all
```

Artifacts are written to `bench_results/<timestamp>/` with:

- commit hash
- build type
- cpu info (`/proc/cpuinfo` best effort)
- benchmark text/json outputs
- config path used
