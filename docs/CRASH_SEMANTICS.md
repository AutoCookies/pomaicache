# Crash Semantics

## fsync modes

- `never`: best throughput; recent writes may be lost on crash
- `everysec`: fsync at most once per second; bounded recent loss window
- `always`: fsync each append; highest durability and latency cost

## Harness

`pomai_cache_crash_harness`:

- runs mixed SET/GET/DEL + TTL workload
- issues frequent `SIGKILL`
- restarts server repeatedly
- validates restart health (no startup crash, INFO responds, GET paths usable)

Run locally:

```bash
make crash-suite
```

## Guarantees

- no startup corruption panic on torn tail records
- torn tail truncated during recovery
- expired values are not served after restart
