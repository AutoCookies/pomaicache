# Pomai Cache v1 Architecture

- `src/server`: RESP parser and TCP connection handling.
- `src/engine`: key-value storage, TTL min-heap, memory enforcement.
- `src/policy`: LRU, LFU, PomaiCostPolicy.
- `src/metrics`: INFO-focused metrics surface.
- `tuner`: offline policy parameter tuner.
