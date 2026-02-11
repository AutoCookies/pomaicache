# Soak + chaos
Run soak:
`python3 tests/soak/pomai_cache_soak.py --port 6379 --duration 180`

Chaos coverage uses `tests/test_chaos.cpp` for churn with mixed set/get/del/expire ensuring no memory overflow.
