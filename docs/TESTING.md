# Testing

Tests are migrated to Catch2 and split into:

- **Unit tests** (`tests/test_engine.cpp`, `tests/test_resp.cpp`)
- **Integration/adversarial tests** (`tests/test_integration.cpp`) that start a real server on a random localhost port.

## Run

```bash
make test
```

## Determinism guarantees

- Engine benchmark uses fixed seeds and prints the seed.
- `INFO` output has stable key ordering.
- `topk_hits` is sorted by descending hit count and then key.
- Config reload applies atomically and clamps values to safe ranges.
