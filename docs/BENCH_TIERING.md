# Tiering Benchmarks

Use:

```bash
make bench-all
```

Presets:

- `tier_off_ram_only`
- `tier_on_large_values`
- `tier_on_pressure_demotion`
- `ttl_storm_with_tier`
- `warm_restart_time`

JSON output includes:

- `ops_per_sec`
- `p50_us`, `p95_us`, `p99_us`, `p999_us`
- `hit_rate`, `ram_hits`, `ssd_hits`
- `ssd_read_mb`, `ssd_write_mb`
- `ssd_index_rebuild_ms`
- `fragmentation_estimate`
