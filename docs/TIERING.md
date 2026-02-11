# Tiering (RAM + SSD)

Pomai Cache supports a two-tier layout:

- **RAM tier**: key index, hot values, low-latency hits.
- **SSD tier**: large values and demoted entries from RAM pressure.

## Configuration

Server flags:

- `--ssd-enabled`
- `--data-dir <path>`
- `--ssd-value-min-bytes <n>`
- `--ssd-max-bytes <n>`
- `--ssd-read-mb-s <n>`
- `--ssd-write-mb-s <n>`
- `--promotion-hits <n>`
- `--demotion-pressure <0..1>`

## Placement

- `SET`: values `>= ssd_value_min_bytes` are written to SSD by default.
- RAM pressure beyond `demotion_pressure` queues demotions to SSD.
- `GET` miss in RAM checks SSD index and reads from segment files.
- Repeated SSD hits queue promotion work (bounded per tick).

## Tail-latency controls

- bounded `tier_work_per_tick` for promotion/demotion
- token-bucket IO limiter for SSD reads/writes
- bounded TTL cleanup for RAM + SSD

## INFO metrics

- `ram_bytes`, `ssd_bytes`
- `ssd_gets`, `ssd_hits`, `ssd_misses`
- `promotions`, `demotions`
- `ssd_read_mb`, `ssd_write_mb`
- `tier_backlog`
