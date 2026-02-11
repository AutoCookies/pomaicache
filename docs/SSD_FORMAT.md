# SSD Storage Format

## Files

- `segment_<id>.log`: append-only record log
- `manifest.txt`: active segment + known segment ids

## Record layout

Each record is encoded as:

1. fixed header (`magic`, `checksum`, `key_hash`, `seq`, `ttl_epoch_ms`, `key_len`, `value_len`, `tombstone`)
2. key bytes
3. value bytes

`checksum` validates header+key+value integrity.

## Crash safety

- Segment writes are append-only.
- Manifest update: write temp + fsync + rename + fsync directory.
- `--fsync never|everysec|always` controls segment fsync policy.

## Recovery

At startup:

1. load manifest (fallback to default segment)
2. scan required segments in order
3. verify checksum per record
4. truncate corrupted tail to last valid offset
5. rebuild in-memory SSD index with latest `seq` per key
