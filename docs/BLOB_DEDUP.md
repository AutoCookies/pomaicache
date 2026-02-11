# Blob Dedup

Pomai AI artifacts use content-addressed blobs:

- Blob key: `blob:<content_hash>`
- Logical AI key stores hash reference

## Dedup behavior

- Storing equal payloads with different logical keys reuses one blob key
- `AI.STATS` exposes dedup counters (`dedup_hits`, `blob_count`)
- Refcounting is best-effort (cache semantics), not transactional

## Failure semantics

- If blob write fails under caps/tiering pressure, AI.PUT fails
- If metadata is missing after restart, lookup is treated as cache miss
