# Tracing
Tracing is off by default.

Enable:
- `CONFIG SET TRACE.PATH /tmp/pomai.trace.jsonl`
- `CONFIG SET TRACE.SAMPLE_RATE 0.1`
- `CONFIG SET TRACE.ENABLED yes`

Trace lines are JSONL with hashed keys, op type, value size, ttl class, owner, hit/miss and latency bucket.
Use `TRACE STREAM` for capped in-memory recent trace lines.
