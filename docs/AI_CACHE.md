# AI Artifact Cache

Pomai Cache Phase 5 adds an AI artifact layer on top of the existing KV/tiering engine.

## What it is

A **local, lossy cache** for AI pipeline artifacts:

- embeddings
- prompts
- rag chunks
- rerank buffers
- responses

## What it is not

- not a database
- not WAL semantics
- no correctness-critical guarantees
- restart may lose metadata or entries; loss is treated as a cache miss

## Storage model

- Logical AI key maps to content hash
- Payload is stored in `blob:<content_hash>`
- AI metadata is tracked in-memory and returned by `AI.GET`
- Blob dedup is best-effort
- Refcounts are best-effort and bounded by eviction/invalidation/GC behavior

## TTL classes

Owner defaults:

- `rerank`: ~5 minutes
- `response`: ~1 hour
- `prompt`: ~24 hours
- `vector` (embeddings): ~7 days
- `rag`: ~6 hours

Per-item TTL from metadata overrides defaults.

### Prompt prefix cache TTLs

Prompt prefixes cached via the HTTP prompt cache API share the `owner=prompt`
class but typically use a much shorter horizon (5–10 minutes) to reflect the
ephemeral nature of interactive LLM sessions. The default can be overridden
with the `--default-prompt-ttl-sec` server flag or per-prefix `ttl_ms`
parameter. Cached prefixes are treated as significantly cheaper than fresh
tokens in AI stats, simulating ~10x lower cost for reused tokens.

## Tiering/persistence semantics

SSD remains a warm cache tier:

- async write-behind behavior in SSD store
- fsync default for server is `never`
- queue pressure may drop writes
- restart rebuild is best effort; corrupted tails are skipped

Missing data after restart is a cache miss by design.
