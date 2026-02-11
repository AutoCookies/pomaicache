# AI Invalidation

Bounded invalidation indexes are maintained in-memory.

## Supported invalidation

- Epoch-based: `AI.INVALIDATE EPOCH <epoch>`
- Model-based: `AI.INVALIDATE MODEL <model_id>`
- Prefix-based: `AI.INVALIDATE PREFIX <prefix>`

## Bounded behavior

- Prefix index stores up to a bounded key set per prefix bucket
- No full keyspace scan by default
- Invalidation is deterministic with sorted internal handling

## Semantics

- Visibility is removed immediately for indexed keys
- Blob references are decremented best-effort
- Orphan blobs are eligible for cleanup through refcount drop + engine eviction
