# AI Commands

All commands are RESP-compatible and usable with `redis-cli`.

## Store / fetch

```bash
redis-cli -p 6379 AI.PUT embedding emb:modelX:ih:768:float16 '{"artifact_type":"embedding","owner":"vector","schema_version":"v1","model_id":"modelX","snapshot_epoch":"ix42"}' "<binary>"
redis-cli -p 6379 AI.GET emb:modelX:ih:768:float16
redis-cli -p 6379 AI.MGET emb:k1 emb:k2 emb:k3
```

## Embedding helpers

```bash
redis-cli -p 6379 AI.EMB.PUT emb:modelX:ih:768:float16 modelX 768 float16 3600 "<vector-bytes>"
redis-cli -p 6379 AI.EMB.GET emb:modelX:ih:768:float16
```

## Invalidation

```bash
redis-cli -p 6379 AI.INVALIDATE EPOCH ix42
redis-cli -p 6379 AI.INVALIDATE MODEL modelX
redis-cli -p 6379 AI.INVALIDATE PREFIX emb:modelX:
```

## Introspection

```bash
redis-cli -p 6379 AI.STATS
redis-cli -p 6379 AI.TOP HOT 20
redis-cli -p 6379 AI.TOP COSTLY 20
redis-cli -p 6379 AI.EXPLAIN emb:modelX:ih:768:float16
```

## Prompt prefix cache (HTTP)

The HTTP server exposes a lightweight prompt prefix cache optimized for
pre-tokenized prompts and K/V-style reuse:

- `POST /ai/prompt_cache/put/<tokenizer_id>/<prompt_prefix_hash>?cached_tokens=N&ttl_ms=M`  
  Body is the serialized token sequence for the prefix. This stores the prefix
  as a `prompt` artifact and indexes it for longest-prefix matching.
- `POST /ai/prompt_cache/get/<tokenizer_id>/<prompt_full_hash>?prefix_min_tokens=K`  
  Body is the serialized token sequence for the full prompt. The response
  includes `hit`, `prompt_prefix_hash`, `cached_tokens`, `suffix_tokens`, and
  `savings_ratio`, indicating how many tokens can be reused from cache.
- `POST /ai/prompt_cache/invalidate/<tokenizer_id>/<prompt_prefix_hash>`  
  Invalidates a single cached prefix and its backing artifact.
- `GET /ai/prompt_cache/stats`  
  Returns aggregate prompt cache metrics, including `prompt_cache_hits`,
  `prompt_cache_misses`, `prompt_cache_cached_prefix_bytes`,
  `prompt_cache_average_savings_ratio`, and `prompt_cache_entries`.

These endpoints are designed to front LLM providers or PomaiDB-backed RAG
pipelines, allowing stable system prompts and common prefixes to be reused
instead of recomputed on each request.
