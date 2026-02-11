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
