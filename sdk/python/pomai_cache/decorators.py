"""Decorator support for Pomai Cache memoization."""

from __future__ import annotations

import functools
import hashlib
import json
from typing import Any, Callable, Optional


def memoize(cache=None, artifact_type: str = "response",
            model_id: str = "", ttl_ms: Optional[int] = None):
    """Decorator that caches function results in Pomai Cache.

    Usage:
        cache = PomaiCache("localhost", 6379)
        cache.connect()

        @memoize(cache=cache, artifact_type="response", model_id="gpt-4")
        def generate(prompt: str) -> str:
            return call_llm(prompt)
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if cache is None:
                return fn(*args, **kwargs)

            sig = json.dumps({"args": [str(a) for a in args],
                              "kwargs": {k: str(v) for k, v in sorted(kwargs.items())}},
                             sort_keys=True)
            key_hash = hashlib.sha256(sig.encode()).hexdigest()[:16]
            cache_key = f"memo:{fn.__name__}:{key_hash}"

            existing = cache.get_artifact(cache_key)
            if existing is not None:
                _, payload = existing
                return payload.decode() if isinstance(payload, bytes) else payload

            result = fn(*args, **kwargs)

            meta = {
                "artifact_type": artifact_type,
                "owner": "default",
                "schema_version": "v1",
            }
            if model_id:
                meta["model_id"] = model_id

            payload = result if isinstance(result, (str, bytes)) else json.dumps(result)
            try:
                cache.put_artifact(artifact_type, cache_key, meta, payload)
            except Exception:
                pass

            return result
        return wrapper
    return decorator
