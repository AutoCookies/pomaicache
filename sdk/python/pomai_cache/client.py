"""Pomai Cache client with AI-first semantics."""

from __future__ import annotations

import json
import socket
import struct
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pomai_cache.resp import encode_command, read_reply

try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    from opentelemetry import trace

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False


def _vector_to_str(vector) -> str:
    """Convert a vector (list, numpy array, or torch tensor) to comma-separated string."""
    if HAS_NUMPY and isinstance(vector, np.ndarray):
        vector = vector.astype(float).flatten().tolist()
    elif hasattr(vector, "detach"):
        vector = vector.detach().cpu().numpy().astype(float).flatten().tolist()
    return ",".join(str(float(v)) for v in vector)


def _bytes_to_payload(data: bytes | str) -> str:
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    return data


class PomaiCache:
    """Synchronous Pomai Cache client."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379,
                 timeout: float = 5.0, password: Optional[str] = None):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._password = password
        self._tracer = None
        if HAS_OTEL:
            self._tracer = trace.get_tracer("pomai_cache")

    def connect(self) -> "PomaiCache":
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self._timeout)
        self._sock.connect((self._host, self._port))
        if self._password:
            self._execute("AUTH", self._password)
        return self

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.close()

    def _execute(self, *args: str) -> Any:
        if not self._sock:
            self.connect()
        self._sock.sendall(encode_command(*args))
        return read_reply(self._sock)

    def _traced(self, op_name: str, fn, *args, **kwargs):
        if self._tracer:
            with self._tracer.start_as_current_span(f"pomai_cache.{op_name}"):
                return fn(*args, **kwargs)
        return fn(*args, **kwargs)

    # --- Standard KV ---

    def get(self, key: str) -> Optional[str]:
        return self._traced("get", self._execute, "GET", key)

    def set(self, key: str, value: str, ttl_ms: Optional[int] = None) -> str:
        if ttl_ms is not None:
            return self._traced("set", self._execute, "SET", key, value, "PX", str(ttl_ms))
        return self._traced("set", self._execute, "SET", key, value)

    def delete(self, *keys: str) -> int:
        return self._traced("delete", self._execute, "DEL", *keys)

    # --- AI Artifact Operations ---

    def put_artifact(self, artifact_type: str, key: str, meta: Dict[str, Any],
                     payload: bytes | str, depends_on: Optional[List[str]] = None) -> str:
        meta.setdefault("artifact_type", artifact_type)
        meta.setdefault("owner", "default")
        meta.setdefault("schema_version", "v1")
        meta_json = json.dumps(meta)
        payload_str = _bytes_to_payload(payload)
        args = ["AI.PUT", artifact_type, key, meta_json, payload_str]
        if depends_on:
            args.append("DEPENDS_ON")
            args.extend(depends_on)
        return self._traced("put_artifact", self._execute, *args)

    def get_artifact(self, key: str) -> Optional[Tuple[Dict[str, Any], bytes]]:
        result = self._traced("get_artifact", self._execute, "AI.GET", key)
        if result is None:
            return None
        meta = json.loads(result[0])
        payload = result[1].encode() if isinstance(result[1], str) else result[1]
        return meta, payload

    def put_embedding(self, model_id: str, input_hash: str, vector,
                      payload: bytes | str = b"", dim: Optional[int] = None,
                      dtype: str = "float32", ttl_ms: Optional[int] = None,
                      **extra_meta) -> str:
        if dim is None:
            if HAS_NUMPY and isinstance(vector, np.ndarray):
                dim = vector.shape[-1]
            elif hasattr(vector, "shape"):
                dim = vector.shape[-1]
            else:
                dim = len(vector)

        key = f"emb:{model_id}:{input_hash}:{dim}:{dtype}"
        meta = {
            "artifact_type": "embedding",
            "owner": "vector",
            "schema_version": "v1",
            "model_id": model_id,
            **extra_meta,
        }
        if ttl_ms:
            meta["ttl_deadline"] = ttl_ms
        return self.put_artifact("embedding", key, meta, payload)

    def put_response(self, prompt_hash: str, params_hash: str, model_id: str,
                     response: str, inference_tokens: int = 0,
                     dollar_cost: float = 0.0, **extra_meta) -> str:
        key = f"rsp:{prompt_hash}:{params_hash}:{model_id}"
        meta = {
            "artifact_type": "response",
            "owner": "response",
            "schema_version": "v1",
            "model_id": model_id,
            "inference_tokens": inference_tokens,
            "dollar_cost": dollar_cost,
            **extra_meta,
        }
        return self.put_artifact("response", key, meta, response)

    # --- Similarity Search ---

    def sim_put(self, key: str, vector, payload: bytes | str,
                meta: Optional[Dict[str, Any]] = None) -> str:
        vec_str = _vector_to_str(vector)
        payload_str = _bytes_to_payload(payload)
        meta_json = json.dumps(meta) if meta else ""
        args = ["AI.SIM.PUT", key, vec_str, payload_str]
        if meta_json:
            args.append(meta_json)
        return self._traced("sim_put", self._execute, *args)

    def sim_get(self, vector, top_k: int = 1,
                threshold: float = 0.9) -> List[Dict[str, Any]]:
        vec_str = _vector_to_str(vector)
        result = self._traced(
            "sim_get", self._execute,
            "AI.SIM.GET", vec_str, "TOPK", str(top_k), "THRESHOLD", str(threshold),
        )
        if not result:
            return []
        results = []
        for i in range(0, len(result), 4):
            results.append({
                "key": result[i],
                "score": float(result[i + 1]),
                "meta": json.loads(result[i + 2]),
                "payload": result[i + 3],
            })
        return results

    # --- Streaming ---

    def stream_begin(self, key: str, meta: Dict[str, Any]) -> str:
        meta.setdefault("artifact_type", "response")
        meta.setdefault("owner", "response")
        meta.setdefault("schema_version", "v1")
        return self._traced("stream_begin", self._execute,
                            "AI.STREAM.BEGIN", key, json.dumps(meta))

    def stream_append(self, key: str, chunk: str | bytes) -> str:
        return self._traced("stream_append", self._execute,
                            "AI.STREAM.APPEND", key, _bytes_to_payload(chunk))

    def stream_end(self, key: str) -> str:
        return self._traced("stream_end", self._execute, "AI.STREAM.END", key)

    def stream_get(self, key: str) -> Optional[Tuple[Dict[str, Any], bytes]]:
        result = self._traced("stream_get", self._execute, "AI.STREAM.GET", key)
        if result is None:
            return None
        meta = json.loads(result[0])
        payload = result[1].encode() if isinstance(result[1], str) else result[1]
        return meta, payload

    # --- Invalidation ---

    def invalidate_epoch(self, epoch: str) -> int:
        return self._traced("invalidate", self._execute,
                            "AI.INVALIDATE", "EPOCH", epoch)

    def invalidate_model(self, model_id: str) -> int:
        return self._traced("invalidate", self._execute,
                            "AI.INVALIDATE", "MODEL", model_id)

    def invalidate_cascade(self, key: str) -> int:
        return self._traced("invalidate_cascade", self._execute,
                            "AI.INVALIDATE", "CASCADE", key)

    # --- Cost & Budget ---

    def cost_report(self) -> Dict[str, Any]:
        result = self._traced("cost_report", self._execute, "AI.COST.REPORT")
        report = {}
        for line in result.strip().split("\n"):
            if ":" in line:
                k, v = line.split(":", 1)
                try:
                    report[k] = float(v) if "." in v else int(v)
                except ValueError:
                    report[k] = v
        return report

    def set_budget(self, max_dollar_per_hour: float) -> str:
        return self._traced("set_budget", self._execute,
                            "AI.BUDGET", str(max_dollar_per_hour))

    # --- Info ---

    def stats(self) -> str:
        return self._traced("stats", self._execute, "AI.STATS")

    def info(self) -> str:
        return self._traced("info", self._execute, "INFO")


class AsyncPomaiCache:
    """Async wrapper using asyncio. Requires an event loop."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379,
                 timeout: float = 5.0, password: Optional[str] = None):
        self._sync = PomaiCache(host, port, timeout, password)

    async def connect(self) -> "AsyncPomaiCache":
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._sync.connect)
        return self

    async def close(self):
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._sync.close)

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, *args):
        await self.close()

    async def _run(self, method, *args, **kwargs):
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: method(*args, **kwargs))

    async def get(self, key: str):
        return await self._run(self._sync.get, key)

    async def set(self, key: str, value: str, ttl_ms: Optional[int] = None):
        return await self._run(self._sync.set, key, value, ttl_ms)

    async def put_artifact(self, artifact_type: str, key: str, meta: Dict, payload, **kw):
        return await self._run(self._sync.put_artifact, artifact_type, key, meta, payload, **kw)

    async def get_artifact(self, key: str):
        return await self._run(self._sync.get_artifact, key)

    async def sim_put(self, key: str, vector, payload, meta=None):
        return await self._run(self._sync.sim_put, key, vector, payload, meta)

    async def sim_get(self, vector, top_k: int = 1, threshold: float = 0.9):
        return await self._run(self._sync.sim_get, vector, top_k, threshold)

    async def cost_report(self):
        return await self._run(self._sync.cost_report)

    async def stats(self):
        return await self._run(self._sync.stats)
