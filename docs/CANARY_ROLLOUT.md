# Canary rollout
1. Load existing params as control (LKG persists automatically).
2. Enable canary split: `CONFIG SET POLICY.CANARY_PCT 10`.
3. Reload params file with candidate values via `CONFIG SET PARAMS <path>`.

Server tracks control vs candidate hit-rate and p99 latency and auto-rolls back to LKG if guardrails are violated.
INFO includes canary fields and last rollback event.
