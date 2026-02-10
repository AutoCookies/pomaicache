#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="Offline tuner for PomaiCostPolicy")
    ap.add_argument("--input", required=True, help="stats snapshot JSON")
    ap.add_argument("--output", default="policy_params.json")
    args = ap.parse_args()

    data = json.loads(Path(args.input).read_text())
    miss_rate = float(data.get("miss_rate", 0.1))
    avg_size = float(data.get("avg_size_bytes", 1024))

    weights = {
        "w_miss": round(1.0 + miss_rate * 5, 4),
        "w_reuse": 1.0,
        "w_mem": round(min(10.0, max(0.1, avg_size / 8192)), 4),
        "w_risk": 1.0,
    }
    out = {
        "version": "tuner-v1",
        "weights": weights,
        "thresholds": {
            "admit_threshold": 0.0,
            "evict_pressure": 0.85,
        },
        "guardrails": {
            "max_evictions_per_second": 50000,
            "max_admissions_per_second": 50000,
        },
        "per_owner_priors": {
            "default": {"p_reuse_prior": 0.5},
            "premium": {"p_reuse_prior": 0.7},
        },
        "ranges": {
            "w_miss": [0.0, 1000.0],
            "w_reuse": [0.0, 1000.0],
            "w_mem": [0.0, 1000.0],
            "w_risk": [0.0, 1000.0],
            "evict_pressure": [0.1, 1.0],
        },
    }
    Path(args.output).write_text(json.dumps(out, indent=2) + "\n")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
