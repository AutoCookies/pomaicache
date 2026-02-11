#!/usr/bin/env bash
set -euo pipefail
perf record -F 99 -g -- "$@"
