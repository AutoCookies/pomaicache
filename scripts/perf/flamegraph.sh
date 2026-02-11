#!/usr/bin/env bash
set -euo pipefail
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
