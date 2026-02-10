#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${ROOT}/build-release"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ROOT}/bench_results/${TS}"
mkdir -p "${OUT_DIR}"

cmake -S "${ROOT}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release
cmake --build "${BUILD_DIR}" -j

PORT=6389
"${BUILD_DIR}/pomai_cache_server" --port ${PORT} --policy pomai_cost --params "${ROOT}/config/policy_params.json" >"${OUT_DIR}/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID} >/dev/null 2>&1 || true' EXIT
sleep 1

for wl in hotset uniform writeheavy mixed pipeline ttlheavy; do
  "${BUILD_DIR}/pomai_cache_netbench" --port ${PORT} --workload "${wl}" --duration 8 --warmup 2 --pipeline 8 --json "${OUT_DIR}/${wl}.json" | tee -a "${OUT_DIR}/summary.txt"
done

"${BUILD_DIR}/pomai_cache_bench" | tee "${OUT_DIR}/engine_bench.txt"

{
  echo "commit=$(git -C "${ROOT}" rev-parse HEAD)"
  echo "build_type=Release"
  echo "date=${TS}"
  echo "config=${ROOT}/config/policy_params.json"
  echo "cpu_info:" 
  cat /proc/cpuinfo 2>/dev/null | head -n 20 || true
} >"${OUT_DIR}/meta.txt"

cp -r "${OUT_DIR}" "${ROOT}/out/latest_bench" 2>/dev/null || true

echo "Saved benchmark artifacts to ${OUT_DIR}"
