#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${ROOT}/build-release"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ROOT}/bench_results/${TS}"
DATA_DIR="${OUT_DIR}/tier_data"
mkdir -p "${OUT_DIR}" "${DATA_DIR}"

cmake -S "${ROOT}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release
cmake --build "${BUILD_DIR}" -j

PORT=6389
"${BUILD_DIR}/pomai_cache_server" --port ${PORT} --policy pomai_cost --params "${ROOT}/config/policy_params.json" \
  --data-dir "${DATA_DIR}" --ssd-enabled --ssd-value-min-bytes 2048 >"${OUT_DIR}/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID} >/dev/null 2>&1 || true' EXIT
sleep 1

for wl in tier_off_ram_only tier_on_large_values tier_on_pressure_demotion ttl_storm_with_tier; do
  "${BUILD_DIR}/pomai_cache_netbench" --port ${PORT} --workload "${wl}" --duration 8 --warmup 2 --pipeline 8 --json "${OUT_DIR}/${wl}.json" | tee -a "${OUT_DIR}/summary.txt"
done

# warm restart benchmark
start_ms=$(date +%s%3N)
kill -INT ${SERVER_PID}
wait ${SERVER_PID}
"${BUILD_DIR}/pomai_cache_server" --port ${PORT} --policy pomai_cost --params "${ROOT}/config/policy_params.json" \
  --data-dir "${DATA_DIR}" --ssd-enabled --ssd-value-min-bytes 2048 >"${OUT_DIR}/server_restart.log" 2>&1 &
SERVER_PID=$!
for i in $(seq 1 50); do
  if printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 ${PORT} 2>/dev/null | head -n1 | grep -q PONG; then
    break
  fi
  sleep 0.1
done
accept_ms=$(( $(date +%s%3N) - start_ms ))
printf '{"workload":"warm_restart_time","accept_ms":%d}\n' "${accept_ms}" > "${OUT_DIR}/warm_restart_time.json"

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
