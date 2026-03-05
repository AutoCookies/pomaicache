#!/usr/bin/env bash
# Run all unit tests and embedded benchmarks (no server required).
# Use prompt_cache_bench --quick so the full run completes in reasonable time.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build}"
mkdir -p "${BUILD_DIR}"

cmake -S "${ROOT}" -B "${BUILD_DIR}" -DBUILD_TESTING=ON -DBUILD_PYTHON_BINDINGS=OFF
cmake --build "${BUILD_DIR}" -j

echo "========== Running tests =========="
ctest --test-dir "${BUILD_DIR}" --output-on-failure

echo ""
echo "========== Running benchmarks =========="
"${BUILD_DIR}/pomai_cache_bench"
"${BUILD_DIR}/ai_artifact_bench"
"${BUILD_DIR}/vector_cache_bench"
"${BUILD_DIR}/prompt_cache_bench" --quick
"${BUILD_DIR}/pomai_cache_crash_harness"

echo ""
echo "All tests and benchmarks completed successfully."
