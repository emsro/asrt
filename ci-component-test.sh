#!/usr/bin/env sh
# Verify that asrtl+asrtr and asrtl+asrtc can each be consumed independently
# after an install.
#
# Usage (from repo root):
#   ./ci-component-test.sh [install_prefix]
#
# Default prefix: /tmp/asrt-component-test-install

set -e

REPO_ROOT=$(cd "$(dirname "$0")" && pwd)
PREFIX=${1:-/tmp/asrt-component-test-install}
BUILD_DIR=${2:-${REPO_ROOT}/_build/release-c}

echo "=== Installing from ${BUILD_DIR} to ${PREFIX} ==="
cmake --install "${BUILD_DIR}" --prefix "${PREFIX}"

# --- reactor (asrtl + asrtr) ---
echo "=== Building reactor consumer ==="
REACTOR_BUILD=$(mktemp -d)
cmake \
    -S "${REPO_ROOT}/test/component/reactor" \
    -B "${REACTOR_BUILD}" \
    -DCMAKE_PREFIX_PATH="${PREFIX}"
cmake --build "${REACTOR_BUILD}"
"${REACTOR_BUILD}/reactor_smoke"
echo "reactor_smoke: PASS"

# --- controller (asrtl + asrtc) ---
echo "=== Building controller consumer ==="
CONTROLLER_BUILD=$(mktemp -d)
cmake \
    -S "${REPO_ROOT}/test/component/controller" \
    -B "${CONTROLLER_BUILD}" \
    -DCMAKE_PREFIX_PATH="${PREFIX}"
cmake --build "${CONTROLLER_BUILD}"
"${CONTROLLER_BUILD}/controller_smoke"
echo "controller_smoke: PASS"

echo "=== Component test PASSED ==="
