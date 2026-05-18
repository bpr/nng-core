#!/usr/bin/env bash
# bench_large_msg_nngcat.sh — measure nngcat (C libnng) PUSH/PULL throughput
# for large message payloads using --file for payload delivery.
#
# Methodology: generate a temp file of the desired size, run nngcat PUSH with
# different message counts at each size, subtract wall times to get marginal
# per-message cost and MiB/s, then repeat for the next size.
#
# Requirements: nngcat in PATH (from system NNG package, e.g. `apt install nng-utils`)
#               python3 in PATH (for timing math)
#
# Usage:
#   ./scripts/bench_large_msg_nngcat.sh

set -euo pipefail

PORT="19610"
URL="tcp://127.0.0.1:${PORT}"
TMPFILE="$(mktemp /tmp/nng_bench_payload_XXXXXX)"
trap 'rm -f "${TMPFILE}"; kill "${PULL_PID}" 2>/dev/null; wait "${PULL_PID}" 2>/dev/null' EXIT

if ! command -v nngcat &>/dev/null; then
    echo "error: nngcat not found in PATH" >&2
    echo "  Install with: sudo apt install nng-utils  (or equivalent)" >&2
    exit 1
fi

echo "C libnng large-message PUSH/PULL throughput"
echo "  peer: nngcat ${URL}"
echo ""

# Start PULL sink in background; no --count so it runs indefinitely
nngcat --pull0 --listen "${URL}" --silent &
PULL_PID=$!
sleep 0.2  # wait for SP handshake

run_push() {
    local payload_file="$1"
    local count="$2"
    { time for _ in $(seq 1 "${count}"); do
        nngcat --push0 --dial "${URL}" --file "${payload_file}" 2>/dev/null
    done; } 2>&1 \
        | awk '/real/ { split($2, a, /[ms]/); print a[1]*60000 + a[2]*1000 }'
}

for SIZE_MIB in 1 4 16; do
    SIZE_BYTES=$(( SIZE_MIB * 1024 * 1024 ))
    echo "── ${SIZE_MIB} MiB payload ──────────────────────────────────────"

    # Generate payload file
    dd if=/dev/zero bs=1024 count=$(( SIZE_MIB * 1024 )) of="${TMPFILE}" 2>/dev/null

    echo -n "  5 msgs  ... "; T5="$(run_push "${TMPFILE}" 5)";   echo "${T5} ms"
    echo -n "  20 msgs ... "; T20="$(run_push "${TMPFILE}" 20)";  echo "${T20} ms"
    echo -n "  50 msgs ... "; T50="$(run_push "${TMPFILE}" 50)";  echo "${T50} ms"
    echo ""

    python3 - "${T5}" "${T20}" "${T50}" "${SIZE_BYTES}" <<'EOF'
import sys
t5, t20, t50, payload = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])

marginal_low  = (t20 - t5)  / (20 - 5)   # ms/msg
marginal_high = (t50 - t20) / (50 - 20)  # ms/msg

for label, m in [("20-5 msgs", marginal_low), ("50-20 msgs", marginal_high)]:
    ms   = m
    mbs  = (payload / (m / 1000)) / (1024 * 1024)
    print(f"  {label:12s}  {ms:7.1f} ms/msg   {mbs:7.2f} MiB/s")
EOF
    echo ""
done
