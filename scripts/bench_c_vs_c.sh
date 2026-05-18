#!/usr/bin/env bash
# bench_c_vs_c.sh — measure nngcat (C libnng) PUSH/PULL marginal per-message cost
#
# Methodology: run nngcat PUSH with three different message counts, subtract
# wall times to isolate the per-message cost from process startup overhead.
#
# Requirements: nngcat in PATH (from system NNG package, e.g. `apt install nng-utils`)
#
# Usage:
#   ./scripts/bench_c_vs_c.sh [payload_bytes]
#
# Example:
#   ./scripts/bench_c_vs_c.sh 256

set -euo pipefail

PAYLOAD_BYTES="${1:-256}"
PORT="19200"
URL="tcp://127.0.0.1:${PORT}"

if ! command -v nngcat &>/dev/null; then
    echo "error: nngcat not found in PATH" >&2
    echo "  Install with: sudo apt install nng-utils  (or equivalent)" >&2
    exit 1
fi

PAYLOAD="$(python3 -c "import sys; sys.stdout.write('x' * ${PAYLOAD_BYTES})")"

echo "C libnng PUSH/PULL marginal per-message cost"
echo "  payload: ${PAYLOAD_BYTES} bytes"
echo "  peer:    nngcat ${URL}"
echo ""

# Start PULL sink in background; no --count so it runs indefinitely
nngcat --pull0 --listen "${URL}" --silent &
PULL_PID=$!
trap 'kill ${PULL_PID} 2>/dev/null; wait ${PULL_PID} 2>/dev/null' EXIT
sleep 0.15  # wait for SP handshake

run_push() {
    local count="$1"
    # Capture wall time in milliseconds
    { time nngcat --push0 --dial "${URL}" --data "${PAYLOAD}" --count "${count}" ; } 2>&1 \
        | awk '/real/ { split($2, a, /[ms]/); print a[1]*60000 + a[2]*1000 }'
}

echo -n "  100 msgs ...   "; T100="$(run_push 100)";   echo "${T100} ms"
echo -n "  10000 msgs ... "; T10K="$(run_push 10000)"; echo "${T10K} ms"
echo -n "  100000 msgs .. "; T100K="$(run_push 100000)"; echo "${T100K} ms"

echo ""
python3 - "${T100}" "${T10K}" "${T100K}" "${PAYLOAD_BYTES}" <<'EOF'
import sys
t100, t10k, t100k, payload = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])

marginal_low  = (t10k  - t100)  / (10000  - 100)    # ms/msg
marginal_high = (t100k - t10k)  / (100000 - 10000)  # ms/msg

for label, m in [("10k-100 msgs", marginal_low), ("100k-10k msgs", marginal_high)]:
    us   = m * 1000
    mbs  = (payload / (m / 1000)) / (1024 * 1024)
    print(f"  {label:18s}  {us:6.1f} µs/msg   {mbs:7.2f} MiB/s")
EOF
