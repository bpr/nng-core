#!/usr/bin/env bash
# bench_large_msg_c_client.sh — measure C libnng streaming PUSH/PULL throughput
# for large message payloads using a persistent connection.
#
# Unlike bench_large_msg_nngcat.sh (which spawns one nngcat process per message),
# this uses push_client + pull_sink from c_bench/: each binary holds a single
# connection and transfers all N messages in a loop, matching the Rust criterion
# benchmark pattern and eliminating per-message process-spawn overhead.
#
# Methodology: run push_client at 5/20/50 messages per payload size, subtract
# wall times to get marginal per-message cost and MiB/s. The sink receives
# exactly N messages and exits, so the port is free before the next run.
#
# Requirements:
#   libnng-dev installed (sudo apt install libnng-dev)
#   gcc in PATH
#
# Usage:
#   ./scripts/bench_large_msg_c_client.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CBENCH_DIR="${SCRIPT_DIR}/../c_bench"
PUSH_CLIENT="${CBENCH_DIR}/push_client"
PULL_SINK="${CBENCH_DIR}/pull_sink"
PORT="19620"
URL="tcp://127.0.0.1:${PORT}"

compile_if_needed() {
    local name="$1"
    local src="${CBENCH_DIR}/${name}.c"
    local bin="${CBENCH_DIR}/${name}"
    if [[ ! -x "${bin}" || "${src}" -nt "${bin}" ]]; then
        echo "Compiling ${name}.c ..."
        gcc -O2 -o "${bin}" "${src}" -lnng
    fi
}

compile_if_needed push_client
compile_if_needed pull_sink

echo "C libnng large-message PUSH/PULL streaming throughput"
echo "  push_client → pull_sink  ${URL}"
echo "  (persistent connection; marginal per-message cost via subtraction)"
echo ""

run_push() {
    local size_bytes="$1"
    local count="$2"

    # Start sink first — it listens; the port is guaranteed bound before push dials
    "${PULL_SINK}" "${URL}" "${count}" &
    local sink_pid=$!
    sleep 0.05  # allow SP handshake

    # Launch push in background, then time how long the sink takes to drain all
    # count messages.  This measures end-to-end delivery (sender→receiver), not
    # just the sender's queue-flush time, and avoids the NNG linger race where
    # nng_close returns before large TCP buffers have been fully sent.
    "${PUSH_CLIENT}" "${URL}" "${size_bytes}" "${count}" &
    local push_pid=$!

    # Use date for timing — { time wait; } 2>&1 | awk creates a nested subshell
    # where wait cannot see processes started in the outer $() subshell.
    local t0 t1 ms
    t0="$(date +%s%3N)"
    wait "${sink_pid}"
    t1="$(date +%s%3N)"
    ms=$(( t1 - t0 ))

    wait "${push_pid}"
    echo "${ms}"
}

for SIZE_MIB in 1 4 16; do
    SIZE_BYTES=$(( SIZE_MIB * 1024 * 1024 ))
    echo "── ${SIZE_MIB} MiB payload ──────────────────────────────────────"

    T5="$(run_push  "${SIZE_BYTES}" 5)";  echo "  5 msgs  ... ${T5} ms"
    T20="$(run_push "${SIZE_BYTES}" 20)"; echo "  20 msgs ... ${T20} ms"
    T50="$(run_push "${SIZE_BYTES}" 50)"; echo "  50 msgs ... ${T50} ms"
    echo ""

    python3 - "${T5}" "${T20}" "${T50}" "${SIZE_BYTES}" <<'EOF'
import sys
t5, t20, t50, payload = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])

for label, n1, t1, n2, t2 in [("20-5  msgs", 5, t5, 20, t20), ("50-20 msgs", 20, t20, 50, t50)]:
    m   = (t2 - t1) / (n2 - n1)
    mbs = (payload / (m / 1000)) / (1024 * 1024)
    print(f"  {label:12s}  {m:7.1f} ms/msg   {mbs:7.2f} MiB/s")
EOF
    echo ""
done
