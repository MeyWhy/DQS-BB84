#!/usr/bin/env bash
# =============================================================
# launch.sh — BB84 Decentralized Agent Architecture
#
# Start order:
#   1. Redis
#   2. QKDL  (QuNetSim quantum link layer)
#   3. KME   (Key Management Entity / session registry)
#   4. Nodes (Alice, Bob, ... from network.yaml)
#
# Usage:
#   ./launch.sh              # start everything
#   ./launch.sh --no-nodes   # start infrastructure only
#
# Session flow (after launch):
#   curl -X POST http://localhost:8001/start \
#     -H "Content-Type: application/json" \
#     -d '{"receiver_label":"bob-1","n_qubits":200}'
#
#   # Poll KME for status:
#   curl http://localhost:8000/sessions/{session_id}
#
#   # Consume key (one-time):
#   curl -X POST http://localhost:8000/sessions/{session_id}/consume-key
# =============================================================

set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
LOGS="$ROOT/logs"
mkdir -p "$LOGS"

export PYTHONPATH="$ROOT:$PYTHONPATH"
export KME_URL="http://localhost:8000"
export QKDL_URL="http://localhost:8003"
export REDIS_URL="redis://localhost:6379/0"

PIDS=()
cleanup() {
    echo ""
    echo "Stopping all services..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    redis-cli shutdown nosave 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT INT TERM

svc() {
    local name=$1 port=$2; shift 2
    echo "  Starting $name (port $port)..."
    python -m uvicorn "$@" --host 0.0.0.0 --port "$port" \
        --log-level warning > "$LOGS/${name}.log" 2>&1 &
    PIDS+=($!)
    sleep 1
}

# ── Redis ──────────────────────────────────────────────────────
redis-cli ping &>/dev/null || {
    echo "  Starting Redis..."
    redis-server --daemonize yes --logfile "$LOGS/redis.log" --port 6379
    sleep 1
}
echo "  Redis ready"

# ── QKDL ──────────────────────────────────────────────────────
svc qkdl 8003 qkdl.main:app

# ── KME ───────────────────────────────────────────────────────
svc kme 8000 kme.main:app
sleep 1

# ── Nodes (from network.yaml) ─────────────────────────────────
if [[ "$1" != "--no-nodes" ]]; then
    echo "  Starting nodes from network.yaml..."
    python -m node.node_runner > "$LOGS/node_runner.log" 2>&1 &
    PIDS+=($!)
    sleep 3
fi

# ── Health checks ─────────────────────────────────────────────
echo ""
echo "Health checks:"
for port in 8003 8000 8001 8002; do
    resp=$(curl -s "http://localhost:$port/health" 2>/dev/null \
           || echo '{"error":"unreachable"}')
    printf "  :%s → %s\n" "$port" "$resp"
done

echo ""
echo "System ready."
echo "  KME  : http://localhost:8000/docs"
echo "  Alice: http://localhost:8001/docs"
echo "  Bob  : http://localhost:8002/docs"
echo "  QKDL : http://localhost:8003/docs"
echo ""
echo "Start a session:"
echo "  curl -X POST http://localhost:8001/start \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"receiver_label\":\"bob-1\",\"n_qubits\":200}'"
echo ""
echo "Ctrl+C to stop."
wait
