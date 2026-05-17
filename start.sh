#!/usr/bin/env bash
# start.sh : lancer the full QKD stack

# Usage:
# ./start.sh            # 2 pairs (default)
# ./start.sh 1          # 1 pair only (alice-1/bob-1 + qkdl:8003)
#
# Requires:
# - redis-server on PATH
# - Python env with all dependencies installed

set -e
PAIRS=${1:-2}

echo "=== QKD Stack startup (pairs=$PAIRS) ==="

#Redis 
if ! redis-cli ping &>/dev/null; then
    echo "[start] Starting Redis..."
    redis-server --daemonize yes --logfile logs/redis.log
    sleep 1
else
    echo "[start] Redis already running."
fi

#Build QKDL_URLS for KME 
QKDL_URLS="http://localhost:8003"
if [ "$PAIRS" -ge 2 ]; then
    QKDL_URLS="$QKDL_URLS,http://localhost:8013"
fi
if [ "$PAIRS" -ge 3 ]; then
    QKDL_URLS="$QKDL_URLS,http://localhost:8023"
fi

echo "[start] QKDL pool: $QKDL_URLS"

#KME 
mkdir -p logs
echo "[start] Starting KME on :8000..."
QKDL_URLS="$QKDL_URLS" python kme/main.py > logs/kme.log 2>&1 &
KME_PID=$!
echo "[start] KME pid=$KME_PID"
sleep 2

#Nodes + QKDLs via node_runner 
echo "[start] Starting node_runner (pairs=$PAIRS)..."
python -m node.node_runner
# node_runner blocks and handles Ctrl+C

#Cleanup on exit 
echo "[start] Shutting down KME (pid=$KME_PID)..."
kill $KME_PID 2>/dev/null || true
