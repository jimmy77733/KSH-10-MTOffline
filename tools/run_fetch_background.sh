#!/bin/bash
# 公開 repo KSH-10-MTOffline 用：背景抓取
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLS="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export MT_OFFLINE_ROOT="$ROOT"
export MT_REPO_ROOT="$ROOT"
LOG="$TOOLS/fetch_run.log"
PIDFILE="$TOOLS/fetch.pid"

if [[ -f "$PIDFILE" ]]; then
  OLD="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [[ -n "$OLD" ]] && kill -0 "$OLD" 2>/dev/null; then
    echo "Stopping previous fetch (pid $OLD)..."
    kill "$OLD" 2>/dev/null || true
    sleep 2
  fi
fi
pkill -f 'tools/fetch_historical_data.py' 2>/dev/null || true
pkill -f "$TOOLS/fetch_historical_data.py" 2>/dev/null || true
sleep 1

echo "=== $(date '+%Y-%m-%d %H:%M:%S') fetch start ===" >> "$LOG"
nohup python3 -u "$TOOLS/fetch_historical_data.py" >> "$LOG" 2>&1 &
echo $! > "$PIDFILE"
echo "Started pid $(cat "$PIDFILE"). Log: $LOG"
echo "Status: cat $ROOT/status.json"
