#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLS="$(cd "$(dirname "$0")" && pwd)"
export MT_OFFLINE_ROOT="$ROOT"
export MT_REPO_ROOT="$ROOT"
PORT="${FETCH_DASHBOARD_PORT:-8765}"
if ! lsof -i ":$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  nohup python3 "$TOOLS/fetch_status_server.py" >> "$TOOLS/fetch_dashboard.log" 2>&1 &
  echo $! > "$TOOLS/fetch_dashboard.pid"
  sleep 0.8
fi
open "http://127.0.0.1:$PORT/"
