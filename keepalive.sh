#!/bin/bash
# Source from .zshrc/.bashrc to auto-restart the sync daemon on new shells.
#   source /path/to/claude-history-sync/keepalive.sh

SYNC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")" && pwd)"
PID_FILE="$SYNC_DIR/.sync.pid"
JOBS_FILE="$SYNC_DIR/.sync_jobs.json"

[ ! -f "$JOBS_FILE" ] && return 0 2>/dev/null

JOB_COUNT=$(python3 -c "
import json
jobs = json.loads(open('$JOBS_FILE').read())
print(sum(1 for k in jobs if not k.startswith('_')))
" 2>/dev/null)

[ "$JOB_COUNT" = "0" ] 2>/dev/null && return 0 2>/dev/null

# Check if daemon is alive
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
    kill -0 "$OLD_PID" 2>/dev/null && return 0 2>/dev/null
fi

# Daemon is dead or missing — restart with existing jobs
echo "[sync-keepalive] Restarting daemon with $JOB_COUNT job(s)..."
rm -f "$PID_FILE"
python3 "$SYNC_DIR/sync_claude_history.py" --background
