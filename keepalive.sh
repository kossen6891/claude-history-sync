#!/bin/bash
# Keeps the sync daemon alive. Works both as:
#   1. Cron job:  */2 * * * * /path/to/keepalive.sh >> /path/to/keepalive.log 2>&1
#   2. Shell source:  source /path/to/keepalive.sh

SYNC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
PID_FILE="$SYNC_DIR/.sync.pid"
JOBS_FILE="$SYNC_DIR/.sync_jobs.json"

_exit() { return $1 2>/dev/null || exit $1; }

[ ! -f "$JOBS_FILE" ] && _exit 0

JOB_COUNT=$(python3 -c "
import json
jobs = json.loads(open('$JOBS_FILE').read())
print(sum(1 for k in jobs if not k.startswith('_')))
" 2>/dev/null)

[ "$JOB_COUNT" = "0" ] && _exit 0

# Check if daemon is alive
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
    kill -0 "$OLD_PID" 2>/dev/null && _exit 0
fi

# Daemon is dead or missing — restart with existing jobs
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restarting daemon with $JOB_COUNT job(s)..."
rm -f "$PID_FILE"
python3 "$SYNC_DIR/sync_claude_history.py" --background
