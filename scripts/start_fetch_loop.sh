#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INTERVAL_MINUTES="${1:-30}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

SUMMARY_HOUR_UTC="${SUMMARY_HOUR_UTC:-11}"
SUMMARY_MARKER="$LOG_DIR/.last-summary-date"

echo "[loop] starting fetch loop every ${INTERVAL_MINUTES} minutes (daily summary hour UTC: ${SUMMARY_HOUR_UTC})" | tee -a "$LOG_DIR/pipeline-loop.log"

while true; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$TS] run start" | tee -a "$LOG_DIR/pipeline-loop.log"
  if "$ROOT/scripts/run_pipeline_once.sh"; then
    echo "[$TS] run ok" | tee -a "$LOG_DIR/pipeline-loop.log"
  else
    echo "[$TS] run failed" | tee -a "$LOG_DIR/pipeline-loop.log"
  fi

  TODAY_UTC="$(date -u +"%Y-%m-%d")"
  HOUR_UTC="$(date -u +"%H")"
  LAST_SENT="$(cat "$SUMMARY_MARKER" 2>/dev/null || true)"
  if [ "$HOUR_UTC" = "$SUMMARY_HOUR_UTC" ] && [ "$LAST_SENT" != "$TODAY_UTC" ]; then
    echo "[$TS] daily summary run" | tee -a "$LOG_DIR/pipeline-loop.log"
    if [ -x "$ROOT/.venv/bin/python" ]; then
      PY="$ROOT/.venv/bin/python"
    else
      PY="python3"
    fi
    if [ -f "$ROOT/.env" ]; then
      set -a
      # shellcheck disable=SC1091
      source "$ROOT/.env"
      set +a
    fi
    "$PY" "$ROOT/pipeline/jobs/run_daily_summary.py" >> "$LOG_DIR/daily-summary.log" 2>&1 || true
    echo "$TODAY_UTC" > "$SUMMARY_MARKER"
  fi

  sleep "$((INTERVAL_MINUTES * 60))"
done
