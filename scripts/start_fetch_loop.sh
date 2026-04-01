#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INTERVAL_MINUTES="${1:-30}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

echo "[loop] starting fetch loop every ${INTERVAL_MINUTES} minutes" | tee -a "$LOG_DIR/pipeline-loop.log"

while true; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$TS] run start" | tee -a "$LOG_DIR/pipeline-loop.log"
  if "$ROOT/scripts/run_pipeline_once.sh"; then
    echo "[$TS] run ok" | tee -a "$LOG_DIR/pipeline-loop.log"
  else
    echo "[$TS] run failed" | tee -a "$LOG_DIR/pipeline-loop.log"
  fi
  sleep "$((INTERVAL_MINUTES * 60))"
done
