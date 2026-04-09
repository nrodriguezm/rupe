#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

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

END_EPOCH=$(( $(date +%s) + 24*60*60 ))
CYCLE=0

echo "[paths-24h] start $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_DIR/path-experiments.log"

while [ "$(date +%s)" -lt "$END_EPOCH" ]; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$TS] cycle $CYCLE" | tee -a "$LOG_DIR/path-experiments.log"

  # Always refresh analytics + measure
  "$PY" "$ROOT/pipeline/jobs/run_analytics_refresh.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true
  "$PY" "$ROOT/pipeline/jobs/run_path_benchmarks.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true

  # Rotate targeted path work, one heavier task per cycle
  MOD=$(( CYCLE % 4 ))
  if [ "$MOD" -eq 0 ]; then
    # Path B - publish recovery pass
    "$PY" "$ROOT/pipeline/jobs/run_publish_backfill_from_raw.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true
  elif [ "$MOD" -eq 1 ]; then
    # Path D - OCR/attachments prep path currently via archive+attachment processing
    ARCHIVE_EXTRACT_LIMIT=20 "$PY" "$ROOT/pipeline/jobs/run_archive_extract.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true
    "$PY" "$ROOT/pipeline/jobs/run_attachment_ingest.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true
  elif [ "$MOD" -eq 2 ]; then
    # Path C - outcomes extraction
    "$PY" "$ROOT/pipeline/jobs/run_outcomes_extract.py" >> "$LOG_DIR/path-experiments.log" 2>&1 || true
  else
    # Path A - query perf baseline refresh only (already measured)
    true
  fi

  CYCLE=$((CYCLE+1))
  sleep $((60*60))
done

echo "[paths-24h] end $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_DIR/path-experiments.log"
