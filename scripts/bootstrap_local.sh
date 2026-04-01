#!/usr/bin/env bash
set -euo pipefail

# Local bootstrap helper for Debian/Ubuntu-like hosts.
# Installs python venv/pip and project dependencies.

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found" >&2
  exit 1
fi

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3-venv missing. Install with: sudo apt-get install -y python3-venv python3-pip"
  exit 2
fi

cd "$(dirname "$0")/.."
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Bootstrap complete. Activate with: source .venv/bin/activate"
