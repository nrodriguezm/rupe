from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


JOBS = [
    [sys.executable, str(ROOT / "pipeline/jobs/run_rupe_ingest.py")],
    [sys.executable, str(ROOT / "pipeline/jobs/run_rss_etl.py")],
    [sys.executable, str(ROOT / "pipeline/jobs/run_details_enrich.py")],
    [sys.executable, str(ROOT / "pipeline/jobs/run_entity_resolution.py")],
    [sys.executable, str(ROOT / "pipeline/jobs/run_assignment_etl.py")],
    [sys.executable, str(ROOT / "pipeline/jobs/run_digest.py")],
]


def main() -> None:
    results = []
    for cmd in JOBS:
        p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        results.append(
            {
                "cmd": " ".join(cmd),
                "returncode": p.returncode,
                "stdout": p.stdout.strip()[-2000:],
                "stderr": p.stderr.strip()[-2000:],
            }
        )
        if p.returncode != 0:
            break
    print(json.dumps({"ok": all(r["returncode"] == 0 for r in results), "runs": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
