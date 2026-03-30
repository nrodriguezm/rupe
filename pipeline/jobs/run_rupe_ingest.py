from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.rupe_ckan import ckan_csv_resources
from pipeline.db import get_conn
from pipeline.transforms.rupe_parse import parse_rows
from pipeline.transforms.upsert_suppliers_rupe import upsert_many


def pick_latest(resources: list[dict]) -> dict | None:
    if not resources:
        return None

    def score(res: dict) -> tuple[int, str]:
        name = (res.get("name") or "").lower()
        month_weight = 1 if any(m in name for m in ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "setiembre", "septiembre", "octubre", "noviembre", "diciembre"]) else 0
        lm = res.get("last_modified") or ""
        return (month_weight, lm)

    return sorted(resources, key=score, reverse=True)[0]


def main() -> None:
    resources = ckan_csv_resources()
    latest = pick_latest(resources)
    if not latest:
        print(json.dumps({"ok": False, "error": "No RUPE CSV resources found"}, ensure_ascii=False))
        return

    url = latest["url"]
    with urlopen(url, timeout=60) as fp:
        data = fp.read()

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("latin-1")

    period = (latest.get("name") or "rupe").strip()
    rows = parse_rows(text, period)

    # ensure jsonb payload is stringified for SQL binding
    for r in rows:
        r["raw"] = json.dumps(r["raw"], ensure_ascii=False)

    with get_conn() as conn:
        n = upsert_many(conn, rows)

    print(json.dumps({"ok": True, "resource": latest, "parsed": len(rows), "upserted": n}, ensure_ascii=False))


if __name__ == "__main__":
    main()
