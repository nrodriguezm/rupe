from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn
from pipeline.transforms.normalize_opportunities import _amount_from_text


def main() -> None:
    scanned = 0
    updated = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select id, description from opportunities where amount is null")
            rows = cur.fetchall()
        scanned = len(rows)

        upd = []
        for oid, desc in rows:
            amount, currency = _amount_from_text(desc or "")
            if amount is not None:
                upd.append((amount, currency, oid))

        with conn.cursor() as cur:
            cur.executemany(
                "update opportunities set amount=%s, currency=coalesce(currency,%s) where id=%s",
                upd,
            )
        updated = len(upd)

    print(json.dumps({"ok": True, "scanned": scanned, "updated": updated}, ensure_ascii=False))


if __name__ == "__main__":
    main()
