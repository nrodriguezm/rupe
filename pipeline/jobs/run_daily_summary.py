from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print(json.dumps({"ok": False, "error": "DATABASE_URL missing"}, ensure_ascii=False))
        return

    out = {}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from opportunities")
            out["opportunities"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunity_assignments")
            out["assignments"] = cur.fetchone()[0]
            cur.execute("select count(*) from raw_detail_snapshots where synced_at is null")
            out["raw_pending_sync"] = cur.fetchone()[0]
            cur.execute("""
                select o.external_id, o.title, o.deadline_at
                from opportunity_assignments a
                join opportunities o on o.id=a.opportunity_id
                order by a.score desc, o.deadline_at asc
                limit 5
            """)
            out["top5"] = cur.fetchall()

    lines = [
        "📊 Daily pipeline summary",
        f"- opportunities: {out['opportunities']}",
        f"- assignments: {out['assignments']}",
        f"- raw pending sync: {out['raw_pending_sync']}",
        "- top assigned:",
    ]
    for ext, title, ddl in out["top5"]:
        lines.append(f"  • {ext} | {title[:90]} | {ddl}")

    print(json.dumps({"ok": True, "summary": "\n".join(lines), "data": out}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
