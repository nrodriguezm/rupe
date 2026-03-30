from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn
from pipeline.delivery.telegram_digest import build_digest
from pipeline.models import Opportunity


def fetch_assigned(conn, limit: int = 30) -> list[Opportunity]:
    q = """
    select o.source, o.external_id, o.title, coalesce(o.description,''), o.buyer_name,
           o.publish_at, o.deadline_at, o.status, o.amount, o.currency, o.category, o.department,
           o.source_url, o.raw_hash
    from opportunities o
    join opportunity_assignments a on a.opportunity_id = o.id
    order by a.score desc, coalesce(o.deadline_at, now() + interval '365 days') asc
    limit %s
    """
    out = []
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        for r in cur.fetchall():
            out.append(Opportunity(*r))
    return out


def main() -> None:
    with get_conn() as conn:
        ops = fetch_assigned(conn)
    text = build_digest(ops)
    print(json.dumps({"ok": True, "count": len(ops), "digest": text}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
