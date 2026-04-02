from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_details import parse_detail
from pipeline.db import get_conn


def fetch_latest_raw_details(conn, limit: int = 1000):
    q = """
    select distinct on (external_id) external_id, source_url, payload_html, payload_path, fetched_at
    from raw_detail_snapshots
    order by external_id, fetched_at desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return cur.fetchall()


def update_from_replay(conn, ext_id: str, detail) -> None:
    q = """
    update opportunities
    set description = coalesce(%(description)s, description),
        buyer_name = coalesce(%(buyer)s, buyer_name),
        category = coalesce(%(category)s, category),
        amount = coalesce(%(amount)s, amount),
        currency = coalesce(%(currency)s, currency),
        parser_version = %(parser_version)s,
        parsed_at = now(),
        last_seen_at = now()
    where source='compras_estatales' and external_id=%(external_id)s
    """
    with conn.cursor() as cur:
        cur.execute(
            q,
            {
                "description": detail.body_text,
                "buyer": detail.buyer or detail.organismo,
                "category": detail.category,
                "amount": detail.amount,
                "currency": detail.currency,
                "parser_version": "replay-v1.3",
                "external_id": ext_id,
            },
        )


def main() -> None:
    scanned = 0
    updated = 0
    with get_conn() as conn:
        rows = fetch_latest_raw_details(conn)
        for ext_id, _url, html, payload_path, _ts in rows:
            scanned += 1
            if not html and payload_path:
                p = ROOT / payload_path
                if p.exists():
                    html = p.read_text(encoding="utf-8")
            if not html:
                continue
            d = parse_detail(html, ext_id)
            update_from_replay(conn, ext_id, d)
            updated += 1

    print(json.dumps({"ok": True, "scanned": scanned, "updated": updated}, ensure_ascii=False))


if __name__ == "__main__":
    main()
