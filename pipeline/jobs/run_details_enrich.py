from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_details import DETAIL_PREFIX, fetch_html, parse_detail
from pipeline.db import get_conn
from pipeline.storage_local import save_raw
from pipeline.transforms.upsert_raw_snapshots import insert_detail_snapshot


def pending_ids(conn, limit: int = 50) -> list[str]:
    q = """
    select external_id
    from opportunities
    where source = 'compras_estatales'
      and (
        buyer_name is null
        or category is null
        or amount is null
      )
    order by id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [r[0] for r in cur.fetchall()]


def update_detail(conn, ext_id: str, description: str, buyer: str | None, category: str | None, amount_val: float | None, currency: str | None) -> None:
    q = """
    update opportunities
    set description = %(description)s,
        buyer_name = coalesce(%(buyer)s, buyer_name),
        category = coalesce(%(category)s, category),
        amount = coalesce(%(amount_val)s, amount),
        currency = coalesce(%(currency)s, currency),
        last_seen_at = now()
    where source = 'compras_estatales' and external_id = %(external_id)s
    """
    with conn.cursor() as cur:
        cur.execute(
            q,
            {
                "description": description,
                "buyer": buyer,
                "category": category,
                "amount_val": amount_val,
                "currency": currency,
                "external_id": ext_id,
            },
        )


def main() -> None:
    updated = 0
    errors = []
    batch_size = 50
    max_batches = 10

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")

        for _ in range(max_batches):
            ids = pending_ids(conn, limit=batch_size)
            if not ids:
                break
            for ext_id in ids:
                try:
                    with conn.transaction():
                        url = DETAIL_PREFIX + ext_id
                        html = fetch_html(url)
                        d = parse_detail(html, ext_id)
                        payload_path, payload_hash, payload_size = save_raw("detail", html, "html", prefix=ext_id)
                        insert_detail_snapshot(
                            conn,
                            {
                                "external_id": ext_id,
                                "source_url": url,
                                "payload_html": None,
                                "payload_path": payload_path,
                                "payload_size_bytes": payload_size,
                                "payload_hash": payload_hash,
                            },
                        )
                        update_detail(
                            conn,
                            ext_id,
                            d.body_text,
                            d.buyer or d.organismo,
                            d.category,
                            d.amount,
                            d.currency,
                        )
                        updated += 1
                except Exception as e:
                    errors.append({"id": ext_id, "error": str(e)})

    print(json.dumps({"ok": len(errors) == 0, "updated": updated, "errors": errors[:10]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
