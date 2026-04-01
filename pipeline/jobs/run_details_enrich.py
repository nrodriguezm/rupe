from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_details import fetch_detail
from pipeline.db import get_conn


def pending_ids(conn, limit: int = 50) -> list[str]:
    q = """
    select external_id
    from opportunities
    where source = 'compras_estatales'
      and (description is null or length(description) < 120)
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [r[0] for r in cur.fetchall()]


def update_detail(conn, ext_id: str, description: str, buyer: str | None, category: str | None, amount_raw: str | None, currency: str | None) -> None:
    q = """
    update opportunities
    set description = %(description)s,
        buyer_name = coalesce(%(buyer)s, buyer_name),
        category = coalesce(%(category)s, category),
        amount = coalesce(
          case
            when %(amount_raw)s is not null then nullif(replace(regexp_replace(%(amount_raw)s, '[^0-9.,]', '', 'g'), ',', ''), '')::numeric
            else null
          end,
          amount
        ),
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
                "amount_raw": amount_raw,
                "currency": currency,
                "external_id": ext_id,
            },
        )


def main() -> None:
    updated = 0
    errors = []

    with get_conn() as conn:
        ids = pending_ids(conn)
        for ext_id in ids:
            try:
                d = fetch_detail(ext_id)
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
