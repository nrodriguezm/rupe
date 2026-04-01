from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn
from pipeline.transforms.entity_resolution import resolve_buyer_to_rupe


def fetch_suppliers(conn, limit: int = 10000) -> list[dict]:
    q = """
    select identification, legal_name, department, status
    from suppliers_rupe
    order by id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [
            {
                "identification": r[0],
                "legal_name": r[1],
                "department": r[2],
                "status": r[3],
            }
            for r in cur.fetchall()
        ]


def fetch_opps(conn, limit: int = 500) -> list[tuple[int, str | None]]:
    q = """
    select id, buyer_name
    from opportunities
    where source='compras_estatales' and buyer_name is not null
    order by id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [(r[0], r[1]) for r in cur.fetchall()]


def ensure_columns(conn) -> None:
    q = """
    alter table opportunities
      add column if not exists buyer_rupe_identification text,
      add column if not exists buyer_rupe_name text,
      add column if not exists buyer_match_score numeric;
    """
    with conn.cursor() as cur:
        cur.execute(q)


def update_match(conn, op_id: int, supplier: dict | None, score: float) -> None:
    q = """
    update opportunities
    set buyer_rupe_identification = %(ident)s,
        buyer_rupe_name = %(name)s,
        buyer_match_score = %(score)s
    where id = %(id)s
    """
    with conn.cursor() as cur:
        cur.execute(
            q,
            {
                "ident": supplier.get("identification") if supplier else None,
                "name": supplier.get("legal_name") if supplier else None,
                "score": score,
                "id": op_id,
            },
        )


def main() -> None:
    matched = 0
    scanned = 0
    with get_conn() as conn:
        ensure_columns(conn)
        suppliers = fetch_suppliers(conn)
        opps = fetch_opps(conn)
        for op_id, buyer_name in opps:
            scanned += 1
            s, score = resolve_buyer_to_rupe(buyer_name, suppliers, min_score=0.82)
            update_match(conn, op_id, s, score)
            if s:
                matched += 1

    print(json.dumps({"ok": True, "scanned": scanned, "matched": matched}, ensure_ascii=False))


if __name__ == "__main__":
    main()
