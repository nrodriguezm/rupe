from __future__ import annotations

from typing import Iterable

from pipeline.models import Assignment

UPSERT_ASSIGN_SQL = """
insert into opportunity_assignments (opportunity_id, business_id, score, reasons, assigned_at)
values (%(opportunity_id)s, %(business_id)s, %(score)s, %(reasons)s::jsonb, now())
on conflict (opportunity_id, business_id)
do update set
  score = excluded.score,
  reasons = excluded.reasons,
  assigned_at = now();
"""


def upsert_assignments(conn, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_ASSIGN_SQL, rows)
    return len(rows)
