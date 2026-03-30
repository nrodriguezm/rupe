from __future__ import annotations

from typing import Iterable

UPSERT_SQL = """
insert into suppliers_rupe (
  source_period, country, identification, legal_name, fiscal_address,
  locality, department, status, raw
) values (
  %(source_period)s, %(country)s, %(identification)s, %(legal_name)s, %(fiscal_address)s,
  %(locality)s, %(department)s, %(status)s, %(raw)s::jsonb
)
on conflict (source_period, identification, legal_name)
do update set
  country = excluded.country,
  fiscal_address = excluded.fiscal_address,
  locality = excluded.locality,
  department = excluded.department,
  status = excluded.status,
  raw = excluded.raw;
"""


def upsert_many(conn, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    return len(rows)
