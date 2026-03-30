from __future__ import annotations

from typing import Iterable

from pipeline.models import Opportunity

UPSERT_SQL = """
insert into opportunities (
  source, external_id, title, description, buyer_name, buyer_entity_id,
  publish_at, deadline_at, status, amount, currency, category, department,
  source_url, raw_hash, first_seen_at, last_seen_at
) values (
  %(source)s, %(external_id)s, %(title)s, %(description)s, %(buyer_name)s, %(buyer_entity_id)s,
  %(publish_at)s, %(deadline_at)s, %(status)s, %(amount)s, %(currency)s, %(category)s, %(department)s,
  %(source_url)s, %(raw_hash)s, now(), now()
)
on conflict (source, external_id)
do update set
  title = excluded.title,
  description = excluded.description,
  buyer_name = excluded.buyer_name,
  publish_at = excluded.publish_at,
  deadline_at = excluded.deadline_at,
  status = excluded.status,
  amount = excluded.amount,
  currency = excluded.currency,
  category = excluded.category,
  department = excluded.department,
  source_url = excluded.source_url,
  raw_hash = excluded.raw_hash,
  last_seen_at = now();
"""


def to_row(op: Opportunity) -> dict:
    return {
        "source": op.source,
        "external_id": op.external_id,
        "title": op.title,
        "description": op.description,
        "buyer_name": op.buyer_name,
        "buyer_entity_id": None,
        "publish_at": op.publish_at,
        "deadline_at": op.deadline_at,
        "status": op.status,
        "amount": op.amount,
        "currency": op.currency,
        "category": op.category,
        "department": op.department,
        "source_url": op.source_url,
        "raw_hash": op.raw_hash,
    }


def upsert_many(conn, opportunities: Iterable[Opportunity]) -> int:
    rows = [to_row(o) for o in opportunities]
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    return len(rows)
