from __future__ import annotations

UPSERT_SQL = """
insert into opportunity_outcomes (
  opportunity_id, external_id, winner_name, runner_up_name, outcome_text, source, confidence, parsed_at
) values (
  %(opportunity_id)s, %(external_id)s, %(winner_name)s, %(runner_up_name)s, %(outcome_text)s, %(source)s, %(confidence)s, now()
)
on conflict (external_id)
do update set
  winner_name = excluded.winner_name,
  runner_up_name = excluded.runner_up_name,
  outcome_text = excluded.outcome_text,
  source = excluded.source,
  confidence = excluded.confidence,
  parsed_at = now();
"""


def upsert_many(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    return len(rows)
