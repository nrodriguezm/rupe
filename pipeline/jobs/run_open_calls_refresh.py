from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn


def main() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update opportunities
                set status = case
                  when deadline_at is not null and deadline_at < now() then 'closed'
                  else 'open'
                end,
                last_seen_at = now()
                where source='compras_estatales'
                """
            )
            updated = cur.rowcount

            cur.execute(
                """
                create or replace view v_open_calls_with_deadlines as
                select
                  id,
                  external_id,
                  title,
                  buyer_name,
                  category,
                  publish_at,
                  deadline_at,
                  case
                    when deadline_at is null then null
                    when deadline_at < now() then 'expired'
                    when deadline_at <= now() + interval '24 hours' then 'due_<24h'
                    when deadline_at <= now() + interval '72 hours' then 'due_<72h'
                    when deadline_at <= now() + interval '7 days' then 'due_<7d'
                    else 'due_later'
                  end as urgency_bucket,
                  source_url
                from opportunities
                where status='open'
                order by deadline_at asc nulls last
                """
            )

            cur.execute("select count(*) from opportunities where status='open'")
            open_count = cur.fetchone()[0]
            cur.execute("select count(*) from opportunities where status='closed'")
            closed_count = cur.fetchone()[0]
            cur.execute("select urgency_bucket, count(*) from v_open_calls_with_deadlines group by 1 order by 2 desc")
            buckets = cur.fetchall()

    print(json.dumps({"ok": True, "updated": updated, "open": open_count, "closed": closed_count, "urgency": buckets}, ensure_ascii=False))


if __name__ == "__main__":
    main()
