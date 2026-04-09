from __future__ import annotations

import json
import os
import time
from datetime import datetime, UTC

import psycopg

QUERIES = {
    "q1_top_institutions": """
        select institution, sum(calls) as calls
        from analytics.v_call_aggregates_by_filters
        where institution <> 'unknown'
        group by 1
        order by 2 desc
        limit 20
    """,
    "q2_dependency": """
        select company, institution, wins
        from analytics.v_company_dependency
        where company <> 'unknown' and institution <> 'unknown'
        order by wins desc
        limit 50
    """,
    "q3_specs_search": """
        select external_id, spec_type
        from analytics.fact_specs
        where spec_text ilike '%servicio%'
        order by spec_id desc
        limit 200
    """,
}


def timed(cur, sql: str):
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    dt = (time.perf_counter() - t0) * 1000
    return dt, len(rows)


def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print(json.dumps({"ok": False, "error": "DATABASE_URL missing"}))
        return

    out = {"ts": datetime.now(UTC).isoformat()}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")

            # quality snapshot
            cur.execute("select count(*) from opportunities")
            out["opportunities_total"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunities where publish_at is null")
            out["publish_null"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunities where amount is not null")
            out["amount_filled"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunity_outcomes")
            out["outcomes"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunity_attachments")
            out["attachments"] = cur.fetchone()[0]
            cur.execute("select count(*) from opportunity_attachments where extracted_text is not null and length(extracted_text)>0")
            out["attachments_with_text"] = cur.fetchone()[0]
            cur.execute("select count(*) from attachment_specs")
            out["specs"] = cur.fetchone()[0]

            # query timings
            timings = {}
            for name, sql in QUERIES.items():
                ms, n = timed(cur, sql)
                timings[name] = {"ms": round(ms, 2), "rows": n}
            out["timings"] = timings

    print(json.dumps({"ok": True, **out}, ensure_ascii=False))


if __name__ == "__main__":
    main()
