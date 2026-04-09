from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SQL_PATH = ROOT / "sql" / "analytics_v1.sql"


def apply_sql(conn) -> None:
    sql = SQL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0")
        cur.execute(sql)


def refresh_quality_daily(conn) -> None:
    q = """
    insert into analytics.quality_daily (
      day, opportunities_total, publish_at_filled, category_filled, amount_filled,
      outcomes_filled, attachments_total, attachments_with_text, specs_total,
      buyers_linked, companies_linked
    )
    select
      current_date,
      (select count(*) from opportunities),
      (select count(*) from opportunities where publish_at is not null),
      (select count(*) from opportunities where category is not null),
      (select count(*) from opportunities where amount is not null),
      (select count(*) from opportunity_outcomes),
      (select count(*) from opportunity_attachments),
      (select count(*) from opportunity_attachments where extracted_text is not null and length(extracted_text)>0),
      (select count(*) from attachment_specs),
      (select count(*) from opportunities where buyer_name is not null),
      (select count(*) from suppliers_rupe where identification is not null)
    on conflict (day)
    do update set
      opportunities_total = excluded.opportunities_total,
      publish_at_filled = excluded.publish_at_filled,
      category_filled = excluded.category_filled,
      amount_filled = excluded.amount_filled,
      outcomes_filled = excluded.outcomes_filled,
      attachments_total = excluded.attachments_total,
      attachments_with_text = excluded.attachments_with_text,
      specs_total = excluded.specs_total,
      buyers_linked = excluded.buyers_linked,
      companies_linked = excluded.companies_linked,
      created_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(q)


def refresh_dims(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into analytics.dim_institutions (institution_name, normalized_name, confidence)
            select distinct buyer_name, lower(regexp_replace(buyer_name,'\s+',' ','g')), 0.7
            from opportunities
            where buyer_name is not null
            on conflict (institution_name) do nothing
            """
        )
        inst_added = cur.rowcount

        cur.execute(
            """
            insert into analytics.dim_companies (rupe_identification, legal_name, normalized_name, source_period)
            select distinct identification, legal_name, lower(regexp_replace(legal_name,'\s+',' ','g')), source_period
            from suppliers_rupe
            where legal_name is not null
            on conflict (rupe_identification, legal_name) do nothing
            """
        )
        comp_added = cur.rowcount

    return {"institutions_added": inst_added, "companies_added": comp_added}


def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print(json.dumps({"ok": False, "error": "DATABASE_URL missing"}, ensure_ascii=False))
        return

    with psycopg.connect(dsn) as conn:
        apply_sql(conn)
        dim_stats = refresh_dims(conn)
        refresh_quality_daily(conn)

    print(json.dumps({"ok": True, "sql_applied": str(SQL_PATH), **dim_stats}, ensure_ascii=False))


if __name__ == "__main__":
    main()
