from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn
from pipeline.models import Opportunity
from pipeline.transforms.score_and_assign import score
from pipeline.transforms.upsert_assignments import upsert_assignments
from pipeline.utils.profile_loader import load_simple_yaml

PROFILE = ROOT / "pipeline/config/businesses/example_school.yaml"


def fetch_open_opportunities(conn, limit: int = 5000) -> list[Opportunity]:
    q = """
    select source, external_id, title, coalesce(description,''), buyer_name,
           publish_at, deadline_at, status, amount, currency, category, department,
           source_url, raw_hash
    from opportunities
    where status = 'open'
    order by coalesce(deadline_at, now() + interval '365 days') asc
    limit %s
    """
    out: list[Opportunity] = []
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        for r in cur.fetchall():
            out.append(
                Opportunity(
                    source=r[0], external_id=r[1], title=r[2], description=r[3], buyer_name=r[4],
                    publish_at=r[5], deadline_at=r[6], status=r[7], amount=r[8], currency=r[9],
                    category=r[10], department=r[11], source_url=r[12], raw_hash=r[13]
                )
            )
    return out


def fetch_opportunity_id_map(conn) -> dict[str, int]:
    m = {}
    with conn.cursor() as cur:
        cur.execute("select id, source, external_id from opportunities")
        for oid, src, ext in cur.fetchall():
            m[f"{src}:{ext}"] = oid
    return m


def ensure_business(conn, business_id: str, name: str) -> None:
    q = """
    insert into businesses (id, name, active)
    values (%s::uuid, %s, true)
    on conflict (id) do update set name = excluded.name
    """
    with conn.cursor() as cur:
        cur.execute(q, (business_id, name))


def main() -> None:
    profile = load_simple_yaml(PROFILE)
    business_id = profile.get("business_id", "11111111-1111-1111-1111-111111111111")
    business_name = profile.get("name", "Default Business")
    threshold = float(profile.get("alert_threshold", 60))

    with get_conn() as conn:
        ensure_business(conn, business_id, business_name)
        opportunities = fetch_open_opportunities(conn)
        idmap = fetch_opportunity_id_map(conn)

        rows = []
        for op in opportunities:
            s, reasons = score(op, profile)
            if s >= threshold:
                key = f"{op.source}:{op.external_id}"
                oid = idmap.get(key)
                if oid:
                    rows.append(
                        {
                            "opportunity_id": oid,
                            "business_id": business_id,
                            "score": s,
                            "reasons": json.dumps(reasons, ensure_ascii=False),
                        }
                    )

        n = upsert_assignments(conn, rows)

    print(json.dumps({"ok": True, "assigned": n, "threshold": threshold}, ensure_ascii=False))


if __name__ == "__main__":
    main()
