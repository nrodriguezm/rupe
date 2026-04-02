from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_details import DETAIL_PREFIX, fetch_html
from pipeline.db import get_conn
from pipeline.transforms.upsert_raw_snapshots import insert_detail_snapshot


def pending_ids(conn, limit: int = 500) -> list[str]:
    q = """
    select o.external_id
    from opportunities o
    left join (
      select external_id, max(fetched_at) as last_raw
      from raw_detail_snapshots
      group by external_id
    ) r on r.external_id = o.external_id
    where o.source='compras_estatales' and r.external_id is null
    order by o.id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    done = 0
    errs = []
    with get_conn() as conn:
        ids = pending_ids(conn)
        for ext_id in ids:
            try:
                url = DETAIL_PREFIX + ext_id
                html = fetch_html(url)
                insert_detail_snapshot(
                    conn,
                    {
                        "external_id": ext_id,
                        "source_url": url,
                        "payload_html": html,
                        "payload_hash": hashlib.sha256(html.encode("utf-8")).hexdigest(),
                    },
                )
                done += 1
            except Exception as e:
                errs.append({"id": ext_id, "error": str(e)})

    print(json.dumps({"ok": len(errs) == 0, "captured": done, "errors": errs[:10]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
