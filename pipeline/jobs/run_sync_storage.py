from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn


def cfg() -> dict:
    return {
        "url": os.getenv("SUPABASE_URL", "").rstrip("/"),
        "key": os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""),
        "bucket": os.getenv("SUPABASE_STORAGE_BUCKET", "rupe-raw"),
        "limit": int(os.getenv("SYNC_LIMIT", "200")),
    }


def pending(conn, table: str, limit: int) -> list[tuple[int, str]]:
    q = f"""
    select id, payload_path
    from {table}
    where payload_path is not null and synced_at is null
    order by id asc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return cur.fetchall()


def mark_synced(conn, table: str, row_id: int, key: str) -> None:
    q = f"update {table} set synced_at=now(), storage_object_key=%s where id=%s"
    with conn.cursor() as cur:
        cur.execute(q, (key, row_id))


def upload_file(base_url: str, service_key: str, bucket: str, object_key: str, file_path: Path) -> None:
    endpoint = f"{base_url}/storage/v1/object/{bucket}/{object_key}"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "x-upsert": "false",
        "content-type": "application/octet-stream",
    }
    data = file_path.read_bytes()
    r = requests.post(endpoint, headers=headers, data=data, timeout=60)
    if r.status_code in (200, 201):
        return
    # treat conflict as already uploaded
    if r.status_code == 409:
        return
    raise RuntimeError(f"upload failed {r.status_code}: {r.text[:300]}")


def sync_table(conn, table: str, c: dict) -> tuple[int, int]:
    rows = pending(conn, table, c["limit"])
    synced = 0
    attempted = 0
    for row_id, rel_path in rows:
        attempted += 1
        p = ROOT / rel_path
        if not p.exists():
            continue
        object_key = rel_path
        upload_file(c["url"], c["key"], c["bucket"], object_key, p)
        mark_synced(conn, table, row_id, object_key)
        synced += 1
    return attempted, synced


def main() -> None:
    c = cfg()
    if not c["url"] or not c["key"]:
        print(json.dumps({"ok": False, "error": "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"}, ensure_ascii=False))
        return

    with get_conn() as conn:
        a1, s1 = sync_table(conn, "raw_rss_snapshots", c)
        a2, s2 = sync_table(conn, "raw_detail_snapshots", c)

    print(
        json.dumps(
            {
                "ok": True,
                "bucket": c["bucket"],
                "rss_attempted": a1,
                "rss_synced": s1,
                "detail_attempted": a2,
                "detail_synced": s2,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
