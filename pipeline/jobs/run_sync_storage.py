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
        "x-upsert": "true",
        "content-type": "application/octet-stream",
    }
    data = file_path.read_bytes()
    r = requests.post(endpoint, headers=headers, data=data, timeout=60)
    if r.status_code in (200, 201):
        return
    # treat conflict/duplicate as already uploaded
    if r.status_code == 409:
        return
    if r.status_code == 400 and "Duplicate" in (r.text or ""):
        return
    raise RuntimeError(f"upload failed {r.status_code}: {r.text[:300]}")


def sync_table(conn, table: str, c: dict) -> tuple[int, int, int]:
    rows = pending(conn, table, c["limit"])
    synced = 0
    attempted = 0
    failed = 0
    for row_id, rel_path in rows:
        attempted += 1
        p = ROOT / rel_path
        if not p.exists():
            failed += 1
            continue
        object_key = rel_path
        try:
            upload_file(c["url"], c["key"], c["bucket"], object_key, p)
            mark_synced(conn, table, row_id, object_key)
            synced += 1
        except Exception:
            failed += 1
    return attempted, synced, failed


def pending_attachments(conn, limit: int) -> list[tuple[int, str]]:
    q = """
    select id, storage_path
    from opportunity_attachments
    where storage_path is not null and storage_object_key is null
    order by id asc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return cur.fetchall()


def mark_attachment_synced(conn, row_id: int, key: str) -> None:
    q = "update opportunity_attachments set storage_object_key=%s, updated_at=now() where id=%s"
    with conn.cursor() as cur:
        cur.execute(q, (key, row_id))


def sync_attachments(conn, c: dict) -> tuple[int, int, int]:
    rows = pending_attachments(conn, c["limit"])
    synced = 0
    attempted = 0
    failed = 0
    for row_id, rel_path in rows:
        attempted += 1
        p = ROOT / rel_path
        if not p.exists():
            failed += 1
            continue
        object_key = rel_path
        try:
            upload_file(c["url"], c["key"], c["bucket"], object_key, p)
            mark_attachment_synced(conn, row_id, object_key)
            synced += 1
        except Exception:
            failed += 1
    return attempted, synced, failed


def main() -> None:
    c = cfg()
    if not c["url"] or not c["key"]:
        print(json.dumps({"ok": False, "error": "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"}, ensure_ascii=False))
        return

    with get_conn() as conn:
        a1, s1, f1 = sync_table(conn, "raw_rss_snapshots", c)
        a2, s2, f2 = sync_table(conn, "raw_detail_snapshots", c)
        a3, s3, f3 = sync_attachments(conn, c)

    print(
        json.dumps(
            {
                "ok": True,
                "bucket": c["bucket"],
                "rss_attempted": a1,
                "rss_synced": s1,
                "rss_failed": f1,
                "detail_attempted": a2,
                "detail_synced": s2,
                "detail_failed": f2,
                "attachment_attempted": a3,
                "attachment_synced": s3,
                "attachment_failed": f3,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
