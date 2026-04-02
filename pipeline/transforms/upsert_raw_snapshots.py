from __future__ import annotations

UPSERT_RSS = """
insert into raw_rss_snapshots (source_url, payload_xml, payload_path, payload_size_bytes, payload_hash, item_count)
values (%(source_url)s, %(payload_xml)s, %(payload_path)s, %(payload_size_bytes)s, %(payload_hash)s, %(item_count)s)
on conflict (source_url, payload_hash)
do nothing;
"""

UPSERT_DETAIL = """
insert into raw_detail_snapshots (external_id, source_url, payload_html, payload_path, payload_size_bytes, payload_hash)
values (%(external_id)s, %(source_url)s, %(payload_html)s, %(payload_path)s, %(payload_size_bytes)s, %(payload_hash)s)
on conflict (external_id, payload_hash)
do nothing;
"""


def insert_rss_snapshot(conn, row: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(UPSERT_RSS, row)


def insert_detail_snapshot(conn, row: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(UPSERT_DETAIL, row)
