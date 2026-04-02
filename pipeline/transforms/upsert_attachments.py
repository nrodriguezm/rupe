from __future__ import annotations

UPSERT_SQL = """
insert into opportunity_attachments (
  opportunity_id, external_id, file_url, file_name, mime_type, file_size_bytes,
  file_hash, storage_path, downloaded_at, extraction_status, extracted_text, summary, updated_at
) values (
  %(opportunity_id)s, %(external_id)s, %(file_url)s, %(file_name)s, %(mime_type)s, %(file_size_bytes)s,
  %(file_hash)s, %(storage_path)s, %(downloaded_at)s, %(extraction_status)s, %(extracted_text)s, %(summary)s, now()
)
on conflict (external_id, file_url)
do update set
  file_name = excluded.file_name,
  mime_type = excluded.mime_type,
  file_size_bytes = excluded.file_size_bytes,
  file_hash = excluded.file_hash,
  storage_path = excluded.storage_path,
  downloaded_at = excluded.downloaded_at,
  extraction_status = excluded.extraction_status,
  extracted_text = excluded.extracted_text,
  summary = excluded.summary,
  updated_at = now();
"""


def upsert_many(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    return len(rows)
