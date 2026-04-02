from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, UTC
from pathlib import Path
from urllib.parse import urljoin

import requests
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn

BASE = "https://www.comprasestatales.gub.uy"
ATT_ROOT = ROOT / "raw_storage" / "attachments"

from pipeline.transforms.upsert_attachments import upsert_many


ATT_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)


def discover_links(html: str, source_url: str) -> list[str]:
    urls = []
    for href in ATT_HREF_RE.findall(html or ""):
        h = href.strip()
        if not h:
            continue
        if any(x in h.lower() for x in [".pdf", "/adjunto", "pliego", "archivo"]):
            urls.append(urljoin(source_url, h))
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def fetch_raw_details(conn, limit: int = 200) -> list[tuple[str, str, str, int]]:
    q = """
    select r.external_id, r.source_url, r.payload_path, o.id as opportunity_id
    from raw_detail_snapshots r
    join opportunities o on o.external_id = r.external_id and o.source='compras_estatales'
    left join opportunity_attachments a on a.external_id = r.external_id
    where r.payload_path is not null
    group by r.external_id, r.source_url, r.payload_path, o.id
    having count(a.id) = 0
    order by max(r.fetched_at) desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return cur.fetchall()


def download_file(url: str) -> tuple[bytes, str | None]:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Referer": BASE + "/consultas/",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    ctype = r.headers.get("content-type")
    return r.content, ctype


def save_attachment(external_id: str, url: str, blob: bytes) -> tuple[str, str, int, str]:
    h = hashlib.sha256(blob).hexdigest()
    now = datetime.now(UTC)
    ext = "pdf" if url.lower().endswith(".pdf") else "bin"
    base = ATT_ROOT / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    base.mkdir(parents=True, exist_ok=True)
    name = f"{external_id}-{h[:20]}.{ext}"
    p = base / name
    p.write_bytes(blob)
    return str(p.relative_to(ROOT)), h, len(blob), name


def extract_pdf_text(path: Path) -> str:
    try:
        reader = PdfReader(str(path))
        pages = []
        for pg in reader.pages[:20]:
            pages.append(pg.extract_text() or "")
        return "\n".join(pages).strip()
    except Exception:
        return ""


def _clean_text(text: str) -> str:
    if not text:
        return ""
    # PostgreSQL text can't store NUL bytes
    return text.replace("\x00", "")


def summarize(text: str) -> str:
    if not text:
        return ""
    t = _clean_text(text)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:600]


def main() -> None:
    rows_out = []
    errors = []
    processed = 0

    with get_conn() as conn:
        items = fetch_raw_details(conn, limit=120)
        for ext_id, source_url, payload_path, opp_id in items:
            try:
                html_path = ROOT / payload_path
                if not html_path.exists():
                    continue
                html = html_path.read_text(encoding="utf-8", errors="replace")
                links = discover_links(html, source_url)
                for u in links[:10]:
                    try:
                        blob, ctype = download_file(u)
                    except Exception as e:
                        errors.append({"id": ext_id, "url": u, "error": str(e)[:200]})
                        continue

                    rel, h, sz, fname = save_attachment(ext_id, u, blob)
                    extracted = ""
                    if fname.lower().endswith(".pdf"):
                        extracted = extract_pdf_text(ROOT / rel)
                    extracted = _clean_text(extracted)
                    rows_out.append(
                        {
                            "opportunity_id": opp_id,
                            "external_id": ext_id,
                            "file_url": u,
                            "file_name": fname,
                            "mime_type": ctype,
                            "file_size_bytes": sz,
                            "file_hash": h,
                            "storage_path": rel,
                            "downloaded_at": datetime.now(UTC),
                            "extraction_status": "ok" if extracted else "none",
                            "extracted_text": extracted or None,
                            "summary": summarize(extracted) or None,
                        }
                    )
                    processed += 1
            except Exception as e:
                errors.append({"id": ext_id, "error": str(e)[:200]})

        n = upsert_many(conn, rows_out)

    print(json.dumps({"ok": True, "processed_files": processed, "upserted": n, "errors": errors[:20]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
