from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

from pipeline.models import Opportunity

DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2})")


def parse_uy_dt(text: str):
    m = DATE_RE.search(text or "")
    if not m:
        return None
    d, mo, y, hh, mm = map(int, m.groups())
    return datetime(y, mo, d, hh, mm)


def mk_hash(data: dict) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _buyer_from_title(title: str) -> str | None:
    # e.g. "Compra Directa ... - Organismo | Unidad"
    if " - " in title and "|" in title:
        parts = title.split(" - ", 1)
        if len(parts) == 2:
            rhs = parts[1]
            pair = rhs.split("|", 1)
            if len(pair) == 2:
                return pair[1].strip()
    return None


def normalize_listing_item(item: dict, source_url: str) -> Opportunity:
    ext_id = item.get("external_id") or mk_hash({"t": item.get("title"), "d": item.get("deadline")})[:16]
    raw_hash = mk_hash(item)
    title = item.get("title", "").strip()
    buyer_name = item.get("buyer_name") or _buyer_from_title(title)
    return Opportunity(
        source="compras_estatales",
        external_id=str(ext_id),
        title=title,
        description=item.get("description", "").strip(),
        buyer_name=buyer_name,
        publish_at=parse_uy_dt(item.get("published", "")),
        deadline_at=parse_uy_dt(item.get("deadline", "")),
        status=item.get("status", "open"),
        amount=item.get("amount"),
        currency=item.get("currency"),
        category=item.get("category"),
        department=item.get("department"),
        source_url=source_url,
        raw_hash=raw_hash,
    )
