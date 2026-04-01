from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

from pipeline.models import Opportunity

DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2})")
AMOUNT_RE = re.compile(r"(U\$S|USD|\$)\s*([0-9][0-9\.,]*)", re.IGNORECASE)


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


def _category_from_title(title: str) -> str | None:
    t = title.lower()
    if "licitación pública" in t:
        return "Licitación Pública"
    if "licitación abreviada" in t:
        return "Licitación Abreviada"
    if "concurso de precios" in t:
        return "Concurso de Precios"
    if "compra por excepción" in t:
        return "Compra por Excepción"
    if "compra directa" in t:
        return "Compra Directa"
    if "procedimiento especial" in t:
        return "Procedimiento Especial"
    if "solicitud de información" in t:
        return "Solicitud de Información"
    return None


def _amount_from_text(text: str) -> tuple[float | None, str | None]:
    m = AMOUNT_RE.search(text or "")
    if not m:
        return None, None
    c = m.group(1).upper()
    raw = m.group(2)
    if raw.count(",") > 0 and raw.count(".") > 0:
        num = raw.replace(".", "").replace(",", ".")
    elif raw.count(".") > 1 and raw.count(",") == 0:
        num = raw.replace(".", "")
    elif raw.count(",") > 0 and raw.count(".") == 0:
        num = raw.replace(",", ".")
    else:
        num = raw.replace(",", "")
    try:
        v = float(num)
    except ValueError:
        return None, None
    curr = "USD" if c in {"U$S", "USD"} else "UYU"
    return v, curr


def normalize_listing_item(item: dict, source_url: str) -> Opportunity:
    ext_id = item.get("external_id") or mk_hash({"t": item.get("title"), "d": item.get("deadline")})[:16]
    raw_hash = mk_hash(item)
    title = item.get("title", "").strip()
    desc = item.get("description", "").strip()
    buyer_name = item.get("buyer_name") or _buyer_from_title(title)
    category = item.get("category") or _category_from_title(title)
    amount = item.get("amount")
    currency = item.get("currency")
    if amount is None:
        amount, currency2 = _amount_from_text(desc)
        currency = currency or currency2

    return Opportunity(
        source="compras_estatales",
        external_id=str(ext_id),
        title=title,
        description=desc,
        buyer_name=buyer_name,
        publish_at=parse_uy_dt(item.get("published", "")),
        deadline_at=parse_uy_dt(item.get("deadline", "")),
        status=item.get("status", "open"),
        amount=amount,
        currency=currency,
        category=category,
        department=item.get("department"),
        source_url=source_url,
        raw_hash=raw_hash,
    )
