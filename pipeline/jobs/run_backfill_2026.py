from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_rss import fetch_items
from pipeline.db import get_conn
from pipeline.transforms.normalize_opportunities import normalize_listing_item
from pipeline.transforms.upsert_opportunities import upsert_many


def build_rss_url(d0: date, d1: date) -> str:
    r0 = f"{d0.isoformat()}+00:00:00"
    r1 = f"{d1.isoformat()}+23:59:59"
    # published-date slices to capture whole 2026 timeline
    return (
        "https://www.comprasestatales.gub.uy/consultas/rss/"
        "tipo-pub/ALL/tipo-fecha/PUB/orden/ORD_PUB/tipo-orden/DESC/"
        f"rango-fecha/{quote_plus(r0)}_{quote_plus(r1)}"
    )


def week_ranges_2026() -> list[tuple[date, date]]:
    start = date(2026, 1, 1)
    end = date(2026, 12, 31)
    ranges = []
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=6), end)
        ranges.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return ranges


def main() -> None:
    total_fetched = 0
    total_upserted = 0
    windows = 0

    with get_conn() as conn:
        for d0, d1 in week_ranges_2026():
            url = build_rss_url(d0, d1)
            items = fetch_items(url=url, limit=2000)
            total_fetched += len(items)
            normalized = []
            for it in items:
                obj = {
                    "external_id": it.external_id,
                    "title": it.title,
                    "description": it.description,
                    "published": it.published or "",
                    "deadline": it.deadline or "",
                    "status": "open",
                }
                normalized.append(normalize_listing_item(obj, it.source_url))
            n = upsert_many(conn, normalized)
            total_upserted += n
            windows += 1

    print(json.dumps({"ok": True, "windows": windows, "fetched": total_fetched, "upserted": total_upserted}, ensure_ascii=False))


if __name__ == "__main__":
    main()
