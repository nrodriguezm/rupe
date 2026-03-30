from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_listings import URL, fetch_text, parse_samples
from pipeline.db import get_conn
from pipeline.transforms.normalize_opportunities import normalize_listing_item
from pipeline.transforms.upsert_opportunities import upsert_many


def sample_to_item(sample) -> dict:
    title = re.sub(r"\s+", " ", sample.title).strip()
    return {
        "title": title,
        "description": title,
        "published": (sample.published_text or "").replace("hs", ""),
        "deadline": (sample.deadline_text or "").replace("hs", ""),
        "status": "open",
    }


def main() -> None:
    html = fetch_text(URL)
    samples = parse_samples(html, limit=50)
    ops = [normalize_listing_item(sample_to_item(s), URL) for s in samples if s.title]

    inserted = 0
    try:
        with get_conn() as conn:
            inserted = upsert_many(conn, ops)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "parsed": len(ops)}, ensure_ascii=False))
        return

    print(json.dumps({"ok": True, "parsed": len(ops), "upserted": inserted}, ensure_ascii=False))


if __name__ == "__main__":
    main()
