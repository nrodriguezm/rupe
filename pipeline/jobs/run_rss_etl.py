from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.collectors.compras_rss import RSS_URL, fetch_items, fetch_xml, xml_hash
from pipeline.db import get_conn
from pipeline.transforms.normalize_opportunities import normalize_listing_item
from pipeline.transforms.upsert_opportunities import upsert_many
from pipeline.transforms.upsert_raw_snapshots import insert_rss_snapshot


def main() -> None:
    xml_text = fetch_xml(RSS_URL)
    rss_items = fetch_items(limit=500, xml_text=xml_text)
    normalized = []
    for it in rss_items:
        item = {
            "external_id": it.external_id,
            "title": it.title,
            "description": it.description,
            "published": it.published or "",
            "deadline": it.deadline or "",
            "status": "open",
        }
        normalized.append(normalize_listing_item(item, it.source_url))

    with get_conn() as conn:
        insert_rss_snapshot(
            conn,
            {
                "source_url": RSS_URL,
                "payload_xml": xml_text,
                "payload_hash": xml_hash(xml_text),
                "item_count": len(rss_items),
            },
        )
        n = upsert_many(conn, normalized)

    print(json.dumps({"ok": True, "fetched": len(rss_items), "upserted": n}, ensure_ascii=False))


if __name__ == "__main__":
    main()
