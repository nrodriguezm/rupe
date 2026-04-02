from __future__ import annotations

import hashlib
import html
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from urllib.request import urlopen

RSS_URL = "https://www.comprasestatales.gub.uy/consultas/rss"
DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})hs")


@dataclass
class RssItem:
    external_id: str
    title: str
    description: str
    source_url: str
    published: str | None
    deadline: str | None
    last_modified: str | None


def _extract_id(url: str) -> str:
    m = re.search(r"/id/([^/?#]+)", url)
    return m.group(1) if m else url


def _strip_html(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_dates(desc: str) -> tuple[str | None, str | None, str | None]:
    norm = html.unescape(desc)
    deadline = None
    published = None
    modified = None

    m_deadline = re.search(r"Recepción de ofertas hasta:\s*([0-9/ :]+)hs", norm, flags=re.IGNORECASE)
    if m_deadline:
        deadline = m_deadline.group(1).strip()

    m_pub = re.search(r"Publicado:\s*([0-9/ :]+)hs", norm, flags=re.IGNORECASE)
    if m_pub:
        published = m_pub.group(1).strip()

    m_mod = re.search(r"Última Modificación:\s*([0-9/ :]+)hs", norm, flags=re.IGNORECASE)
    if m_mod:
        modified = m_mod.group(1).strip()

    return published, deadline, modified


def fetch_xml(url: str = RSS_URL) -> str:
    with urlopen(url, timeout=30) as fp:
        return fp.read().decode("utf-8", errors="replace")


def xml_hash(xml_text: str) -> str:
    return hashlib.sha256(xml_text.encode("utf-8")).hexdigest()


def fetch_items(url: str = RSS_URL, limit: int = 200, xml_text: str | None = None) -> list[RssItem]:
    if xml_text is None:
        xml_text = fetch_xml(url)

    root = ET.fromstring(xml_text.encode("utf-8"))
    out: list[RssItem] = []
    for item in root.findall("./channel/item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc_html = item.findtext("description") or ""
        desc = _strip_html(desc_html)
        pub, deadline, modified = _extract_dates(desc_html)
        out.append(
            RssItem(
                external_id=_extract_id(link),
                title=title,
                description=desc,
                source_url=link,
                published=pub,
                deadline=deadline,
                last_modified=modified,
            )
        )
    return out


def main() -> None:
    items = fetch_items()
    print(
        json.dumps(
            {
                "source": RSS_URL,
                "fetched_at": datetime.now(UTC).isoformat(),
                "count": len(items),
                "items": [asdict(i) for i in items[:10]],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
