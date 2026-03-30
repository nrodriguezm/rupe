from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from urllib.request import urlopen

DETAIL_PREFIX = "https://www.comprasestatales.gub.uy/consultas/detalle/id/"


@dataclass
class Detail:
    external_id: str
    title: str | None
    body_text: str
    buyer: str | None
    organismo: str | None


def fetch_html(url: str) -> str:
    with urlopen(url, timeout=30) as fp:
        return fp.read().decode("utf-8", errors="replace")


def parse_detail(html: str, external_id: str) -> Detail:
    text = re.sub(r"<script[\s\S]*?<\/script>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?<\/style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # heuristics
    title = None
    m_title = re.search(r"(Compra\s+Directa[^|]{0,200}|Licitación[^|]{0,200}|Solicitud de Información[^|]{0,200})", text, re.IGNORECASE)
    if m_title:
        title = m_title.group(1).strip()

    buyer = None
    m_buyer = re.search(r"\|\s*([^|]{3,120})\s+Recepción de ofertas", text)
    if m_buyer:
        buyer = m_buyer.group(1).strip()

    organismo = None
    m_org = re.search(r"Organismo\s*:?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 .,&\-/]{4,120})", text)
    if m_org:
        organismo = m_org.group(1).strip()

    return Detail(external_id=external_id, title=title, body_text=text[:6000], buyer=buyer, organismo=organismo)


def fetch_detail(external_id: str) -> Detail:
    url = DETAIL_PREFIX + external_id
    return parse_detail(fetch_html(url), external_id)


def main() -> None:
    import sys

    ext = sys.argv[1] if len(sys.argv) > 1 else "1324132"
    d = fetch_detail(ext)
    print(json.dumps(asdict(d), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
