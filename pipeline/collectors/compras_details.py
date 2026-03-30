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
    raw_h1 = None
    m_h1 = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", html, flags=re.IGNORECASE)
    if m_h1:
        raw_h1 = re.sub(r"<[^>]+>", " ", m_h1.group(1))
        raw_h1 = re.sub(r"\s+", " ", raw_h1).strip()

    text = re.sub(r"<script[\s\S]*?<\/script>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?<\/style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # trim leading global nav noise keeping first procurement marker
    marker = re.search(r"(Compra\s+Directa|Licitación|Solicitud de Información)", text, re.IGNORECASE)
    if marker:
        text = text[marker.start():]

    title = raw_h1
    if not title:
        m_title = re.search(
            r"((?:Compra\s+Directa|Licitación|Solicitud de Información)[^\n]{0,220}?)(?:\s+Recepci[oó]n de ofertas|\s+Fecha Publicaci[oó]n|\s+Archivo adjunto)",
            text,
            re.IGNORECASE,
        )
        if m_title:
            title = m_title.group(1).strip(" -|")

    # fallback title as first sentence-like segment
    if not title:
        title = text[:180]

    buyer = None
    organismo = None

    # most common pattern: "... - Organismo | Unidad"
    if title and " - " in title and "|" in title:
        rhs = title.split(" - ", 1)[1]
        organismo = rhs.split("|", 1)[0].strip()
        buyer = rhs.split("|", 1)[1].strip()

    if not (organismo and buyer):
        m_pair = re.search(
            r"(?:Compra\s+Directa|Licitación|Solicitud de Información)[^\-]{0,80}-\s*([^|]{3,120})\|\s*(.{3,140}?)(?=\s+Recepci[oó]n de ofertas|\s+Fecha Publicaci[oó]n|\s+Archivo adjunto|$)",
            text,
            re.IGNORECASE,
        )
        if m_pair:
            organismo = m_pair.group(1).strip()
            buyer = m_pair.group(2).strip()

    # fallbacks
    if not organismo:
        m_org = re.search(r"Organismo\s*:?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 .,&\-/]{4,120})", text)
        if m_org:
            organismo = m_org.group(1).strip()

    if not buyer:
        m_buyer = re.search(r"\|\s*([^|]{3,120})\s+Recepción de ofertas", text)
        if m_buyer:
            buyer = m_buyer.group(1).strip()

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
