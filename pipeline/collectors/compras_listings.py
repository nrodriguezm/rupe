from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, UTC

import requests
from bs4 import BeautifulSoup

URL = "https://www.comprasestatales.gub.uy/consultas/"


@dataclass
class ListingSample:
    title: str
    deadline_text: str | None
    published_text: str | None
    last_modified_text: str | None = None


def fetch_text(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_samples(html: str, limit: int = 10) -> list[ListingSample]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # title + reception + publication (+ optional last-modification)
    pattern = re.compile(
        r"(?P<title>.*?)\s*Recepción de ofertas hasta:\s*(?P<deadline>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}hs)"
        r"\s*Publicado:\s*(?P<published>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}hs)"
        r"(?:\s*\|\s*Última Modificación:\s*(?P<modified>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}hs))?",
        flags=re.IGNORECASE,
    )

    out: list[ListingSample] = []
    for m in pattern.finditer(text):
        raw_title = m.group("title")
        # keep only the tail since this is global page text
        title = raw_title[-220:]
        title = re.sub(r"\s+", " ", title).strip(" -|\n\t")
        # remove obvious menu noise if present
        for marker in ["Se encontraron", "Filtrando por", "Categorías"]:
            if marker in title:
                title = title.split(marker)[-1].strip()

        out.append(
            ListingSample(
                title=title,
                deadline_text=m.group("deadline"),
                published_text=m.group("published"),
                last_modified_text=m.group("modified"),
            )
        )
        if len(out) >= limit:
            break

    return out


def main() -> None:
    html = fetch_text(URL)
    samples = parse_samples(html)
    payload = {
        "source": URL,
        "fetched_at": datetime.now(UTC).isoformat(),
        "count": len(samples),
        "samples": [asdict(s) for s in samples],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
