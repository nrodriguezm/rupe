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


def fetch_text(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_samples(html: str, limit: int = 10) -> list[ListingSample]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # heuristic split around common marker in listing page
    chunks = re.split(r"Recepción de ofertas hasta:\s*", text)
    out: list[ListingSample] = []

    for i, chunk in enumerate(chunks[1:limit + 1], start=1):
        # Title is typically right before deadline marker in previous text region.
        # We reconstruct from current chunk tail/head with lightweight heuristics.
        prev = chunks[i - 1] if i > 0 else ""
        title = prev[-180:].split("Última Modificación")[-1].strip()
        title = re.sub(r"\s+", " ", title)[-120:]

        deadline = chunk.split("Publicado:")[0].strip()[:32]
        published = None
        if "Publicado:" in chunk:
            published = chunk.split("Publicado:")[1].strip()[:32]

        if title:
            out.append(ListingSample(title=title, deadline_text=deadline, published_text=published))

    return out[:limit]


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
