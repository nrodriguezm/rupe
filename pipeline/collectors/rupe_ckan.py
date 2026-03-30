from __future__ import annotations

import csv
import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DATASET_URL = "https://catalogodatos.gub.uy/dataset/arce-registro-unico-de-proveedores-del-estado-rupe-2026"


def find_csv_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/dataset/" in href:
            continue
        if "csv" in (a.get_text(" ", strip=True).lower() + href.lower()):
            if href.startswith("http"):
                links.append(href)
            elif href.startswith("/"):
                links.append("https://catalogodatos.gub.uy" + href)
    # de-dup preserve order
    out = []
    seen = set()
    for x in links:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def main() -> None:
    r = requests.get(DATASET_URL, timeout=30)
    r.raise_for_status()
    links = find_csv_links(r.text)
    payload = {"dataset": DATASET_URL, "csv_resources": links}
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
