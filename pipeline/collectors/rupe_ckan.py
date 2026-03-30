from __future__ import annotations

import json

import re
import urllib.parse
from urllib.request import urlopen

DATASET_URL = "https://catalogodatos.gub.uy/dataset/arce-registro-unico-de-proveedores-del-estado-rupe-2026"
CKAN_PACKAGE_SHOW = "https://catalogodatos.gub.uy/api/3/action/package_show"
DATASET_SLUG = "arce-registro-unico-de-proveedores-del-estado-rupe-2026"


def find_csv_links(html: str) -> list[str]:
    links = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        if "/dataset/" in href:
            continue
        if "csv" in href.lower() or "download" in href.lower():
            if href.startswith("http"):
                links.append(href)
            elif href.startswith("/"):
                links.append("https://catalogodatos.gub.uy" + href)
    out, seen = [], set()
    for x in links:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def ckan_csv_resources(dataset_slug: str = DATASET_SLUG) -> list[dict]:
    q = urllib.parse.urlencode({"id": dataset_slug})
    with urlopen(f"{CKAN_PACKAGE_SHOW}?{q}", timeout=30) as fp:
        data = json.loads(fp.read().decode("utf-8"))
    result = data.get("result", {})
    out = []
    for res in result.get("resources", []):
        fmt = (res.get("format") or "").lower()
        url = res.get("url")
        if fmt == "csv" and url:
            out.append(
                {
                    "id": res.get("id"),
                    "name": res.get("name"),
                    "format": res.get("format"),
                    "url": url,
                    "last_modified": res.get("last_modified"),
                }
            )
    return out


def main() -> None:
    with urlopen(DATASET_URL, timeout=30) as fp:
        html = fp.read().decode("utf-8", errors="replace")
    links = find_csv_links(html)
    resources = []
    try:
        resources = ckan_csv_resources(DATASET_SLUG)
    except Exception:
        resources = []

    payload = {"dataset": DATASET_URL, "csv_resources_html": links, "csv_resources_ckan": resources}
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
