from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.delivery.telegram_digest import build_digest
from pipeline.transforms.normalize_opportunities import normalize_listing_item
from pipeline.transforms.score_and_assign import assign


def run_demo() -> None:
    sample = {
        "external_id": "demo-1",
        "title": "Compra de uniformes escolares para educación primaria",
        "description": "Se requieren remeras, túnicas y pantalones deportivos",
        "published": "23/02/2026 09:15",
        "deadline": "25/02/2026 13:00",
        "status": "open",
    }
    op = normalize_listing_item(sample, "https://www.comprasestatales.gub.uy/consultas/")

    profile_path = Path("pipeline/config/businesses/example_school.yaml")
    # lightweight YAML parse fallback (no pyyaml dependency in v1)
    txt = profile_path.read_text(encoding="utf-8")
    profile = {"keywords_include": [], "keywords_exclude": []}
    for line in txt.splitlines():
        if line.strip().startswith("- "):
            val = line.strip()[2:].strip().lower()
            if any(k in val for k in ["colegio", "educación", "escolar", "uniforme", "túnica", "remera"]):
                profile["keywords_include"].append(val)
            if any(k in val for k in ["hospitalaria", "dragado", "petróleo"]):
                profile["keywords_exclude"].append(val)

    assignments = assign([op], "11111111-1111-1111-1111-111111111111", profile, threshold=30)
    digest = build_digest([op] if assignments else [])
    print(json.dumps({"assignments": [a.__dict__ for a in assignments], "digest": digest}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run_demo()
