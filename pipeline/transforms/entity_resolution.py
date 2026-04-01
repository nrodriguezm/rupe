from __future__ import annotations

import difflib
import re
import unicodedata


def norm_name(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def resolve_buyer_to_rupe(buyer_name: str | None, suppliers: list[dict], min_score: float = 0.80) -> tuple[dict | None, float]:
    target = norm_name(buyer_name)
    if not target:
        return None, 0.0

    best = None
    best_score = 0.0
    for s in suppliers:
        cand = norm_name(s.get("legal_name"))
        if not cand:
            continue
        score = difflib.SequenceMatcher(None, target, cand).ratio()
        if score > best_score:
            best_score = score
            best = s

    if best_score >= min_score:
        return best, best_score
    return None, best_score
