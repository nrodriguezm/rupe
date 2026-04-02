from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_conn
from pipeline.transforms.upsert_outcomes import upsert_many

WIN_PATTERNS = [
    re.compile(r"adjudicad[oa]\s+a\s+la\s+(?:empresa|firma|proveedora?)\s+([^\.;,\n]{4,120})", re.IGNORECASE),
    re.compile(r"se\s+adjudica\s+a\s+([^\.;,\n]{4,120})", re.IGNORECASE),
    re.compile(r"adjudicatari[oa]\s*:?\s*([^\.;,\n]{4,120})", re.IGNORECASE),
]
RUNNER_PATTERNS = [
    re.compile(r"segund[oa]\s+ofert[ao]\s*:?\s*([^\.;,\n]{4,120})", re.IGNORECASE),
    re.compile(r"segunda\s+mejor\s+oferta\s*:?\s*([^\.;,\n]{4,120})", re.IGNORECASE),
]


BAD_TOKENS = {"DEBERA", "DEBERÁ", "RESPONSABILIDADES", "GARANTIA", "RECEPCION", "RECEPCIÓN", "PLAZO", "RUPE", "SERA", "SERÁ", "FIRMA DEL CONTRATO", "A LA FECHA", "PRESTADOR", "SERVICIOS"}


def _clean_candidate(s: str | None) -> str | None:
    if not s:
        return None
    c = re.sub(r"\s+", " ", s).strip(" .,-:")
    if len(c) < 4 or len(c) > 120:
        return None
    upper = c.upper()
    if any(tok in upper for tok in BAD_TOKENS):
        return None
    # expect likely company/person-like tokenization
    words = [w for w in re.split(r"\s+", c) if w]
    if len(words) < 2 or len(words) > 10:
        return None
    # avoid sentence-like fragments
    lower_words = sum(1 for w in words if w.islower())
    if lower_words >= 3:
        return None
    return c


def extract_names(text: str) -> tuple[str | None, str | None, float]:
    t = re.sub(r"\s+", " ", text or "")
    winner = None
    runner = None
    conf = 0.0

    for p in WIN_PATTERNS:
        m = p.search(t)
        if m:
            cand = _clean_candidate(m.group(1))
            if cand:
                winner = cand
                conf = 0.75
                break
    for p in RUNNER_PATTERNS:
        m = p.search(t)
        if m:
            cand = _clean_candidate(m.group(1))
            if cand:
                runner = cand
                conf = max(conf, 0.85 if winner else 0.65)
                break

    return winner, runner, conf


def fetch_closed_with_text(conn, limit: int = 800):
    q = """
    select o.id, o.external_id,
           coalesce(o.description,'') || ' ' || coalesce(string_agg(a.extracted_text, ' '), '') as full_text
    from opportunities o
    left join opportunity_attachments a on a.opportunity_id = o.id
    where o.status = 'closed'
    group by o.id, o.external_id, o.deadline_at
    order by o.deadline_at desc nulls last
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (limit,))
        return cur.fetchall()


def main() -> None:
    rows_out = []
    scanned = 0
    with get_conn() as conn:
        rows = fetch_closed_with_text(conn)
        for op_id, ext_id, text in rows:
            scanned += 1
            winner, runner, conf = extract_names(text)
            if winner or runner:
                rows_out.append(
                    {
                        "opportunity_id": op_id,
                        "external_id": ext_id,
                        "winner_name": winner,
                        "runner_up_name": runner,
                        "outcome_text": text[:2000],
                        "source": "details+attachments",
                        "confidence": conf,
                    }
                )
        n = upsert_many(conn, rows_out)

    print(json.dumps({"ok": True, "scanned": scanned, "extracted": n}, ensure_ascii=False))


if __name__ == "__main__":
    main()
