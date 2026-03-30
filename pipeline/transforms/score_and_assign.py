from __future__ import annotations

from pipeline.models import Assignment, Opportunity


def score(op: Opportunity, profile: dict) -> tuple[float, dict]:
    text = f"{op.title} {op.description}".lower()
    include = [k.lower() for k in profile.get("keywords_include", [])]
    exclude = [k.lower() for k in profile.get("keywords_exclude", [])]

    hits = [k for k in include if k in text]
    bad = [k for k in exclude if k in text]

    fit = min(40, len(hits) * 10)
    penalty = min(30, len(bad) * 15)

    urgency = 0
    if op.deadline_at:
        urgency = 10

    score_value = max(0, min(100, fit + urgency - penalty))
    reasons = {"include_hits": hits, "exclude_hits": bad, "fit": fit, "urgency": urgency, "penalty": penalty}
    return score_value, reasons


def assign(opportunities: list[Opportunity], business_id: str, profile: dict, threshold: float = 60) -> list[Assignment]:
    out: list[Assignment] = []
    for op in opportunities:
        s, reasons = score(op, profile)
        if s >= threshold:
            out.append(Assignment(business_id=business_id, score=s, reasons=reasons))
    return out
