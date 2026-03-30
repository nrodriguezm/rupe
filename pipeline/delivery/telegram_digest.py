from __future__ import annotations

from pipeline.models import Opportunity


def build_digest(ops: list[Opportunity]) -> str:
    if not ops:
        return "Sin oportunidades nuevas relevantes."
    lines = ["📌 Resumen de oportunidades relevantes:"]
    for op in ops[:15]:
        deadline = op.deadline_at.strftime("%d/%m %H:%M") if op.deadline_at else "s/d"
        lines.append(f"- {op.title} | cierre: {deadline}")
        lines.append(f"  {op.source_url}")
    return "\n".join(lines)
