from __future__ import annotations

import csv
import io
import unicodedata


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()


def detect_field(row: dict, choices: list[str]) -> str | None:
    keys = {_norm(k): k for k in row.keys()}
    for c in choices:
        cn = _norm(c)
        for low, orig in keys.items():
            if cn in low:
                return orig
    return None


def parse_rows(csv_text: str, period: str) -> list[dict]:
    head = (csv_text.splitlines()[0] if csv_text else "")
    delimiter = ";" if head.count(";") > head.count(",") else ","
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return []

    sample = rows[0]
    f_country = detect_field(sample, ["pais", "país", "country"])
    f_id = detect_field(sample, ["ident", "rut", "documento"])
    f_name = detect_field(sample, ["denomin", "razon", "razón", "social", "nombre"])
    f_addr = detect_field(sample, ["domicilio", "direccion", "dirección"])
    f_loc = detect_field(sample, ["localidad", "ciudad"])
    f_dep = detect_field(sample, ["departamento"])
    f_status = detect_field(sample, ["estado", "situacion", "situación"])

    out = []
    for r in rows:
        out.append(
            {
                "source_period": period,
                "country": (r.get(f_country) if f_country else None),
                "identification": (r.get(f_id) if f_id else None),
                "legal_name": (r.get(f_name) if f_name else None),
                "fiscal_address": (r.get(f_addr) if f_addr else None),
                "locality": (r.get(f_loc) if f_loc else None),
                "department": (r.get(f_dep) if f_dep else None),
                "status": (r.get(f_status) if f_status else None),
                "raw": r,
            }
        )
    return out
