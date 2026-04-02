from __future__ import annotations

import hashlib
from datetime import datetime, UTC
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "raw_storage"


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def save_raw(kind: str, text: str, ext: str, prefix: str | None = None) -> tuple[str, str, int]:
    h = hash_text(text)
    now = datetime.now(UTC)
    base = RAW_ROOT / kind / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    base.mkdir(parents=True, exist_ok=True)
    fname = f"{prefix + '-' if prefix else ''}{h[:20]}.{ext}"
    path = base / fname
    path.write_text(text, encoding="utf-8")
    rel = str(path.relative_to(ROOT))
    return rel, h, len(text.encode("utf-8"))
