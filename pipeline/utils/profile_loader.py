from __future__ import annotations

from pathlib import Path


def load_simple_yaml(path: str | Path) -> dict:
    """Very small YAML subset loader for current profile shape.

    Supports:
    - key: value
    - key:\n    -  - list items
    """
    p = Path(path)
    data: dict = {}
    current_list_key: str | None = None

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        if not line or line.strip().startswith("#"):
            continue

        if line.startswith("  - ") and current_list_key:
            data.setdefault(current_list_key, []).append(line[4:].strip().strip('"'))
            continue

        if ":" in line and not line.startswith("  "):
            k, v = line.split(":", 1)
            k = k.strip()
            v = v.strip()
            if not v:
                current_list_key = k
                data.setdefault(k, [])
            else:
                current_list_key = None
                vv = v.strip().strip('"')
                if vv.replace(".", "", 1).isdigit():
                    data[k] = float(vv) if "." in vv else int(vv)
                else:
                    data[k] = vv
    return data
