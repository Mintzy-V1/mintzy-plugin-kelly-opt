"""Small, dependency-free helper functions shared across the plugin API."""
from pathlib import Path
from typing import Dict, List

from fastapi import HTTPException


def _limit_rows(rows: List[Dict], limit: int) -> List[Dict]:
    if not isinstance(limit, int) or limit <= 0:
        return rows
    if len(rows) > limit:
        return rows[-limit:]
    return rows


def read_csv_data(file_path: Path | str, limit: int = 100) -> List[Dict]:
    """Read CSV content and return the most recent records."""
    import csv

    path = Path(file_path)
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
        limit = max(1, min(limit, 500))
        if len(rows) > limit:
            rows = rows[-limit:]
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read {path.name}: {exc}")


def flatten_numeric_values(obj, numbers):
    """Recursively collect numeric-looking values from nested dict/list."""
    if obj is None:
        return
    if isinstance(obj, dict):
        for v in obj.values():
            flatten_numeric_values(v, numbers)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            flatten_numeric_values(v, numbers)
    else:
        try:
            if isinstance(obj, (int, float)):
                numbers.append(float(obj))
            else:
                s = str(obj).replace(",", "").strip()
                if s.replace(".", "", 1).lstrip("+-").isdigit():
                    numbers.append(float(s))
        except Exception:
            pass


def guess_free_cash_from_resp(resp):
    """Extract free cash from broker response."""
    if resp is None:
        return None
    numbers = []
    candidates = []
    if isinstance(resp, dict):
        if "data" in resp:
            candidates.append(resp["data"])
        if "raw" in resp:
            candidates.append(resp["raw"])
        candidates.append(resp)
    else:
        candidates.append(resp)

    for c in candidates:
        flatten_numeric_values(c, numbers)

    pos = [n for n in numbers if n is not None and n > 0 and n < 1e9]
    if not pos:
        return None
    pos_sorted = sorted(pos)
    large = [n for n in pos_sorted if n >= 100]
    if large:
        return float(large[-1])
    return float(pos_sorted[-1])
