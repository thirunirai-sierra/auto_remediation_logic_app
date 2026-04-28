from __future__ import annotations

import re
from typing import List, Optional


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_]+", (text or "").lower())


def jaccard(a: set[str], b: set[str]) -> float:
    union = len(a | b)
    if union == 0:
        return 0.0
    return len(a & b) / union


def normalize_action_name(name: str) -> str:
    raw = (name or "").strip()
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw or "unknown"


def extract_missing_field(message: str) -> Optional[str]:
    m = re.search(r"'([^']+)'\s+is required", message or "", re.I)
    if m:
        return m.group(1)
    m2 = re.search(
        r"required\s+(field|property|parameter)\s*[:=]?\s*['\"]?([A-Za-z0-9_.-]+)",
        message or "",
        re.I,
    )
    if m2:
        return m2.group(2)
    return None
