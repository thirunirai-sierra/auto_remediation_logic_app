from __future__ import annotations

import re
from typing import List, Optional


def tokenize(text: str) -> List[str]:
    """
    Tokenize input text into lowercase alphanumeric and underscore tokens.

    Args:
        text (str): Input string.

    Returns:
        List[str]: List of normalized tokens.
    """
    return re.findall(r"[a-z0-9_]+", (text or "").lower())


def jaccard(a: set[str], b: set[str]) -> float:
    """
    Compute Jaccard similarity between two token sets.

    Args:
        a (set[str]): First token set.
        b (set[str]): Second token set.

    Returns:
        float: Similarity score between 0.0 and 1.0.
    """
    union = len(a | b)
    if union == 0:
        return 0.0
    return len(a & b) / union


def normalize_action_name(name: str) -> str:
    """
    Normalize Azure Logic App action name.

    - Removes workflow path prefixes
    - Extracts last segment after '/'

    Args:
        name (str): Raw action name.

    Returns:
        str: Normalized action name or 'unknown' if empty.
    """
    raw = (name or "").strip()
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw or "unknown"


def extract_missing_field(message: str) -> Optional[str]:
    """
    Extract missing field name from validation error messages.

    Supports patterns like:
    - "'field' is required"
    - "required field: fieldName"
    - "required parameter 'fieldName'"

    Args:
        message (str): Error message string.

    Returns:
        Optional[str]: Missing field name if detected, otherwise None.
    """
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
