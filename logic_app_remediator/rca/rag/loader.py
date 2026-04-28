from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from logic_app_remediator.rca.models.rca_model import JSONDict


def load_knowledge_chunks(path: Optional[str]) -> List[JSONDict]:
    if not path:
        return []
    p = Path(path)
    if not p.is_file():
        return []
    rows: List[JSONDict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except json.JSONDecodeError:
                continue
    return rows
