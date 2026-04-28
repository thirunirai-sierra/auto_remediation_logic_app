from __future__ import annotations

from typing import List

from logic_app_remediator.rca.models.rca_model import JSONDict
from logic_app_remediator.rca.utils.helpers import jaccard, tokenize


def retrieve_chunks(query: str, chunks: List[JSONDict], top_k: int = 3) -> List[JSONDict]:
    q_tokens = set(tokenize(query))
    if not q_tokens:
        return chunks[: max(1, top_k)]
    scored = []
    for c in chunks:
        text = f"{c.get('title','')} {c.get('text','')} {' '.join(c.get('tags') or [])}"
        tset = set(tokenize(text))
        score = jaccard(q_tokens, tset)
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for s, c in scored if s > 0][: max(1, top_k)]
