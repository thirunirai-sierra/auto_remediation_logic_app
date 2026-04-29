from __future__ import annotations

import json
from typing import Optional

from agent.knowledge.rag.loader import load_knowledge_chunks
from agent.knowledge.rag.retriever import retrieve_chunks
from agent.observer.error_detector import is_complex_case
from agent.observer.models.rca_model import JSONDict, LLMClient


def enrich_with_rag_and_llm(
    baseline_rca: JSONDict,
    flow_context: Optional[JSONDict],
    *,
    knowledge_path: Optional[str] = None,
    llm_client: Optional[LLMClient] = None,
    top_k: int = 3,
) -> JSONDict:
    if llm_client is None:
        return baseline_rca

    root = str(baseline_rca.get("root_cause") or "")
    complex_case = is_complex_case(root)
    chunks = load_knowledge_chunks(knowledge_path)

    query = " ".join(
        [
            str(baseline_rca.get("error_code") or ""),
            str(baseline_rca.get("root_cause") or ""),
            str(baseline_rca.get("exact_issue") or ""),
            str((flow_context or {}).get("failed_action_name") or ""),
            str((flow_context or {}).get("action_type") or ""),
        ]
    )
    retrieved = retrieve_chunks(query=query, chunks=chunks, top_k=top_k) if chunks else []
    if complex_case and not retrieved:
        return baseline_rca

    packed = "\n\n".join(
        f"[{i+1}] id={c.get('id','')} title={c.get('title','')}\n{c.get('text','')}"
        for i, c in enumerate(retrieved)
    )
    guidance = (
        "Provide a short, practical solution in 1 sentence."
        if not complex_case
        else "Provide a KB-grounded solution using retrieved knowledge and flow context."
    )
    system = (
        "You are an Azure Logic Apps RCA expert. Refine RCA using supplied knowledge and live context. "
        f"{guidance} "
        "Return strict JSON only with keys: "
        "error_location, action_type, error_code, root_cause, exact_issue, recommendation, solution, confidence."
    )
    user = json.dumps(
        {
            "baseline_rca": baseline_rca,
            "flow_context": flow_context or {},
            "retrieved_knowledge": packed,
        },
        default=str,
    )
    enriched = llm_client.complete_json(system_prompt=system, user_prompt=user)
    if not isinstance(enriched, dict):
        return baseline_rca

    out = dict(baseline_rca)
    for key in (
        "error_location",
        "action_type",
        "error_code",
        "root_cause",
        "exact_issue",
        "recommendation",
        "solution",
        "confidence",
    ):
        if key in enriched and enriched[key]:
            out[key] = enriched[key]
    if not out.get("solution"):
        out["solution"] = out.get("recommendation", "")
    out["rag_enriched"] = True
    out["retrieved_sources"] = [{"id": c.get("id"), "title": c.get("title")} for c in retrieved]
    return out
