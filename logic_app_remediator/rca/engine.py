from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from logic_app_remediator.rca.analyzer import (
    ROOT_CAUSE_UNKNOWN,
    confidence_score,
    extract_error_code,
    extract_error_location,
    extract_error_message,
    extract_exact_issue,
    infer_root_cause,
    is_complex_case,
)
from logic_app_remediator.rca.llm_rca import generate_rca_with_llm
from logic_app_remediator.rca.models.llm_client import AICoreLLMClient
from logic_app_remediator.rca.models.rca_model import Action, JSONDict, RCAResult
from logic_app_remediator.rca.rag.enricher import enrich_with_rag_and_llm
from logic_app_remediator.rca.recommender import recommendation_for_root_cause, solution_for_root_cause

logger = logging.getLogger(__name__)


def get_failed_actions(actions: List[Action]) -> List[Action]:
    failed: List[Action] = []
    for action in actions:
        props = action.get("properties") or {}
        status = str(action.get("status") or props.get("status") or "")
        if status.lower() == "failed":
            failed.append(action)
    return failed


def pick_primary_failed_action(actions: List[Action]) -> Optional[Action]:
    failed = get_failed_actions(actions)
    if not failed:
        return None

    def sort_key(action: Action) -> Tuple[int, str]:
        message = extract_error_message(action).upper()
        code = extract_error_code(action).upper()
        score = 0
        if "CONTAINS" in message and "NULL" in message:
            score += 400
        if "ECONNREFUSED" in message or "CONNECTION REFUSED" in message:
            score += 300
        if "TIMEOUT" in message or "TIMED OUT" in message:
            score += 250
        if "NOT FOUND" in message or "404" in message:
            score += 200
        if "ACTIONFAILED" in code and len(message) < 180:
            score -= 120
        end_time = str((action.get("properties") or {}).get("endTime") or "")
        return (score, end_time)

    return max(failed, key=sort_key)


def generate_rca_for_action(action: Action, flow_context: Optional[JSONDict] = None) -> RCAResult:
    code = extract_error_code(action)
    message = extract_error_message(action)
    root = infer_root_cause(code, message)
    location, action_type = extract_error_location(action, message, flow_context)
    exact_issue = extract_exact_issue(message, root, flow_context)
    recommendation = recommendation_for_root_cause(root, flow_context)
    solution = solution_for_root_cause(root, flow_context)
    confidence = confidence_score(root, code, message)
    return RCAResult(
        error_location=location,
        action_type=action_type,
        error_code=code or "unknown",
        root_cause=root,
        exact_issue=exact_issue,
        recommendation=recommendation,
        solution=solution,
        confidence=confidence,
    )


def generate_rca(actions: List[Action], flow_context: Optional[JSONDict] = None) -> JSONDict:
    primary = pick_primary_failed_action(actions)
    if primary is None:
        return RCAResult(
            error_location="unknown",
            action_type="unknown",
            error_code="none",
            root_cause=ROOT_CAUSE_UNKNOWN,
            exact_issue="No failed actions found.",
            recommendation="No remediation required.",
            solution="No remediation required.",
            confidence=0.0,
        ).to_dict()
    baseline = generate_rca_for_action(primary, flow_context).to_dict()

    try:
        llm_client = AICoreLLMClient.from_env()
    except Exception:
        logger.info("RCA source: rule_based_fallback")
        return baseline

    root = str(baseline.get("root_cause") or "")
    if is_complex_case(root):
        knowledge_path = str(
            Path(__file__).resolve().parents[1] / "knowledge" / "chunks.jsonl"
        )
        enriched = enrich_with_rag_and_llm(
            baseline,
            flow_context,
            knowledge_path=knowledge_path,
            llm_client=llm_client,
        )
        if isinstance(enriched, dict) and enriched != baseline:
            logger.info("RCA source: rag+llm")
            return enriched
        logger.warning("RCA source: rule_based_fallback (RAG+LLM failed)")
        return baseline

    llm_result = generate_rca_with_llm(
        action=primary,
        flow_context=flow_context,
        llm_client=llm_client,
        baseline_rca=baseline,
    )
    if llm_result is not None:
        logger.info("RCA source: llm")
        return llm_result.to_dict()
    logger.warning("RCA source: rule_based_fallback (LLM failed)")
    return baseline


def generate_rca_from_error(
    *,
    error_code: str,
    error_message: str,
    error_location: str,
    action_type: str = "system",
    flow_context: Optional[JSONDict] = None,
) -> JSONDict:
    root = infer_root_cause(error_code, error_message)
    return RCAResult(
        error_location=error_location or "unknown",
        action_type=action_type or "system",
        error_code=error_code or "unknown",
        root_cause=root,
        exact_issue=extract_exact_issue(error_message, root, flow_context),
        recommendation=recommendation_for_root_cause(root, flow_context),
        solution=solution_for_root_cause(root, flow_context),
        confidence=confidence_score(root, error_code, error_message),
    ).to_dict()


def to_json_output(rca: JSONDict) -> str:
    return json.dumps(
        {
            "error_location": rca.get("error_location", "unknown"),
            "action_type": rca.get("action_type", "unknown"),
            "error_code": rca.get("error_code", "unknown"),
            "root_cause": rca.get("root_cause", ROOT_CAUSE_UNKNOWN),
            "exact_issue": rca.get("exact_issue", ""),
            "recommendation": rca.get("recommendation", ""),
            "solution": rca.get("solution", rca.get("recommendation", "")),
            "confidence": float(rca.get("confidence", 0.0)),
        },
        ensure_ascii=True,
    )
