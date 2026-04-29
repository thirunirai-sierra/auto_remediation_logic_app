from __future__ import annotations

import json
import logging
from typing import List, Optional, Tuple

from agent.observer.error_detector import (
    ROOT_CAUSE_UNKNOWN,
    confidence_score,
    extract_error_code,
    extract_exact_issue,
    extract_error_location,
    extract_error_message,
    infer_root_cause,
)
from agent.rca_agent.llm_rca import generate_rca_with_llm
from common.llm.llm_client import AICoreLLMClient
from agent.observer.models.rca_model import Action, JSONDict, RCAResult
from agent.rca_agent.recommender import recommendation_for_root_cause, solution_for_root_cause

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

    try:
        llm_client = AICoreLLMClient.from_env()
    except Exception as ex:
        logger.error("RCA source: llm_unavailable (%s)", ex)
        msg = "LLM client unavailable; configure AI Core credentials and deployment."
        code = extract_error_code(primary) or "unknown"
        err = extract_error_message(primary)
        location, action_type = extract_error_location(primary, err, flow_context)
        return RCAResult(
            error_location=location,
            action_type=action_type,
            error_code=code,
            root_cause="llm_unavailable",
            exact_issue=msg,
            recommendation=msg,
            solution=msg,
            confidence=0.0,
        ).to_dict()

    llm_result = generate_rca_with_llm(
        action=primary,
        flow_context=flow_context,
        llm_client=llm_client,
        baseline_rca={},
    )
    if llm_result is not None:
        logger.info("RCA source: llm")
        return llm_result.to_dict()
    logger.error("RCA source: llm_failed")
    code = extract_error_code(primary) or "unknown"
    err = extract_error_message(primary)
    location, action_type = extract_error_location(primary, err, flow_context)
    failure_msg = "LLM request failed; no rule-based fallback is enabled."
    return RCAResult(
        error_location=location,
        action_type=action_type,
        error_code=code,
        root_cause="llm_failed",
        exact_issue=failure_msg,
        recommendation="Fix AI Core connectivity/payload and rerun RCA.",
        solution="Fix AI Core connectivity/payload and rerun RCA.",
        confidence=0.0,
    ).to_dict()


def generate_rca_from_error(
    *,
    error_code: str,
    error_message: str,
    error_location: str,
    action_type: str = "system",
    flow_context: Optional[JSONDict] = None,
) -> JSONDict:
    synthetic_action: Action = {
        "name": error_location or "system",
        "status": "Failed",
        "properties": {
            "status": "Failed",
            "error": {
                "code": error_code or "unknown",
                "message": error_message or "",
            },
        },
    }
    try:
        llm_client = AICoreLLMClient.from_env()
    except Exception as ex:
        logger.error("RCA source: llm_unavailable (%s)", ex)
        msg = "LLM client unavailable; configure AI Core credentials and deployment."
        return RCAResult(
            error_location=error_location or "unknown",
            action_type=action_type or "system",
            error_code=error_code or "unknown",
            root_cause="llm_unavailable",
            exact_issue=msg,
            recommendation=msg,
            solution=msg,
            confidence=0.0,
        ).to_dict()

    llm_result = generate_rca_with_llm(
        action=synthetic_action,
        flow_context=flow_context,
        llm_client=llm_client,
        baseline_rca={},
    )
    if llm_result is not None:
        logger.info("RCA source: llm")
        return llm_result.to_dict()

    logger.error("RCA source: llm_failed")
    failure_msg = "LLM request failed; no rule-based fallback is enabled."
    return RCAResult(
        error_location=error_location or "unknown",
        action_type=action_type or "system",
        error_code=error_code or "unknown",
        root_cause="llm_failed",
        exact_issue=failure_msg,
        recommendation="Fix AI Core connectivity/payload and rerun RCA.",
        solution="Fix AI Core connectivity/payload and rerun RCA.",
        confidence=0.0,
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
