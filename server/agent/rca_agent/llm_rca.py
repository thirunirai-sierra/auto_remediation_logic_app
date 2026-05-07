from __future__ import annotations

import json
import logging
from typing import Optional

from agent.observer.error_detector import (
    extract_action_type,
    extract_error_code,
    extract_error_location,
    extract_error_message,
)
from agent.observer.models.rca_model import Action, JSONDict, LLMClient, RCAResult

logger = logging.getLogger(__name__)


def generate_rca_with_llm(
    *,
    action: Action,
    flow_context: Optional[JSONDict],
    llm_client: LLMClient,
    baseline_rca: Optional[JSONDict] = None,
) -> Optional[RCAResult]:
    code = extract_error_code(action)
    message = extract_error_message(action)
    location, action_type = extract_error_location(action, message, flow_context)
    baseline = baseline_rca or {}

    system = (
        "You are an Azure Logic Apps RCA expert.\n"
        "Return only strict JSON with keys exactly:\n"
        '{"error_location":"","action_type":"","error_code":"","root_cause":"",'
        '"exact_issue":"","recommendation":"","solution":"","confidence":0.0}\n'
        "No markdown, no extra keys."
    )
    user = json.dumps(
        {
            "action": action,
            "flow_context": flow_context or {},
            "baseline_rca": baseline_rca or {},
            "constraints": {
                "error_location": location,
                "action_type": action_type or extract_action_type(action, flow_context),
                "error_code": code or "unknown",
            },
        },
        default=str,
    )
    logger.debug("Calling LLM for RCA")
    logger.debug("LLM RCA system_prompt: %s", system)
    logger.debug("LLM RCA user_prompt: %s", user)
    required_keys = [
        "error_location",
        "action_type",
        "error_code",
        "root_cause",
        "exact_issue",
        "recommendation",
        "solution",
        "confidence",
    ]
    out = llm_client.complete_json(
        system_prompt=system,
        user_prompt=user,
        required_keys=required_keys,
    )
    logger.debug("Raw LLM RCA response: %s", out)
    if not isinstance(out, dict):
        logger.debug("LLM RCA response is None or invalid JSON object")
        return None
    if not all(k in out for k in required_keys):
        logger.warning("LLM response missing required keys; applying partial merge with baseline")
    try:
        return RCAResult(
            error_location=str(out.get("error_location") or location or "unknown"),
            action_type=str(out.get("action_type") or action_type or "unknown"),
            error_code=str(out.get("error_code") or code or "unknown"),
            root_cause=str(out.get("root_cause") or baseline.get("root_cause") or "unknown"),
            exact_issue=str(out.get("exact_issue") or baseline.get("exact_issue") or ""),
            recommendation=str(out.get("recommendation") or baseline.get("recommendation") or ""),
            solution=str(
                out.get("solution")
                or out.get("recommendation")
                or baseline.get("solution")
                or baseline.get("recommendation")
                or ""
            ),
            confidence=float(out.get("confidence") or baseline.get("confidence") or 0.0),
        )
    except Exception as ex:
        logger.debug("LLM RCA response parsing failed: %s", ex)
        return None
