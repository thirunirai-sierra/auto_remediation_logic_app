"""Fixer agent service: RCA-driven rule fix with optional LLM fallback."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

from api import get_workflow, put_workflow
from auth import get_arm_token
from common.llm.model import get_llm
from config import get_settings
from agent.fixer.fix_rules import apply_rule_to_action
from agent.fixer.utils import clone_workflow_definition, extract_failed_action_name, locate_action
from agent.rca_agent.engine import generate_rca_from_error

logger = logging.getLogger(__name__)

JSONDict = Dict[str, Any]


def fetch_workflow_definition(workflow_name: str) -> JSONDict:
    settings = get_settings()
    token = get_arm_token(settings.tenant_id, settings.client_id, settings.client_secret)
    return get_workflow(
        token=token,
        subscription_id=settings.subscription_id,
        resource_group=settings.resource_group,
        workflow_name=workflow_name,
    )


def get_rca_analysis(
    error_code: str,
    error_message: str,
    failed_action_name: str,
    workflow_name: str,
) -> JSONDict:
    flow_context = {
        "workflow_name": workflow_name,
        "failed_action_name": failed_action_name,
    }
    return generate_rca_from_error(
        error_code=error_code or "unknown",
        error_message=error_message,
        error_location=failed_action_name or "unknown",
        action_type="unknown",
        flow_context=flow_context,
    )


def apply_fix_from_rca(
    workflow_definition: JSONDict,
    failed_action_name: str,
    rca_result: JSONDict,
) -> JSONDict:
    """
    Rule-based fixer:
    - modifies only the failed action
    - keeps workflow structure unchanged
    """
    fixed = clone_workflow_definition(workflow_definition)
    actions = fixed.get("actions", {})
    target = locate_action(actions, failed_action_name)
    if not isinstance(target, dict):
        logger.warning("Failed action '%s' not found in workflow definition", failed_action_name)
        return {
            "applied": False,
            "fix_name": "action_not_found",
            "fixed_definition": fixed,
        }

    root_cause = str((rca_result or {}).get("root_cause") or "unknown")
    logger.info("Root cause detected for rule-based fix: %s", root_cause)
    applied, fix_name = apply_rule_to_action(target, root_cause)
    if applied:
        logger.info("Rule-based fix applied: %s", fix_name)
    else:
        logger.info("No rule-based fix applied for root cause: %s", root_cause)

    return {
        "applied": applied,
        "fix_name": fix_name,
        "fixed_definition": fixed,
    }


def call_llm_for_fix(
    error_message: str,
    failed_action_name: str,
    workflow_definition: JSONDict,
    rca_result: Optional[JSONDict] = None,
) -> JSONDict:
    """LLM fallback fixer only when rule-based fixer is not applicable."""
    rca_context = ""
    if rca_result:
        rca_context = json.dumps(
            {
                "root_cause": rca_result.get("root_cause"),
                "exact_issue": rca_result.get("exact_issue"),
                "recommendation": rca_result.get("recommendation"),
                "solution": rca_result.get("solution"),
                "confidence": rca_result.get("confidence"),
            },
            default=str,
        )

    prompt = (
        "You are an Azure Logic App expert. Return strict JSON only with keys: "
        'root_cause, recommendation_steps, fixed_definition.\n'
        f"ERROR_MESSAGE: {error_message}\n"
        f"FAILED_ACTION: {failed_action_name}\n"
        f"RCA: {rca_context}\n"
        f"WORKFLOW_DEFINITION: {json.dumps(workflow_definition, default=str)[:120000]}\n"
    )
    llm = get_llm(temperature=0.0)
    resp = llm.invoke(prompt)
    content = getattr(resp, "content", "") or ""
    match = re.search(r"\{[\s\S]*\}", content)
    if not match:
        raise ValueError("LLM fallback did not return JSON")
    out = json.loads(match.group(0))
    if not isinstance(out.get("fixed_definition"), dict):
        raise ValueError("LLM fallback missing valid fixed_definition")
    if not isinstance(out.get("recommendation_steps"), list):
        out["recommendation_steps"] = [str(out.get("recommendation_steps") or "Review suggested fix")]
    return out


def apply_fixed_definition(workflow_name: str, fixed_definition: JSONDict) -> JSONDict:
    settings = get_settings()
    token = get_arm_token(settings.tenant_id, settings.client_id, settings.client_secret)
    current_workflow = get_workflow(
        token=token,
        subscription_id=settings.subscription_id,
        resource_group=settings.resource_group,
        workflow_name=workflow_name,
    )
    current_workflow["properties"]["definition"] = fixed_definition
    return put_workflow(
        token=token,
        subscription_id=settings.subscription_id,
        resource_group=settings.resource_group,
        workflow_name=workflow_name,
        workflow_body=current_workflow,
        etag=current_workflow.get("etag"),
    )
