"""
Execute one agent step and return updated envelope fields.
Used by /api/agents/{agent}/pipeline and Event Mesh workers.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Tuple

from config import Settings, get_settings
from services.agents.classifier.analyzer import analyze_error, classify_error
from services.agents.fixer.Fixer_agent import get_fixer
from services.agents.observer import Observer
from services.agents.orchestrator import Orchestrator
from services.agents.rca.engine import generate_rca
from services.auth import get_arm_token
from services.event_mesh.messages import PipelineEnvelope
from services.remediation_tracker import get_tracker
from services.workflow_service import (
    find_manual_or_recurrence_trigger,
    get_workflow,
    post_trigger_run,
    should_skip_remediate_newer_succeeded,
)
from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score

logger = logging.getLogger(__name__)


async def run_observer(envelope: PipelineEnvelope, settings: Settings) -> Tuple[Dict[str, Any], str]:
    observer = Observer(settings)
    result = observer.analyze_failed_run(
        envelope.subscription_id,
        envelope.resource_group,
        envelope.workflow_name,
        envelope.run_id,
    )
    if result.get("status") != "failed_action_found":
        return result, "failed"

    ec = result.get("error_context") or {}
    out = {
        "status": "success",
        "failed_action": result.get("failed_action_name"),
        "failed_action_path": result.get("failed_action_path"),
        "failed_action_obj": result.get("failed_action"),
        "error_message": ec.get("error_message", ""),
        "error_code": ec.get("error_code", ""),
        "status_code": ec.get("status_code"),
        "error_context": ec,
    }
    return out, "success"


async def run_classifier(envelope: PipelineEnvelope, settings: Settings) -> Tuple[Dict[str, Any], str]:
    obs = envelope.observer
    if obs.get("status") != "success":
        return {"status": "skipped", "reason": "observer failed"}, "failed"

    ec = obs.get("error_context") or {}
    msg = ec.get("error_message") or obs.get("error_message", "")
    code = ec.get("error_code") or obs.get("error_code", "")
    status_code = ec.get("status_code") or obs.get("status_code")

    orch = Orchestrator(settings)
    error_type = orch._classify_error_rule_based(msg, code, status_code)
    if error_type is None:
        error_type = await classify_error(msg, code, status_code, settings)

    if error_type == "unknown":
        result = await analyze_error(
            error_json={"code": code, "message": msg},
            settings=settings,
            flow_context={"workflow_name": envelope.workflow_name},
        )
        return {
            "error_type": result.get("error_type", "unknown"),
            "root_cause": result.get("root_cause"),
            "recommendation": result.get("recommendation"),
            "confidence": result.get("confidence"),
            "status": "success",
        }, "success" if result.get("error_type") != "unknown" else "failed"

    return {"error_type": error_type, "status": "success"}, "success"


async def run_rca(envelope: PipelineEnvelope, settings: Settings) -> Tuple[Dict[str, Any], str]:
    obs = envelope.observer
    cls = envelope.classifier
    if cls.get("status") == "failed" or obs.get("status") != "success":
        return {"status": "skipped"}, "failed"

    error_type = cls.get("error_type", "unknown")
    ec = obs.get("error_context") or {}
    failed_action = obs.get("failed_action_obj") or {"type": "unknown"}

    try:
        result = await asyncio.wait_for(
            generate_rca(failed_action, ec, error_type, settings),
            timeout=180.0,
        )
    except Exception as exc:
        logger.warning("RCA failed, fallback: %s", exc)
        root = infer_root_cause(ec.get("error_code", ""), ec.get("error_message", ""))
        result = {
            "root_cause": root,
            "exact_issue": extract_exact_issue(ec.get("error_message", ""), root, ec),
            "suggested_fix": "",
            "confidence": confidence_score(root, ec.get("error_code", ""), ec.get("error_message", "")),
            "solution": "",
        }

    result["status"] = "success"
    return result, "success"


async def run_fixer(envelope: PipelineEnvelope, settings: Settings) -> Tuple[Dict[str, Any], str]:
    obs = envelope.observer
    cls = envelope.classifier
    rca = envelope.rca
    if rca.get("status") == "failed" or cls.get("status") == "failed":
        return {"status": "skipped"}, "failed"

    fixer = get_fixer(settings)
    rca_result = {
        "root_cause": rca.get("root_cause", ""),
        "exact_issue": rca.get("exact_issue", rca.get("suggested_fix", "")),
        "solution": rca.get("solution", ""),
    }
    workflow_context = {
        "workflow_name": envelope.workflow_name,
        "run_id": envelope.run_id,
        "subscription_id": envelope.subscription_id,
        "resource_group": envelope.resource_group,
        "failed_action_name": obs.get("failed_action"),
        "failed_action_path": obs.get("failed_action_path"),
        "suggested_fix": rca.get("suggested_fix"),
        "error_type": cls.get("error_type", "unknown"),
        "error_context": obs.get("error_context") or {},
    }

    if getattr(settings, "DRY_RUN", False):
        return {"success": False, "error": "Dry run mode", "status": "skipped"}, "failed"

    result = await asyncio.to_thread(fixer.fix, rca_result, workflow_context)
    if result.get("success"):
        get_tracker().mark_run_remediated(
            run_id=envelope.run_id,
            workflow_name=envelope.workflow_name,
            error_type=cls.get("error_type", "unknown"),
            workflow_definition=result.get("workflow_definition"),
            fix_strategy=(result.get("fix_strategy") or {}).get("strategy_description"),
            root_cause=rca.get("root_cause"),
        )
        return {**result, "status": "success"}, "success"
    return {**result, "status": "failed"}, "failed"


async def run_verifier(envelope: PipelineEnvelope, settings: Settings) -> Tuple[Dict[str, Any], str]:
    fixer = envelope.fixer
    if not fixer.get("success"):
        return {"verified": False, "reason": "fixer did not succeed", "status": "failed"}, "failed"

    token = get_arm_token(
        settings.AZURE_TENANT_ID,
        settings.AZURE_CLIENT_ID,
        settings.AZURE_CLIENT_SECRET,
    )
    try:
        workflow = get_workflow(
            token,
            envelope.subscription_id,
            envelope.resource_group,
            envelope.workflow_name,
        )
        definition = workflow.get("properties", {}).get("definition", {})
        trigger = find_manual_or_recurrence_trigger(definition)
        if not trigger:
            return {
                "verified": False,
                "reason": "No manual/recurrence trigger",
                "status": "skipped",
            }, "success"

        resp = post_trigger_run(
            token,
            envelope.subscription_id,
            envelope.resource_group,
            envelope.workflow_name,
            trigger,
            {},
        )
        ok = resp.status_code in (200, 202)
        return {
            "verified": ok,
            "trigger_used": trigger,
            "status_code": resp.status_code,
            "status": "success" if ok else "failed",
        }, "success" if ok else "failed"
    except Exception as exc:
        return {"verified": False, "reason": str(exc)[:200], "status": "failed"}, "failed"


RUNNERS = {
    "observer": run_observer,
    "classifier": run_classifier,
    "rca": run_rca,
    "fixer": run_fixer,
    "verifier": run_verifier,
}


async def run_agent_step(agent: str, envelope: PipelineEnvelope) -> PipelineEnvelope:
    settings = get_settings()
    runner = RUNNERS.get(agent)
    if not runner:
        envelope.status = "failed"
        envelope.error = f"unknown agent: {agent}"
        return envelope

    # Pre-check on observer only
    if agent == "observer" and getattr(settings, "SKIP_IF_NEWER_RUN_SUCCEEDED", True):
        try:
            token = get_arm_token(
                settings.AZURE_TENANT_ID,
                settings.AZURE_CLIENT_ID,
                settings.AZURE_CLIENT_SECRET,
            )
            skip, _ = should_skip_remediate_newer_succeeded(
                token,
                envelope.subscription_id,
                envelope.resource_group,
                envelope.workflow_name,
                envelope.run_id,
            )
            if skip:
                envelope.status = "skipped"
                envelope.error = "newer_run_succeeded"
                return envelope
        except Exception as exc:
            logger.warning("skip check failed: %s", exc)

    if agent == "observer" and get_tracker().is_run_already_remediated(envelope.run_id):
        envelope.status = "skipped"
        envelope.error = "already_remediated"
        return envelope

    result, step_status = await runner(envelope, settings)
    setattr(envelope, agent, result)

    if step_status == "failed":
        envelope.status = "failed"
        envelope.error = result.get("error") or result.get("reason") or f"{agent} step failed"
    elif agent == "verifier":
        envelope.status = "remediated" if result.get("verified") else "completed"
    elif agent == "fixer" and result.get("success"):
        envelope.status = "fix_deployed"
    else:
        envelope.status = "in_progress"

    return envelope
