"""
Orchestrator Agent - Coordinates Observer → RCA Agent → Fixer → Azure deployment.
Uses LLM throughout for intelligent root cause analysis and fix generation.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from api.services.workflow_service import (
    get_run,
    get_workflow,
    list_run_actions,
    find_manual_or_recurrence_trigger,
    post_trigger_run,
    get_latest_run_status,
)
from auth import get_arm_token
from config import Settings, get_settings

logger = logging.getLogger(__name__)

_RUNTIME_SNAPSHOT_MAX_CHARS = 16000


def _failed_action_runtime_snapshot(failed_action: Dict[str, Any]) -> str:
    """Serialize failed-step inputs/outputs so Fixer/LLM see resolved paths/payloads."""
    props = failed_action.get("properties") or {}
    snap = {"inputs": props.get("inputs"), "outputs": props.get("outputs")}
    try:
        txt = json.dumps(snap, default=str)
        if len(txt) > _RUNTIME_SNAPSHOT_MAX_CHARS:
            return txt[: _RUNTIME_SNAPSHOT_MAX_CHARS - 24] + "\n...<truncated>"
        return txt
    except Exception:
        return ""


# Import the RCA Agent (generates RCAResult via LLM)
try:
    from agent.rca_agent.engine import generate_rca, pick_primary_failed_action
except ImportError:
    generate_rca = None
    pick_primary_failed_action = None

# Import Fixer Agent (applies fixes via LLM)
try:
    from agent.fixer.Fixer_agent import FixerAgent

    logger.info("FixerAgent loaded successfully")
except ImportError as e:
    logger.warning("FixerAgent import error: %s", e)
    FixerAgent = None


def run_remediation(
    workflow_name: str,
    run_id: str,
    subscription_id: str,
    resource_group: str,
    settings: Optional[Settings] = None,
    backup_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Main orchestration: Observer → RCA Agent → Fixer → Azure
    
    Flow:
    1. Get failed run (Observer)
    2. Analyze with RCA Agent (LLM-based root cause)
    3. Generate fix with Fixer Agent (LLM-based fix generation)
    4. Deploy to Azure
    5. Optional: trigger test run
    """
    settings = settings or get_settings()
    
    logger.info("=" * 70)
    logger.info(f"[ORCHESTRATOR] Starting: {workflow_name}/{run_id}")
    logger.info("=" * 70)
    
    try:
        # === GET TOKEN ===
        token = get_arm_token(
            settings.tenant_id,
            settings.client_id,
            settings.client_secret,
        )
        
        # === CHECK: Is workflow already healthy? ===
        logger.info("[CHECK] Checking workflow's latest run status...")
        latest_status = get_latest_run_status(
            token, subscription_id, resource_group, workflow_name
        )
        logger.info(f"[CHECK] Workflow '{workflow_name}' latest status: {latest_status}")
        
        if latest_status.lower() == "succeeded":
            logger.info(f"[CHECK] Skipping - workflow already healthy (latest run succeeded)")
            return {
                "status": "skipped",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "message": "Workflow already healthy - latest run succeeded",
            }
        
        # === OBSERVER: Get run details ===
        logger.info("[OBSERVER] Fetching failed run...")
        
        run = get_run(token, subscription_id, resource_group, workflow_name, run_id)
        run_status = run.get("properties", {}).get("status", "")
        
        if run_status.lower() != "failed":
            return {
                "status": "no_error",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "message": f"Run status: {run_status}",
            }
        
        # === OBSERVER: Get actions and identify failed one ===
        actions = list_run_actions(token, subscription_id, resource_group, workflow_name, run_id)
        
        failed_action_name = None
        failed_action = None
        
        # Use smart action picker if available (prioritizes real errors over generic "dependent failed")
        if pick_primary_failed_action:
            failed_action = pick_primary_failed_action(actions)
            if failed_action:
                failed_action_name = failed_action.get("name", "").split("/")[-1]
                logger.info(f"[OBSERVER] Primary failed action identified: {failed_action_name}")
        
        # Fallback to simple loop if picker not available or didn't find anything
        if not failed_action:
            for action in actions:
                props = action.get("properties", {})
                if props.get("status", "").lower() == "failed":
                    failed_action_name = action.get("name", "").split("/")[-1]
                    failed_action = action
                    break
        
        if not failed_action:
            return {
                "status": "no_error",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "message": "No failed action found",
            }
        
        logger.info(f"[OBSERVER] Failed action: {failed_action_name}")
        
        # === RCA AGENT: LLM-based root cause analysis ===
        logger.info("[RCA-AGENT] Running root cause analysis...")
        
        if not generate_rca:
            logger.warning("[RCA-AGENT] RCA module not available, using fallback")
            rca_result = _fallback_rca(failed_action, workflow_name)
        else:
            try:
                flow_context = {
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "failed_action_name": failed_action_name,
                    "action_inputs_preview": str(failed_action.get("properties", {}).get("inputs", "")),
                    "action_outputs_preview": str(failed_action.get("properties", {}).get("outputs", "")),
                }
                rca_result = generate_rca([failed_action], flow_context=flow_context)
            except Exception as e:
                logger.warning(f"[RCA-AGENT] RCA failed: {e}, using fallback")
                rca_result = _fallback_rca(failed_action, workflow_name)
        
        logger.info(f"[RCA-AGENT] Root cause: {rca_result.get('root_cause')}")
        logger.info(f"[RCA-AGENT] Issue: {rca_result.get('exact_issue')}")
        
        # === FIXER AGENT: LLM-based fix generation ===
        logger.info("[FIXER-AGENT] Generating intelligent fix...")
        
        if not FixerAgent:
            logger.error("[FIXER-AGENT] Fixer module not available")
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error": "Fixer agent not available",
            }
        
        fixer = FixerAgent(settings)
        
        workflow_context = {
            "workflow_name": workflow_name,
            "run_id": run_id,
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "failed_action_name": failed_action_name,
            "backup_dir": backup_dir,
            "failed_action_runtime_json": _failed_action_runtime_snapshot(failed_action),
        }
        
        try:
            fix_result = fixer.fix(rca_result, workflow_context)
        except Exception as e:
            logger.error(f"[FIXER-AGENT] Fix failed: {e}", exc_info=True)
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error": str(e),
            }
        
        if not fix_result.get("success"):
            error_msg = fix_result.get("error", "Unknown error")
            logger.error(f"[FIXER-AGENT] Deployment failed: {error_msg}")
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error": error_msg,
            }
        
        logger.info("[FIXER-AGENT] Fix deployed to Azure")
        
        # === VERIFY: Optional test run ===
        try:
            workflow = get_workflow(token, subscription_id, resource_group, workflow_name)
            definition = workflow.get("properties", {}).get("definition", {})
            trigger = find_manual_or_recurrence_trigger(definition)
            
            if trigger:
                logger.info(f"[VERIFY] Triggering test run with '{trigger}'...")
                resp = post_trigger_run(
                    token,
                    subscription_id,
                    resource_group,
                    workflow_name,
                    trigger,
                    body={},
                )
                if resp.status_code in (200, 202):
                    logger.info("[VERIFY] Test run triggered")
        except Exception as e:
            logger.warning(f"[VERIFY] Could not trigger test run: {e}")
        
        return {
            "status": "remediated",
            "workflow_name": workflow_name,
            "run_id": run_id,
            "root_cause": rca_result.get("root_cause"),
            "fix_strategy": fix_result.get("fix_strategy"),
            "changes_applied": fix_result.get("changes_applied"),
        }
        
    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Unexpected error: {e}", exc_info=True)
        return {
            "status": "failed",
            "workflow_name": workflow_name,
            "run_id": run_id,
            "error": str(e),
        }


def _fallback_rca(failed_action: Dict[str, Any], workflow_name: str) -> Dict[str, Any]:
    """Fallback RCA when LLM not available."""
    from agent.observer.error_detector import extract_error_message, extract_error_code
    
    message = extract_error_message(failed_action)
    code = extract_error_code(failed_action)
    
    return {
        "error_location": failed_action.get("name", "unknown"),
        "action_type": failed_action.get("type", "unknown"),
        "error_code": code or "unknown",
        "root_cause": "unknown",
        "exact_issue": message or "Unknown error",
        "recommendation": "Manual investigation required",
        "solution": "Inspect action inputs/outputs and connector diagnostics",
        "confidence": 0.0,
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 5:
        print("Usage: python Orchestrator_agent.py <workflow> <run_id> <subscription> <resource_group> [backup_dir]")
        sys.exit(1)
    
    logging.basicConfig(level=logging.INFO)
    
    backup = sys.argv[5] if len(sys.argv) > 5 else None
    result = run_remediation(
        workflow_name=sys.argv[1],
        run_id=sys.argv[2],
        subscription_id=sys.argv[3],
        resource_group=sys.argv[4],
        backup_dir=backup,
    )
    print(json.dumps(result, indent=2))