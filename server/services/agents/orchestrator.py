# server/services/agents/orchestrator.py
"""
Orchestrator agent: coordinates the entire remediation process.
"""

import asyncio,time,json,logging,httpx
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from services.agents.fixer.Fixer_agent import FixerAgent
from db.hana_client import get_global_client
from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score
from services.agents.observer import Observer
from services.agents.classifier.analyzer import classify_error
from services.remediation_tracker import get_tracker
from services.agents.rca.engine import generate_rca
from services.auth import get_arm_token
from services.workflow_service import list_runs, get_run, list_run_actions,should_skip_remediate_newer_succeeded 
from config import Settings, get_settings
from monitoring.llm_monitor import log_agent_invoke
try:
    from services.agents.rca.engine import generate_rca
    RCA_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error("RCA module not available: %s", e)
    RCA_AVAILABLE = False
    generate_rca = None

try:
    from services.agents.fixer.Fixer_agent import FixerAgent
    FIXER_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error("Fixer module not available: %s", e)
    FIXER_AVAILABLE = False
    FixerAgent = None

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Orchestrator coordinates the full remediation pipeline:
        1. Observer – identify failed action and error context
        2. Classifier – determine error type (rule‑based + LLM)
        3. RCA – analyse root cause and suggest fix
        4. Fixer – apply dynamic patch and deploy
        5. Verification – optionally trigger a test run (if supported)
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialise the orchestrator.

        Args:
            settings: Application settings. If None, loads from environment.
        """
        self.settings = settings or get_settings()
        self.observer = Observer(self.settings)
        self.tracker = get_tracker()
        if FIXER_AVAILABLE:
            self.fixer = FixerAgent(self.settings)
        else:
            self.fixer = None

    # Private helpers
    def _append_history_entry(self, incident_id: str, step: str, description: str, status: str) -> None:
        """
        Append a step to the incident's history in HANA.

        Args:
            incident_id: The run ID / incident identifier.
            step: Short name of the step (e.g., "RCA Analysis").
            description: Human‑readable description.
            status: "completed", "failed", etc.
        """
        client = get_global_client()
        if not client or not client._ensure_connected():
            logger.warning("Cannot append history: HANA client not available")
            return

        cur = client.conn.cursor()
        try:
            cur.execute(f"SELECT HISTORY_ENTRIES FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
            row = cur.fetchone()
            entries = json.loads(row[0]) if row and row[0] else []
            entries.append({
                "step": step,
                "description": description,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            cur.execute(
                f"UPDATE {client.full_table} SET HISTORY_ENTRIES = ? WHERE INCIDENT_ID = ?",
                (json.dumps(entries), incident_id),
            )
            client.conn.commit()
            logger.info("History entry added for %s: %s", incident_id, step)
        except Exception as e:
            logger.warning("Failed to append history for %s: %s", incident_id, e)
        finally:
            cur.close()

    def _fallback_rca(self, error_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rule‑based fallback when the LLM‑powered RCA module is unavailable or fails.

        Args:
            error_context: Dictionary containing error_message, error_code, etc.

        Returns:
            RCA result with keys: root_cause, exact_issue, confidence,
            solution, suggested_fix.
        """

        error_msg = error_context.get("error_message", "")
        error_code = error_context.get("error_code", "")
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_context)
        confidence = confidence_score(root_cause, error_code, error_msg)

        if root_cause == "not_found":
            suggested_fix = "Update the endpoint URL to a valid address."
        elif root_cause == "auth_or_authorization_error":
            suggested_fix = "Refresh authentication or check permissions."
        elif root_cause == "timeout":
            suggested_fix = "Increase timeout and add a retry policy."
        elif root_cause in ("payload_or_schema_error", "null_reference_error"):
            suggested_fix = "Add null/type checks and defaults to the expression."
        elif root_cause == "throttling":
            suggested_fix = "Implement exponential backoff retry."
        elif root_cause == "dns_resolution_error":
            suggested_fix = "Fix the hostname or DNS configuration."
        elif root_cause == "connection_refused":
            suggested_fix = "Check firewall and port accessibility."
        else:
            suggested_fix = "Review the action inputs and outputs manually."

        return {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "confidence": confidence,
            "solution": f"Rule-based analysis: {exact_issue}",
            "suggested_fix": suggested_fix,
        }

    def _classify_error_rule_based(
        self,
        error_message: str,
        error_code: str,
        status_code: Optional[int],
    ) -> Optional[str]:
        """
        Quick rule‑based error classification used before LLM classification.

        Args:
            error_message: Error message text.
            error_code: Error code from Azure.
            status_code: HTTP status code (if available).

        Returns:
            One of "404", "401", "timeout", "bad_request", or None if no rule matches.
        """
        msg_lower = (error_message or "").lower()
        code_lower = (error_code or "").lower()

        if "404" in code_lower or "not found" in msg_lower or status_code == 404:
            return "404"
        if "401" in code_lower or "unauthorized" in msg_lower or status_code == 401:
            return "401"
        if "403" in code_lower or "forbidden" in msg_lower or status_code == 403:
            return "401"
        if "timeout" in msg_lower or "timed out" in msg_lower or status_code in (408, 504):
            return "timeout"
        if "400" in code_lower or "bad request" in msg_lower or "invalid" in msg_lower:
            return "bad_request"
        if "429" in code_lower or "throttl" in msg_lower or status_code == 429:
            return "timeout"
        if "null" in msg_lower or "contains" in msg_lower:
            return "bad_request"
        if "no dependent actions succeeded" in msg_lower:
            return "bad_request"
        if "action failed" in msg_lower and len(msg_lower) < 100:
            return "bad_request"
        return None

    def _log_agent_step(self, agent_name: str, step: str, input_data: Dict, output_data: Dict) -> None:
        """Log the input and output of a processing step (for debugging)."""
        logger.info("=" * 60)
        logger.info("%s - %s", agent_name, step)
        logger.info("INPUT: %s", json.dumps(input_data, default=str, indent=2)[:500])
        logger.info("OUTPUT: %s", json.dumps(output_data, default=str, indent=2)[:500])
        logger.info("=" * 60)

    async def _resubmit_run(
        self, workflow_name: str, subscription_id: str, resource_group: str, run_id: str
    ) -> Dict[str, Any]:
        """
        Attempt to resubmit a failed run using the Azure resubmit API.

        Args:
            workflow_name: Name of the Logic App.
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group.
            run_id: ID of the run to resubmit.

        Returns:
            Dictionary with keys: success (bool), status_code (if failed), body (if failed).
        """
        token = get_arm_token(
            self.settings.AZURE_TENANT_ID,
            self.settings.AZURE_CLIENT_ID,
            self.settings.AZURE_CLIENT_SECRET,
        )

        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Logic/workflows/{workflow_name}"
            f"/runs/{run_id}/resubmit?api-version=2019-05-01"
        )
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=headers)
            if resp.status_code == 202:
                return {"success": True, "message": "Resubmitted successfully"}
            return {"success": False, "status_code": resp.status_code, "body": resp.text}

    async def _wait_for_resubmitted_run(
        self,
        workflow_name: str,
        subscription_id: str,
        resource_group: str,
        original_run_id: str,
        timeout_seconds: int = 120,
    ) -> Dict[str, Any]:
        """
        Wait for a newly triggered run (after resubmit) and return its status.

        Args:
            workflow_name: Name of the Logic App.
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group.
            original_run_id: The original failed run ID (to exclude it).
            timeout_seconds: Maximum time to wait for completion.

        Returns:
            Dictionary with keys: verified (bool), method="resubmit", run_id,
            run_status, actions_executed, reason.
        """
        token = get_arm_token(
            self.settings.AZURE_TENANT_ID,
            self.settings.AZURE_CLIENT_ID,
            self.settings.AZURE_CLIENT_SECRET,
        )

        before_runs = await asyncio.to_thread(
            list_runs, token, subscription_id, resource_group, workflow_name, 20
        )
        before_ids = {r.get("name") for r in before_runs if r.get("name")}
        before_ids.add(original_run_id)

        started = time.time()
        new_run_id = None

        while time.time() - started < timeout_seconds:
            runs = await asyncio.to_thread(
                list_runs, token, subscription_id, resource_group, workflow_name, 20
            )
            for run in runs:
                rid = run.get("name")
                if rid and rid not in before_ids:
                    new_run_id = rid
                    break
            if new_run_id:
                break
            await asyncio.sleep(3)

        if not new_run_id:
            return {"verified": False, "reason": "resubmitted_run_not_found"}

        final_status = None
        while time.time() - started < timeout_seconds:
            run_data = await asyncio.to_thread(
                get_run, token, subscription_id, resource_group, workflow_name, new_run_id
            )
            final_status = run_data.get("properties", {}).get("status")
            if final_status in ("Succeeded", "Failed", "Cancelled", "TimedOut", "Aborted"):
                break
            await asyncio.sleep(3)

        actions = await asyncio.to_thread(
            list_run_actions, token, subscription_id, resource_group, workflow_name, new_run_id
        )
        action_names = [a.get("name") for a in actions if a.get("name")]

        return {
            "verified": final_status == "Succeeded",
            "method": "resubmit",
            "run_id": new_run_id,
            "run_status": final_status,
            "actions_executed": action_names,
            "reason": "workflow_succeeded" if final_status == "Succeeded" else f"workflow_{str(final_status).lower()}",
        }

    # Public API
    async def remediate(
        self,
        workflow_name: str,
        run_id: str,
        subscription_id: str,
        resource_group: str,
        backup_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run the full remediation pipeline for a failed workflow run.

        Steps:
            1. Check if a newer run already succeeded → skip.
            2. Observe the failed run → get error context.
            3. Classify error type (rule‑based, fallback to LLM).
            4. Perform root cause analysis (RCA) with LLM + knowledge base.
            5. Generate and apply a fix using the Fixer agent.
            6. Optionally resubmit the run to verify the fix.

        Args:
            workflow_name: Name of the Logic App workflow.
            run_id: ID of the failed run.
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group name.
            backup_dir: Optional directory for backups (unused).

        Returns:
            Remediation result dictionary with keys:
                - status: "remediated", "skipped", "failed", etc.
                - workflow_name, run_id, error_type
                - root_cause, suggested_fix, fix_strategy, changes_applied
                - verification (optional)
        """
        logger.info("=" * 80)
        logger.info("Orchestrator: Starting remediation for %s/%s", workflow_name, run_id)
        logger.info("=" * 80)

        if getattr(self.settings, "EVENT_MESH_PIPELINE_ENABLED", True):
            from services.event_mesh.pipeline import start_pipeline
            return await start_pipeline(
                workflow_name, run_id, subscription_id, resource_group, source="orchestrator"
            )

        # 1. Skip if a newer run succeeded
        if getattr(self.settings, "SKIP_IF_NEWER_RUN_SUCCEEDED", True):
            try:
                token = get_arm_token(
                    self.settings.AZURE_TENANT_ID,
                    self.settings.AZURE_CLIENT_ID,
                    self.settings.AZURE_CLIENT_SECRET,
                )
                top = int(getattr(self.settings, "REMEDIATION_RUNS_LIST_TOP", 50))
                skip_newer, _reason = should_skip_remediate_newer_succeeded(
                    token,
                    subscription_id,
                    resource_group,
                    workflow_name,
                    run_id,
                    list_top=top,
                )
                if skip_newer:
                    logger.info("[REMEDIATION] skip: newer workflow run succeeded (ARM)")
                    return {"status": "skipped", "reason": "newer_run_succeeded"}
            except Exception as e:
                logger.warning("Failed to check newer succeeded run: %s", e)

        # 2. Observe the failed run
        obs_result = self.observer.analyze_failed_run(subscription_id, resource_group, workflow_name, run_id)
        if obs_result["status"] != "failed_action_found":
            log_agent_invoke(obs_result)
            logger.warning("Observer: %s", obs_result["status"])
            return {
                "status": obs_result["status"],
                "workflow_name": workflow_name,
                "run_id": run_id,
                "message": obs_result.get("run_status", "No failed action"),
            }

        error_ctx = obs_result["error_context"]
        failed_action_name = obs_result["failed_action_name"]
        failed_action_path = obs_result.get("failed_action_path", failed_action_name)

        self._log_agent_step(
            "Observer",
            "Analyze Failed Run",
            {"workflow": workflow_name, "run_id": run_id},
            obs_result,
        )

        if self.tracker.is_run_already_remediated(run_id):
            logger.info("Run %s already remediated, skipping", run_id)
            return {"status": "skipped", "reason": "already_remediated"}

        # 3. Classify error type
        error_type = self._classify_error_rule_based(
            error_ctx["error_message"],
            error_ctx["error_code"],
            error_ctx.get("status_code"),
        )
        if error_type is None:
            logger.info("Rule-based classification didn't match, trying LLM")
            error_type = await classify_error(
                error_ctx["error_message"],
                error_ctx["error_code"],
                error_ctx.get("status_code"),
                self.settings,
            )
            log_agent_invoke({"error_type": error_type, "status": "classified"})

        logger.info("Classifier final error_type = %s", error_type)

        if error_type == "unknown":
            logger.error("Cannot classify error – manual review required")
            return {
                "status": "needs_manual_review",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "error_context": error_ctx,
            }

        # 4. RCA (LLM + knowledge base or fallback)
        logger.info("=" * 80)
        logger.info("Starting RCA phase for error_type=%s", error_type)
        logger.info("=" * 80)

        if RCA_AVAILABLE and generate_rca:
            try:
                rca_result = await asyncio.wait_for(
                    generate_rca(obs_result["failed_action"], error_ctx, error_type, self.settings),
                    timeout=180.0,
                )
                log_agent_invoke(rca_result)
                logger.info(
                    "RCA result: root_cause=%s, suggested_fix=%s, confidence=%s",
                    rca_result.get("root_cause", "unknown")[:100],
                    rca_result.get("suggested_fix", "none")[:100],
                    rca_result.get("confidence", 0),
                )
            except asyncio.TimeoutError:
                logger.error("RCA timed out after 180 seconds – using fallback")
                rca_result = self._fallback_rca(error_ctx)
            except Exception as e:
                logger.error("RCA failed: %s – using fallback", e)
                rca_result = self._fallback_rca(error_ctx)
        else:
            logger.warning("RCA module not available – using fallback")
            rca_result = self._fallback_rca(error_ctx)

        self._append_history_entry(
            run_id,
            "RCA Analysis",
            f"Root cause identified: {rca_result.get('root_cause', 'unknown')}",
            "completed",
        )

        self._log_agent_step(
            "RCA",
            "Root Cause Analysis",
            {"error_type": error_type, "error_message": error_ctx["error_message"][:200]},
            {
                "root_cause": rca_result.get("root_cause"),
                "suggested_fix": rca_result.get("suggested_fix"),
                "confidence": rca_result.get("confidence"),
            },
        )

        # 5. Fixer
        if not FIXER_AVAILABLE or not self.fixer:
            logger.error("Fixer module unavailable")
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "error": "Fixer not available"
            }

        workflow_context = {
            "workflow_name": workflow_name,
            "run_id": run_id,
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "failed_action_name": failed_action_name,
            "failed_action_path": failed_action_path,
            "backup_dir": backup_dir,
            "suggested_fix": rca_result.get("suggested_fix"),
            "error_type": error_type,
            "error_context": error_ctx
        }

        try:
            fix_result = await asyncio.to_thread(self.fixer.fix, rca_result, workflow_context)
            log_agent_invoke(fix_result) 
        except Exception as e:
            logger.error("Fixer failed: %s", e)
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "error": str(e)
            }

        if not fix_result.get("success"):
            logger.error("Remediation failed: %s", fix_result.get("error"))
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "suggested_fix": rca_result.get("suggested_fix"),
                "error": fix_result.get("error")
            }

        self.tracker.mark_run_remediated(
            run_id=run_id,
            workflow_name=workflow_name,
            error_type=error_type,
            workflow_definition=fix_result.get("workflow_definition"),
            fix_strategy=fix_result.get("fix_strategy", {}).get("strategy_description"),
            root_cause=rca_result.get("root_cause"),
        )
        logger.info("Fix deployed successfully for %s/%s", workflow_name, run_id)

        self._append_history_entry(
            run_id,
            "Auto-Fix Applied",
            f"Fix strategy: {fix_result.get('fix_strategy', {}).get('strategy_description', 'unknown')}",
            "completed"
        )

        return {
            "status": "remediated",
            "workflow_name": workflow_name,
            "run_id": run_id,
            "error_type": error_type,
            "root_cause": rca_result.get("root_cause"),
            "suggested_fix": rca_result.get("suggested_fix"),
            "fix_strategy": fix_result.get("fix_strategy"),
            "changes_applied": fix_result.get("changes_applied")
        }