"""
Orchestrator agent: coordinates the entire remediation process.
"""
import asyncio
import logging
from typing import Any, Dict, Optional

from services.agents.observer import Observer
from services.agents.classifier.analyzer import classify_error
from services.remediation_tracker import get_tracker
from config import Settings, get_settings

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
    Orchestrator agent that observes failed workflow runs, classifies errors,
    performs root cause analysis (RCA), and optionally applies remediation fixes.

    Attributes:
        settings (Settings): Application settings.
        observer (Observer): Observer agent instance.
        tracker: Tracker to prevent duplicate remediation.
        fixer (FixerAgent | None): Fixer agent instance if available.
    """
    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the Orchestrator.

        Args:
            settings (Optional[Settings]): Application settings. If None, default settings are loaded.
        """
        self.settings = settings or get_settings()
        self.observer = Observer(self.settings)
        self.tracker = get_tracker()
        if FIXER_AVAILABLE:
            self.fixer = FixerAgent(self.settings)
        else:
            self.fixer = None

    async def _verify_fix(self, workflow_name: str, subscription_id: str, resource_group: str) -> Dict[str, Any]:
        """
        Trigger a workflow manually or via recurrence to verify that remediation succeeded.

        Args:
            workflow_name (str): Name of the Logic App workflow.
            subscription_id (str): Azure subscription ID.
            resource_group (str): Resource group containing the workflow.

        Returns:
            Dict[str, Any]: Verification result with keys:
                - 'verified' (bool)
                - 'reason' (str, if not verified)
                - 'trigger' (str, if verified)
                - 'status_code' (int, HTTP status code of trigger run)
        """
        from services.auth import get_arm_token
        from services.workflow_service import get_workflow, find_manual_or_recurrence_trigger, post_trigger_run

        token = get_arm_token(
            self.settings.AZURE_TENANT_ID,
            self.settings.AZURE_CLIENT_ID,
            self.settings.AZURE_CLIENT_SECRET,
        )
        try:
            workflow = await asyncio.to_thread(get_workflow, token, subscription_id, resource_group, workflow_name)
            definition = workflow.get("properties", {}).get("definition", {})
            trigger = find_manual_or_recurrence_trigger(definition)
            if not trigger:
                return {"verified": False, "reason": "no_trigger_found"}
            resp = await asyncio.to_thread(post_trigger_run, token, subscription_id, resource_group, workflow_name, trigger, body={})
            if resp.status_code in (200, 202):
                return {"verified": True, "trigger": trigger, "status_code": resp.status_code}
            return {"verified": False, "reason": f"trigger_failed_http_{resp.status_code}"}
        except Exception as e:
            logger.error("Verification trigger failed: %s", e)
            return {"verified": False, "reason": str(e)}

    def _fallback_rca(self, error_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a rule-based fallback RCA in case LLM-based RCA is unavailable.

        Args:
            error_context (Dict[str, Any]): Structured error information from failed action.

        Returns:
            Dict[str, Any]: RCA result with keys:
                - 'root_cause' (str)
                - 'exact_issue' (str)
                - 'suggested_fix' (str)
                - 'confidence' (float)
                - 'solution' (str)
        """
        from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score
        from services.agents.rca.engine import _generate_suggested_fix_from_root_cause
        
        error_msg = error_context.get("error_message", "")
        error_code = error_context.get("error_code", "")
        error_type = "unknown"
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_context)
        confidence = confidence_score(root_cause, error_code, error_msg)
        suggested_fix = _generate_suggested_fix_from_root_cause(root_cause, error_type, error_context)
        
        return {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "suggested_fix": suggested_fix,
            "confidence": confidence,
            "solution": f"Manual investigation required (LLM unavailable). Suggested fix: {suggested_fix}",
        }

    def _classify_error_rule_based(
        self,
        error_message: str,
        error_code: str,
        status_code: Optional[int],
    ) -> Optional[str]:
        """
        Perform a simple rule-based error classification.

        Args:
            error_message (str): Error message string from failed action.
            error_code (str): Error code string from failed action.
            status_code (Optional[int]): HTTP status code if available.

        Returns:
            Optional[str]: Classified error type ('404', '401', 'timeout', 'bad_request', etc.), or None if no match.
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

    async def remediate(
        self, 
        workflow_name: str, 
        run_id: str, 
        subscription_id: str, 
        resource_group: str, 
        backup_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main remediation entry point for a failed workflow run.

        Args:
            workflow_name (str): Name of the Logic App workflow.
            run_id (str): Failed run ID to remediate.
            subscription_id (str): Azure subscription ID.
            resource_group (str): Resource group containing the workflow.
            backup_dir (Optional[str]): Directory to backup original workflow.

        Returns:
            Dict[str, Any]: Remediation result with status and details.
        """
        logger.info("=" * 80)
        logger.info("🚀 Orchestrator: Starting remediation for %s/%s", workflow_name, run_id)
        logger.info("=" * 80)

        # Step 1: Observer - analyze failed run
        obs_result = self.observer.analyze_failed_run(subscription_id, resource_group, workflow_name, run_id)
        if obs_result["status"] != "failed_action_found":
            logger.warning("Observer: %s", obs_result["status"])
            return {
                "status": obs_result["status"], 
                "workflow_name": workflow_name, 
                "run_id": run_id, 
                "message": obs_result.get("run_status", "No failed action")
            }

        error_ctx = obs_result["error_context"]
        failed_action_name = obs_result["failed_action_name"]

        # Step 2: Check if already remediated
        if self.tracker.is_run_already_remediated(run_id):
            logger.info("Run %s already remediated, skipping", run_id)
            return {"status": "skipped", "reason": "already_remediated"}

        # Step 3: Classifier (rule-based first, then LLM)
        error_type = self._classify_error_rule_based(
            error_ctx["error_message"], 
            error_ctx["error_code"], 
            error_ctx.get("status_code")
        )
        if error_type is None:
            logger.info("Rule‐based classification didn't match, trying LLM")
            error_type = await classify_error(
                error_ctx["error_message"], 
                error_ctx["error_code"], 
                error_ctx.get("status_code"), 
                self.settings
            )
        logger.info("Classifier final error_type = %s", error_type)

        if error_type == "unknown":
            logger.error("Cannot classify error – manual review required")
            return {
                "status": "needs_manual_review", 
                "workflow_name": workflow_name, 
                "run_id": run_id, 
                "error_type": error_type, 
                "error_context": error_ctx
            }

        # Step 4: RCA - Root Cause Analysis
        logger.info("=" * 80)
        logger.info("Starting RCA phase for error_type=%s", error_type)
        logger.info("=" * 80)
        
        if RCA_AVAILABLE and generate_rca:
            try:
                # RCA includes: LLM analysis + knowledge base search + enhancement
                rca_result = await asyncio.wait_for(
                    generate_rca(obs_result["failed_action"], error_ctx, error_type, self.settings),
                    timeout=150.0  # 2.5 minutes total for RCA
                )
                logger.info("RCA result: root_cause=%s, suggested_fix=%s, confidence=%s", 
                           rca_result.get("root_cause", "unknown")[:100],
                           rca_result.get("suggested_fix", "none")[:100],
                           rca_result.get("confidence", 0))
            except asyncio.TimeoutError:
                logger.error("RCA timed out after 150 seconds – using fallback")
                rca_result = self._fallback_rca(error_ctx)
            except Exception as e:
                logger.error("RCA failed: %s – using fallback", e)
                rca_result = self._fallback_rca(error_ctx)
        else:
            logger.warning("RCA module not available – using fallback")
            rca_result = self._fallback_rca(error_ctx)

        # Step 5: Fixer - Apply remediation
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
            "backup_dir": backup_dir,
            "suggested_fix": rca_result.get("suggested_fix"),  # Pass suggested fix to fixer
            "error_type": error_type,
        }
        
        try:
            # Run fixer in thread pool to avoid blocking
            fix_result = await asyncio.to_thread(self.fixer.fix, rca_result, workflow_context)
        except Exception as e:
            logger.error("Fixer failed: %s", e)
            return {
                "status": "failed", 
                "workflow_name": workflow_name, 
                "run_id": run_id, 
                "error_type": error_type, 
                "error": str(e)
            }

        # Step 6: Return result
        if fix_result.get("success"):
            self.tracker.mark_run_remediated(
                run_id=run_id,
                workflow_name=workflow_name,
                error_type=error_type,
                workflow_definition=fix_result.get("workflow_definition"),  # if available
                fix_strategy=fix_result.get("fix_strategy"),  # NEW: Pass the fix strategy
                root_cause=rca_result.get("root_cause")       # NEW: Pass the root cause
            )
            logger.info("✅ Fix deployed successfully for %s/%s", workflow_name, run_id)
            
            # Optional verification
            verification = {"verified": False, "reason": "disabled"}
            if self.settings.VERIFY_FIX_WITH_TEST_RUN:
                verification = await self._verify_fix(workflow_name, subscription_id, resource_group)
            
            return {
                "status": "remediated",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "root_cause": rca_result.get("root_cause"),
                "suggested_fix": rca_result.get("suggested_fix"),
                "fix_strategy": fix_result.get("fix_strategy"),
                "changes_applied": fix_result.get("changes_applied"),
                "verification": verification,
            }
        else:
            logger.error("Remediation failed: %s", fix_result.get("error"))
            return {
                "status": "failed", 
                "workflow_name": workflow_name, 
                "run_id": run_id, 
                "error_type": error_type, 
                "suggested_fix": rca_result.get("suggested_fix"),
                "error": fix_result.get("error")
            }