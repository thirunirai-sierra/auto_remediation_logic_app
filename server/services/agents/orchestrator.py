# server/services/agents/orchestrator.py
"""
Orchestrator: Main pipeline coordinating Observer → Classifier → RCA → Fixer → Verification.
✅ Includes HANA database persistence for RCA and remediation results.
"""
import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from services.agents.observer import Observer
from services.agents.classifier.analyzer import classify_error
from services.agents.rca.engine import generate_rca
from services.agents.fixer.Fixer_agent import FixerAgent
from services.remediation_tracker import RemediationTracker
from db.hana_client import get_global_client
from config import Settings

logger = logging.getLogger(__name__)

# Import guards
try:
    from services.agents.rca.engine import generate_rca
    RCA_AVAILABLE = True
except ImportError:
    RCA_AVAILABLE = False
    generate_rca = None

try:
    from services.agents.fixer.Fixer_agent import FixerAgent
    FIXER_AVAILABLE = True
except ImportError:
    FIXER_AVAILABLE = False


class Orchestrator:
    """Pipeline orchestrator for automated Logic App remediation."""

    def __init__(self, settings: Settings):
        """Initialize orchestrator with settings."""
        self.settings = settings
        self.observer = Observer(settings)
        self.tracker = RemediationTracker()
        self.fixer = None
        self.hana = get_global_client()  # ✅ Get HANA client
        
        if FIXER_AVAILABLE:
            self.fixer = FixerAgent(settings)

    def _classify_error_rule_based(self, error_message: str, error_code: str, status_code: int = 0) -> Optional[str]:
        """Rule-based error classification (fast path)."""
        msg_lower = error_message.lower()
        code_lower = error_code.lower()

        if "404" in str(status_code) or "not found" in msg_lower or "notfound" in code_lower:
            return "404"
        if "401" in str(status_code) or "unauthorized" in msg_lower or "unauthenticated" in code_lower:
            return "401"
        if "403" in str(status_code) or "forbidden" in msg_lower:
            return "401"
        if "timeout" in msg_lower or "timed out" in msg_lower or status_code in (408, 504) or "timedout" in code_lower:
            return "timeout"
        if ("400" in str(status_code) or "bad request" in msg_lower or "invalid" in msg_lower 
            or "contains" in msg_lower or "null" in msg_lower):
            return "bad_request"
        if "429" in str(status_code) or "throttl" in msg_lower or "ratelimit" in code_lower:
            return "timeout"
        if "no dependent actions succeeded" in msg_lower:
            return "bad_request"
        if "action failed" in msg_lower and len(msg_lower) < 100:
            return "bad_request"

        return None

    def _fallback_rca(self, error_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Generate fallback RCA using rule-based error detection."""
        from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score

        error_msg = error_ctx.get("error_message", "")
        error_code = error_ctx.get("error_code", "")
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_ctx)
        confidence = confidence_score(root_cause, error_code, error_msg)

        return {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "solution": f"Rule-based analysis: {exact_issue}",
            "confidence": confidence,
            "workflow_name": error_ctx.get("workflow_name", "unknown"),
            "error_message_s": error_msg,
            "code_s": error_code,
            "status_s": error_ctx.get("status", "unknown"),
            "Level": error_ctx.get("level", "Error"),
        }

    async def remediate(
        self,
        workflow_name: str,
        run_id: str,
        subscription_id: str,
        resource_group: str,
        backup_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Main orchestration pipeline:
        Observer → Classifier → RCA → Fixer → Verification → **Database**
        """
        # ✅ Generate unique incident ID for tracking
        incident_id = f"{workflow_name}-{run_id}"
        
        logger.info("=" * 80)
        logger.info("Orchestrator: Starting remediation for %s/%s", workflow_name, run_id)
        logger.info("  Incident ID: %s", incident_id)
        logger.info("=" * 80)

        # OBSERVER
        obs_result = self.observer.analyze_failed_run(subscription_id, resource_group, workflow_name, run_id)
        if obs_result["status"] != "failed_action_found":
            logger.warning("Observer: %s", obs_result["status"])
            # ✅ Store to database with status
            if self.hana:
                self.hana.batch_upsert_observability([{
                    "incident_id": incident_id,
                    "subscription_id": subscription_id,
                    "workflow_name": workflow_name,
                    "error_code": "OBSERVER_FAILED",
                    "error_message": obs_result.get("run_status", "Unknown"),
                    "error_category": "OBSERVER_ERROR",
                    "status": "Observer Failed",
                    "created_at": datetime.now().isoformat(),
                }])
            return {"status": obs_result["status"], "workflow_name": workflow_name, "run_id": run_id}

        error_ctx = obs_result["error_context"]
        failed_action_name = obs_result["failed_action_name"]

        if self.tracker.is_run_already_remediated(run_id):
            logger.info("Run %s already remediated, skipping", run_id)
            return {"status": "skipped", "reason": "already_remediated"}

        # CLASSIFIER
        error_type = self._classify_error_rule_based(
            error_ctx["error_message"], 
            error_ctx["error_code"], 
            error_ctx.get("status_code", 0)
        )
        if error_type is None:
            logger.info("Rule-based classification didn't match, trying LLM")
            error_type = await classify_error(
                error_ctx["error_message"], 
                error_ctx["error_code"], 
                error_ctx.get("status_code"), 
                self.settings
            )
        
        if error_type == "unknown":
            error_type = "bad_request"  # ✅ Default to bad_request instead of unknown
            logger.warning("Classifier returned unknown, defaulting to bad_request")
        
        logger.info("Classifier final error_type = %s", error_type)

        # RCA
        logger.info("=" * 80)
        logger.info("Starting RCA phase for error_type=%s", error_type)
        logger.info("=" * 80)
        
        rca_result = None
        if RCA_AVAILABLE and generate_rca:
            try:
                # ✅ 150-second timeout (increased from 90)
                rca_result = await asyncio.wait_for(
                    generate_rca(obs_result["failed_action"], error_ctx, error_type, self.settings),
                    timeout=150.0
                )
                logger.info("✅ RCA succeeded: root_cause=%s, confidence=%.2f", 
                           rca_result.get("root_cause"), rca_result.get("confidence", 0))
            except asyncio.TimeoutError:
                logger.error("RCA timed out after 150 seconds – using fallback")
                rca_result = self._fallback_rca(error_ctx)
            except Exception as e:
                logger.error("RCA failed: %s – using fallback", str(e)[:150])
                rca_result = self._fallback_rca(error_ctx)
        else:
            rca_result = self._fallback_rca(error_ctx)

        # ✅ STORE RCA TO DATABASE
        if self.hana and rca_result:
            logger.info("Storing RCA result to HANA database...")
            self.hana.batch_upsert_observability([{
                "incident_id": incident_id,
                "subscription_id": subscription_id,
                "workflow_name": workflow_name,
                "error_code": error_ctx.get("error_code", "unknown"),
                "error_message": error_ctx.get("error_message", ""),
                "error_category": error_type,
                "status": "RCA Complete",
                "rca_root_cause": rca_result.get("root_cause", "unknown"),
                "ai_diagnosis": rca_result.get("exact_issue", ""),
                "ai_proposed_fix": rca_result.get("solution", ""),
                "ai_confidence": float(rca_result.get("confidence", 0.0)),
                "created_at": datetime.now().isoformat(),
            }])
            
            # ✅ Also update RCA columns (upsert)
            self.hana.update_rca_record(
                incident_id=incident_id,
                root_cause=rca_result.get("root_cause", "unknown"),
                proposed_fix=rca_result.get("solution", ""),
                confidence=float(rca_result.get("confidence", 0.0)),
                affected_component=failed_action_name
            )
            logger.info("✅ RCA stored to HANA")

        # FIXER
        if not FIXER_AVAILABLE or not self.fixer:
            logger.error("Fixer module unavailable")
            # ✅ Store failure to database
            if self.hana:
                self.hana.batch_upsert_observability([{
                    "incident_id": incident_id,
                    "subscription_id": subscription_id,
                    "workflow_name": workflow_name,
                    "error_code": error_ctx.get("error_code", "unknown"),
                    "error_message": "Fixer not available",
                    "error_category": error_type,
                    "status": "Fixer Failed",
                    "created_at": datetime.now().isoformat(),
                }])
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
        }

        try:
            fix_result = await asyncio.to_thread(self.fixer.fix, rca_result, workflow_context)
        except Exception as e:
            logger.error("Fixer failed: %s", str(e)[:200])
            # ✅ Store fixer failure to database
            if self.hana:
                self.hana.batch_upsert_observability([{
                    "incident_id": incident_id,
                    "subscription_id": subscription_id,
                    "workflow_name": workflow_name,
                    "error_code": error_ctx.get("error_code", "unknown"),
                    "error_message": str(e)[:500],
                    "error_category": error_type,
                    "status": "Fixer Error",
                    "created_at": datetime.now().isoformat(),
                }])
            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "error": str(e)[:200]
            }

        # REMEDIATION RESULT
        if fix_result.get("success"):
            self.tracker.mark_run_remediated(run_id, workflow_name, rca_result.get("root_cause", "unknown"))
            logger.info("✅ Fix deployed successfully for %s/%s", workflow_name, run_id)
            
            # ✅ Store SUCCESS to database
            if self.hana:
                self.hana.batch_upsert_observability([{
                    "incident_id": incident_id,
                    "subscription_id": subscription_id,
                    "workflow_name": workflow_name,
                    "error_code": error_ctx.get("error_code", "unknown"),
                    "error_message": error_ctx.get("error_message", ""),
                    "error_category": error_type,
                    "status": "Remediated",
                    "rca_root_cause": rca_result.get("root_cause", "unknown"),
                    "fix_strategy": fix_result.get("fix_strategy", "unknown"),
                    "ai_confidence": float(rca_result.get("confidence", 0.0)),
                    "auto_fix_attempted": True,
                    "auto_fix_success": True,
                    "created_at": datetime.now().isoformat(),
                }])
                
                # ✅ Store fix details
                self.hana.update_fix_result(
                    incident_id=incident_id,
                    fix_summary=fix_result.get("fix_strategy", ""),
                    fix_steps=str(fix_result.get("changes_applied", [])),
                    verification_status="Success",
                    resolved_at=datetime.now().isoformat()
                )

            return {
                "status": "remediated",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "incident_id": incident_id,
                "error_type": error_type,
                "root_cause": rca_result.get("root_cause"),
                "fix_strategy": fix_result.get("fix_strategy"),
                "changes_applied": fix_result.get("changes_applied"),
            }
        else:
            logger.error("Remediation failed: %s", fix_result.get("error"))
            
            # ✅ Store FAILURE to database
            if self.hana:
                self.hana.batch_upsert_observability([{
                    "incident_id": incident_id,
                    "subscription_id": subscription_id,
                    "workflow_name": workflow_name,
                    "error_code": error_ctx.get("error_code", "unknown"),
                    "error_message": error_ctx.get("error_message", ""),
                    "error_category": error_type,
                    "status": "Fix Failed",
                    "rca_root_cause": rca_result.get("root_cause", "unknown"),
                    "fix_strategy": fix_result.get("fix_strategy", "unknown"),
                    "ai_confidence": float(rca_result.get("confidence", 0.0)),
                    "auto_fix_attempted": True,
                    "auto_fix_success": False,
                    "created_at": datetime.now().isoformat(),
                }])
                
                # ✅ Store failure reason
                self.hana.update_fix_result(
                    incident_id=incident_id,
                    fix_summary=fix_result.get("error", "Unknown fix error"),
                    fix_steps="",
                    verification_status="Failed",
                    resolved_at=None
                )

            return {
                "status": "failed",
                "workflow_name": workflow_name,
                "run_id": run_id,
                "incident_id": incident_id,
                "error_type": error_type,
                "error": fix_result.get("error")
            }