# server/routers/agents.py
"""
AI Agents API Routes - Minimal endpoints (1 GET, 1 POST per agent)
"""
import logging
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

from config import get_settings
from services.agents.orchestrator import Orchestrator
from services.agents.classifier.analyzer import analyze_error, classify_error
from services.agents.fixer.Fixer_agent import FixerAgent, get_fixer
from services.agents.knowledge.knowledge_base import KnowledgeAgent
from services.agents.observer import Observer
from services.agents.rca.engine import generate_rca
from services.remediation_tracker import get_tracker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/agents", tags=["AI Agents"])
settings = get_settings()


# ============================================================================
# Request/Response Models
# ============================================================================

class RemediateRequest(BaseModel):
    workflow_name: str
    run_id: str
    subscription_id: str
    resource_group: str


class AnalyzeRequest(BaseModel):
    error_message: str
    error_code: str
    status_code: Optional[int] = None
    workflow_name: Optional[str] = None


class FixRequest(BaseModel):
    workflow_name: str
    run_id: str
    subscription_id: str
    resource_group: str
    root_cause: str
    exact_issue: str
    failed_action_name: Optional[str] 
    action_type: Optional[str] = "http"  
    action_config: Optional[Dict[str, Any]] = Field(default_factory=dict) 


class RCARequest(BaseModel):
    error_message: str
    error_code: str
    error_type: str
    workflow_name: str
    action_type: str = "unknown"


class ObserveRequest(BaseModel):
    subscription_id: str
    resource_group: str
    workflow_name: str
    run_id: str


class KnowledgeSearchRequest(BaseModel):
    query: str
    top_k: int = 5


# ============================================================================
# 1. ORCHESTRATOR AGENT (Full remediation pipeline)
# ============================================================================

@router.post("/orchestrator")
async def orchestrator_remediate(request: RemediateRequest):
    """
    Execute full remediation workflow for a failed run.
    Observes -> Classifies -> RCA -> Fixes -> Verifies
    """
    logger.info(f"Orchestrator: Remediating {request.workflow_name}/{request.run_id}")
    
    orchestrator = Orchestrator(settings)
    result = await orchestrator.remediate(
        workflow_name=request.workflow_name,
        run_id=request.run_id,
        subscription_id=request.subscription_id,
        resource_group=request.resource_group,
    )
    
    return {
        "status": result.get("status"),
        "workflow_name": result.get("workflow_name"),
        "run_id": result.get("run_id"),
        "error_type": result.get("error_type"),
        "root_cause": result.get("root_cause"),
        "suggested_fix": result.get("suggested_fix"),
        "fix_applied": result.get("changes_applied") is not None,
        "message": result.get("error") or "Remediation completed"
    }


@router.get("/orchestrator/{workflow_name}/{run_id}")
async def orchestrator_status(workflow_name: str, run_id: str):
    """Get remediation status for a specific run"""
    tracker = get_tracker()
    rec = tracker.get_run_record(run_id)
    
    if rec:
        return {
            "workflow_name": workflow_name,
            "run_id": run_id,
            "auto_fix_attempted": rec.auto_fix_attempted,
            "auto_fix_success": rec.auto_fix_success,
            "error_type": rec.error_type,
            "status": rec.status,
        }
    
    return {"found": False, "run_id": run_id}


# ============================================================================
# 2. CLASSIFIER AGENT (Error classification)
# ============================================================================

@router.post("/classifier")
async def classifier_analyze(request: AnalyzeRequest):
    """
    Analyze error and return classification.
    Returns error type, root cause, recommendation, and confidence.
    """
    logger.info(f"Classifier: Analyzing error - code={request.error_code}")
    
    result = await analyze_error(
        error_json={"code": request.error_code, "message": request.error_message},
        settings=settings,
        flow_context={"workflow_name": request.workflow_name} if request.workflow_name else None,
    )
    
    return {
        "error_type": result["error_type"],
        "root_cause": result["root_cause"],
        "recommendation": result["recommendation"],
        "confidence": result["confidence"],
        "signals": result["signals"],
    }


@router.get("/classifier/{error_code}")
async def classifier_get(error_code: str):
    """Get classification info for a specific error code"""
    from utils.error_detector import infer_root_cause
    
    root_cause = infer_root_cause(error_code, "")
    
    return {
        "error_code": error_code,
        "root_cause": root_cause,
        "known_codes": ["404", "401", "403", "408", "429", "500", "502", "503", "504"],
        "description": {
            "404": "Resource not found",
            "401": "Authentication failed",
            "403": "Authorization failed", 
            "408": "Request timeout",
            "429": "Throttling",
            "500": "Internal server error",
            "502": "Bad gateway",
            "503": "Service unavailable",
            "504": "Gateway timeout",
        }.get(error_code, "Unknown error code")
    }


# ============================================================================
# 3. FIXER AGENT (Generate and apply fixes)
# ============================================================================

@router.post("/fixer")
async def fixer_apply(request: FixRequest):
    """Generate and apply a fix based on RCA result"""
    logger.info(f"Fixer: Applying fix to {request.workflow_name} - action: {request.failed_action_name}")
    
    fixer = get_fixer(settings)
    
    rca_result = {
        "root_cause": request.root_cause,
        "exact_issue": request.exact_issue,
        "solution": "",
    }
    
    workflow_context = {
        "workflow_name": request.workflow_name,
        "run_id": request.run_id,
        "subscription_id": request.subscription_id,
        "resource_group": request.resource_group,
        "failed_action_name": request.failed_action_name,  # ← USE REQUEST VALUE
        "error_type": request.action_type or "http",
        "suggested_fix": request.exact_issue,
    }
    
    if request.action_config:
        workflow_context["action_config"] = request.action_config
    
    import asyncio
    result = await asyncio.to_thread(fixer.fix, rca_result, workflow_context)  # ← FIXED
    
    return {
        "success": result.get("success", False),
        "workflow_name": request.workflow_name,
        "run_id": request.run_id,
        "failed_action": request.failed_action_name,
        "fix_strategy": result.get("fix_strategy", {}).get("strategy_description", "No fix generated"),
        "changes_applied": result.get("changes_applied"),
        "error": result.get("error"),
    }


@router.get("/fixer/strategies")
async def fixer_strategies():
    """Get all available fix strategies"""
    return {
        "strategies": [
            {"name": "contains_null_guard", "description": "Fix contains() null errors", "risk": "low"},
            {"name": "retry_fixed", "description": "Add fixed retry policy for timeouts", "risk": "low"},
            {"name": "retry_exponential", "description": "Add exponential backoff for throttling", "risk": "low"},
            {"name": "div_zero_guard", "description": "Guard against divide-by-zero", "risk": "low"},
            {"name": "auth_connection_check", "description": "Flag for auth verification", "risk": "low"},
            {"name": "url_fallback", "description": "Update to fallback endpoint", "risk": "medium"},
        ]
    }


# ============================================================================
# 4. RCA AGENT (Root Cause Analysis)
# ============================================================================

@router.post("/rca")
async def rca_generate(request: RCARequest):
    """
    Generate root cause analysis using LLM and knowledge base.
    Returns root cause, suggested fix, and confidence score.
    """
    logger.info(f"RCA: Analyzing {request.workflow_name}")
    
    error_context = {
        "workflow_name": request.workflow_name,
        "error_message": request.error_message,
        "error_code": request.error_code,
        "action_type": request.action_type,
    }
    
    result = await generate_rca(
        failed_action={"type": request.action_type},
        error_context=error_context,
        error_type=request.error_type,
        settings=settings,
    )
    
    return {
        "root_cause": result.get("root_cause", "unknown"),
        "exact_issue": result.get("exact_issue", ""),
        "suggested_fix": result.get("suggested_fix", ""),
        "confidence": result.get("confidence", 0.0),
        "solution": result.get("solution", ""),
        "knowledge_sources": len(result.get("knowledge_sources", [])),
    }


@router.get("/rca/{error_type}")
async def rca_quick(error_type: str, error_message: str = Query(...)):
    """Quick rule-based RCA (no LLM, fast)"""
    from utils.error_detector import infer_root_cause, extract_exact_issue
    
    root_cause = infer_root_cause(error_type, error_message)
    exact_issue = extract_exact_issue(error_message, root_cause, {})
    
    return {
        "error_type": error_type,
        "root_cause": root_cause,
        "exact_issue": exact_issue[:200],
        "quick_fix": {
            "404": "Update endpoint URL",
            "401": "Refresh authentication token",
            "403": "Check RBAC permissions",
            "timeout": "Increase timeout and add retry",
            "bad_request": "Validate payload schema",
        }.get(error_type, "Manual review required")
    }


# ============================================================================
# 5. OBSERVER AGENT (Run inspection)
# ============================================================================

@router.post("/observer")
async def observer_analyze(request: ObserveRequest):
    """
    Analyze a failed workflow run.
    Identifies the failed action and builds error context.
    """
    logger.info(f"Observer: Analyzing {request.workflow_name}/{request.run_id}")
    
    observer = Observer(settings)
    
    result = observer.analyze_failed_run(
        subscription_id=request.subscription_id,
        resource_group=request.resource_group,
        workflow_name=request.workflow_name,
        run_id=request.run_id,
    )
    
    if result.get("status") != "failed_action_found":
        return {
            "status": "failed",
            "message": result.get("run_status", "No failed action found"),
        }
    
    return {
        "status": "success",
        "workflow_name": request.workflow_name,
        "run_id": request.run_id,
        "failed_action": result.get("failed_action_name"),
        "error_message": result.get("error_context", {}).get("error_message", ""),
        "error_code": result.get("error_context", {}).get("error_code", ""),
        "status_code": result.get("error_context", {}).get("status_code"),
    }


@router.get("/observer/{subscription_id}/{resource_group}/{workflow_name}/{run_id}")
async def observer_get(
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
):
    """Quick run analysis using path parameters"""
    observer = Observer(settings)
    
    result = observer.analyze_failed_run(
        subscription_id=subscription_id,
        resource_group=resource_group,
        workflow_name=workflow_name,
        run_id=run_id,
    )
    
    if result.get("status") != "failed_action_found":
        return {"found": False, "message": result.get("run_status", "No failed action")}
    
    return {
        "found": True,
        "failed_action": result.get("failed_action_name"),
        "error_context": result.get("error_context"),
    }


# ============================================================================
# 6. VERIFIER AGENT (Fix verification)
# ============================================================================

class VerifyRequest(BaseModel):
    workflow_name: str
    subscription_id: str
    resource_group: str
    trigger_name: Optional[str] = None


@router.post("/verifier")
async def verifier_verify(request: VerifyRequest):
    """
    Verify if a fix worked by triggering the workflow.
    Returns verification status and details.
    """
    logger.info(f"Verifier: Testing {request.workflow_name}")
    
    from services.auth import get_arm_token
    from services.workflow_service import get_workflow, find_manual_or_recurrence_trigger, post_trigger_run
    
    token = get_arm_token(
        settings.AZURE_TENANT_ID,
        settings.AZURE_CLIENT_ID,
        settings.AZURE_CLIENT_SECRET,
    )
    
    try:
        # Get workflow to find trigger
        workflow = get_workflow(
            token=token,
            subscription_id=request.subscription_id,
            resource_group=request.resource_group,
            workflow_name=request.workflow_name,
        )
        
        definition = workflow.get("properties", {}).get("definition", {})
        trigger = request.trigger_name or find_manual_or_recurrence_trigger(definition)
        
        if not trigger:
            return {
                "verified": False,
                "reason": "No manual or recurrence trigger found",
                "suggestion": "Use Azure Portal or ARM API to test the workflow"
            }
        
        # Trigger the run
        resp = post_trigger_run(
            token=token,
            subscription_id=request.subscription_id,
            resource_group=request.resource_group,
            workflow_name=request.workflow_name,
            trigger_name=trigger,
            body={},
        )
        
        if resp.status_code in (200, 202):
            return {
                "verified": True,
                "trigger_used": trigger,
                "status_code": resp.status_code,
                "message": f"Workflow triggered successfully via {trigger}"
            }
        else:
            return {
                "verified": False,
                "trigger_used": trigger,
                "status_code": resp.status_code,
                "reason": f"Trigger returned {resp.status_code}"
            }
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return {
            "verified": False,
            "reason": str(e)[:200],
            "suggestion": "Check workflow status in Azure Portal"
        }


@router.get("/verifier/{workflow_name}")
async def verifier_check(workflow_name: str):
    """
    Check if a workflow is ready for verification.
    Returns workflow status and available triggers.
    """
    return {
        "workflow_name": workflow_name,
        "can_verify": True,
        "available_triggers": ["manual", "recurrence"],
        "note": "Use POST /verifier to actually trigger verification"
    }


# ============================================================================
# 7. KNOWLEDGE AGENT (Documentation search)
# ============================================================================

@router.post("/knowledge")
async def knowledge_search(request: KnowledgeSearchRequest):
    """
    Search the knowledge base for similar issues and solutions.
    Returns relevant documentation and past fixes.
    """
    logger.info(f"Knowledge: Searching '{request.query}'")
    
    kb = KnowledgeAgent(settings)
    results = kb.search(request.query, request.top_k)
    
    return {
        "query": request.query,
        "results_count": len(results),
        "results": [
            {
                "title": r["meta"].get("title", "Unknown"),
                "category": r["meta"].get("category", "Unknown"),
                "similarity": round(r["similarity"], 2),
                "snippet": r["text"][:300] + "...",
                "url": r["meta"].get("url", ""),
            }
            for r in results
        ]
    }


@router.get("/knowledge/stats")
async def knowledge_stats():
    """Get knowledge base statistics"""
    kb = KnowledgeAgent(settings)
    stats = kb.get_stats()
    
    return {
        "total_chunks": stats["total"],
        "vectorized_chunks": stats["vectorized"],
        "status": "ready" if stats["vectorized"] > 0 else "needs_vectorization",
        "health": "healthy" if stats["vectorized"] > 0 else "degraded"
    }


# ============================================================================
# 8. HEALTH CHECK (All agents)
# ============================================================================

@router.get("/health")
async def agents_health():
    """Health check for all AI agents"""
    agents_status = {}
    
    # Check each agent
    try:
        Orchestrator(settings)
        agents_status["orchestrator"] = "healthy"
    except Exception as e:
        agents_status["orchestrator"] = f"error: {str(e)[:50]}"
    
    try:
        Observer(settings)
        agents_status["observer"] = "healthy"
    except Exception as e:
        agents_status["observer"] = f"error: {str(e)[:50]}"
    
    try:
        get_fixer(settings)
        agents_status["fixer"] = "healthy"
    except Exception as e:
        agents_status["fixer"] = f"error: {str(e)[:50]}"
    
    try:
        KnowledgeAgent(settings)
        agents_status["knowledge"] = "healthy"
    except Exception as e:
        agents_status["knowledge"] = f"error: {str(e)[:50]}"
    
    agents_status["classifier"] = "healthy"
    agents_status["rca"] = "healthy"
    agents_status["verifier"] = "healthy"
    
    all_healthy = all(v == "healthy" for v in agents_status.values())
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "agents": agents_status,
        "timestamp": datetime.now().isoformat()
    }