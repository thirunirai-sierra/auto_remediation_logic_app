# server/routers/observability.py
"""
Observability endpoints for incident tracking and AI‑assisted analysis.
Uses SAP AI Core LLM and HANA knowledge base for explanations and fix generation.
"""

import json,logging,asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from db.hana_client import get_global_client
from config import get_settings
from services.auth import get_arm_token
from services.workflow_service import get_workflow
from services.agents.knowledge.knowledge_base import KnowledgeAgent
from services.agents.knowledge.embedder import get_embedder
from utils.llm_client import AICoreLLMClient

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()


class ApplyFixRequest(BaseModel):
    trigger_type: str = "user"
    proposed_fix: Optional[str] = None
    force: bool = False
class UpdateTicketRequest(BaseModel):
    status: str
    resolution_notes: Optional[str] = None

class ApproveIncidentRequest(BaseModel):
    approved: bool
    comment: Optional[str] = None

def get_hana_client():
    """Return the singleton HANA client."""
    return get_global_client()

# Incident listing and detail

@router.get("/monitor/messages")
async def get_monitor_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    """
    List incidents with pagination and filtering.

    Args:
        limit: Max number of records to return.
        offset: Number of records to skip.
        status: Filter by status (case‑insensitive).
        search: Search in workflow name or error message.

    Returns:
        dict: Contains 'messages' list and 'total' count.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    conditions = []
    params = []
    if status and status.upper() != "ALL":
        conditions.append("UPPER(STATUS) = ?")
        params.append(status.upper())
    if search:
        conditions.append("(WORKFLOW_NAME LIKE ? OR ERROR_MESSAGE LIKE ?)")
        params.append(f"%{search}%")
        params.append(f"%{search}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    count_sql = f"SELECT COUNT(*) FROM {client.full_table} {where}"
    cursor.execute(count_sql, params)
    total = cursor.fetchone()[0]

    data_sql = f"""
        SELECT INCIDENT_ID, WORKFLOW_NAME, STATUS, ERROR_CATEGORY, CREATED_AT, UPDATED_AT,
               ERROR_MESSAGE, RCA_ROOT_CAUSE
        FROM {client.full_table}
        {where}
        ORDER BY CREATED_AT DESC
        LIMIT ? OFFSET ?
    """
    cursor.execute(data_sql, params + [limit, offset])
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    messages = []
    for row in rows:
        d = dict(zip(cols, row))
        messages.append({
            "message_guid": d["INCIDENT_ID"],
            "iflow_display": d["WORKFLOW_NAME"],
            "title": d["WORKFLOW_NAME"],
            "status": d["STATUS"],
            "log_start": d["CREATED_AT"].isoformat() if d["CREATED_AT"] else None,
            "updatedAt": d["UPDATED_AT"].isoformat() if d["UPDATED_AT"] else None,
            "error_type": d["ERROR_CATEGORY"],
        })
    cursor.close()
    return {"messages": messages, "total": total}


@router.get("/monitor/message/{incident_id}")
async def get_monitor_message_detail(incident_id: str):
    """
    Get full details of a specific incident, including artifact, history, and related knowledge.

    Args:
        incident_id: Unique incident identifier.

    Returns:
        dict: Complete incident information.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    sql = f"""
        SELECT INCIDENT_ID, WORKFLOW_NAME, ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
               STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY, CREATED_AT, UPDATED_AT,
               AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
               AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
               AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
               PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON
        FROM {client.full_table}
        WHERE INCIDENT_ID = ?
    """
    cursor.execute(sql, (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    cols = [desc[0] for desc in cursor.description]
    rec = dict(zip(cols, row))
    cursor.close()

    error_details = {
        "error_message": rec.get("ERROR_MESSAGE"),
        "raw_error_text": rec.get("ERROR_MESSAGE"),
        "error_type": rec.get("ERROR_CATEGORY"),
        "log_start": rec["CREATED_AT"].isoformat() if rec.get("CREATED_AT") else None,
        "log_end": rec["UPDATED_AT"].isoformat() if rec.get("UPDATED_AT") else None,
    }

    ai_diag = rec.get("AI_DIAGNOSIS") or ""
    ai_proposed = rec.get("AI_PROPOSED_FIX") or ""
    ai_conf = rec.get("AI_CONFIDENCE") or 0.0
    fix_patch = json.loads(rec.get("AI_FIX_PATCH") or "null")
    field_changes = json.loads(rec.get("FIELD_CHANGES") or "[]")
    history = json.loads(rec.get("HISTORY_ENTRIES") or "[]")
    properties = json.loads(rec.get("PROPERTIES_JSON") or "{}")
    artifact = json.loads(rec.get("ARTIFACT_JSON") or "{}")

    can_generate_fix = bool(ai_diag)  # show Generate Fix button whenever AI diagnosis exists

    # Knowledge base search
    related_knowledge = []
    error_msg = rec.get("ERROR_MESSAGE") or ""
    if error_msg:
        try:
            embedder = get_embedder()
            query_vec = embedder.embed(error_msg)
            similar = client.search_similar(query_vec, top_k=5)
            related_knowledge = [
                {
                    "title": chunk["meta"].get("title", "Knowledge entry"),
                    "content": chunk["text"][:500],
                    "similarity": round(chunk["similarity"], 1)
                }
                for chunk in similar
            ]
        except Exception as e:
            logger.warning("Knowledge search failed: %s", e)

    return {
        "incident_id": incident_id,
        "iflow_display": rec["WORKFLOW_NAME"],
        "status": rec["STATUS"],
        "last_updated": rec["UPDATED_AT"].isoformat() if rec["UPDATED_AT"] else None,
        "error_details": error_details,
        "ai_recommendation": {
            "diagnosis": ai_diag,
            "proposed_fix": ai_proposed,
            "confidence": ai_conf,
            "confidence_label": "High" if ai_conf >= 0.9 else "Medium" if ai_conf >= 0.7 else "Low",
            "fix_patch": fix_patch,
            "field_changes": field_changes,
            "can_generate_fix": can_generate_fix,
            "fix_summary": rec.get("FIX_STRATEGY"),
        },
        "properties": properties,
        "artifact": artifact,
        "attachments": [],
        "history": history,
        "incident_status": rec["STATUS"],
        "related_knowledge": related_knowledge,
    }

# AI analysis endpoints
@router.post("/monitor/analyze/{incident_id}")
async def analyze_message(incident_id: str):
    """
    Trigger AI analysis for an incident using SAP AI Core LLM and HANA knowledge base.
    Updates AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE in HANA.

    Args:
        incident_id: Incident identifier.

    Returns:
        dict: Analysis status and diagnosis.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT WORKFLOW_NAME, ERROR_MESSAGE, ERROR_CODE, SUBSCRIPTION_ID FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, error_msg, error_code, _sub_id = row
    cursor.close()

    llm = AICoreLLMClient.from_env()
    system_prompt = (
        "You are an Azure Logic Apps expert. Analyze the error and provide diagnosis, proposed fix, and confidence (0-1). "
        "Return ONLY JSON with keys: diagnosis, proposed_fix, confidence."
    )
    user_prompt = f"Workflow: {workflow_name}\nError code: {error_code}\nError message: {error_msg}"

    result = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
    if not result:
        raise HTTPException(500, "LLM analysis failed")

    diagnosis = result.get("diagnosis", "")
    proposed_fix = result.get("proposed_fix", "")
    confidence = float(result.get("confidence", 0.7))

    # Enhance with knowledge base
    try:
        knowledge = KnowledgeAgent(settings)
        similar = knowledge.search(f"{error_code} {error_msg}", 2)
        if similar:
            kb_text = "\n".join([chunk["text"] for chunk in similar])
            enhancement_prompt = (
                f"Refine the diagnosis and fix using this knowledge:\n{kb_text}\n\n"
                f"Current diagnosis: {diagnosis}\nCurrent fix: {proposed_fix}"
            )
            enhanced = await llm.complete_json(
                system_prompt="You are a technical writer. Improve the diagnosis and fix using the knowledge. "
                              "Return JSON with keys: diagnosis, proposed_fix.",
                user_prompt=enhancement_prompt,
            )
            if enhanced:
                diagnosis = enhanced.get("diagnosis", diagnosis)
                proposed_fix = enhanced.get("proposed_fix", proposed_fix)
    except Exception as e:
        logger.warning("Knowledge base enhancement failed: %s", e)

    cursor = client.conn.cursor()
    update_sql = f"""
        UPDATE {client.full_table}
        SET AI_DIAGNOSIS = ?, AI_PROPOSED_FIX = ?, AI_CONFIDENCE = ?,
            RCA_ROOT_CAUSE = COALESCE(RCA_ROOT_CAUSE, ?)
        WHERE INCIDENT_ID = ?
    """
    confidence = float(confidence) if confidence is not None else 0.0
    cursor.execute(update_sql, (diagnosis, proposed_fix, confidence, diagnosis, incident_id))
    client.conn.commit()
    cursor.close()
    return {"status": "analyzed", "diagnosis": diagnosis, "confidence": confidence}


@router.post("/monitor/explain/{incident_id}")
async def explain_error(incident_id: str):
    """
    Get a human‑readable explanation of the error using LLM.

    Args:
        incident_id: Incident identifier.

    Returns:
        dict: Explanation structured as summary, causes, actions.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT ERROR_MESSAGE, ERROR_CODE, WORKFLOW_NAME FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    error_msg, error_code, workflow = row
    cursor.close()

    llm = AICoreLLMClient.from_env()
    system_prompt = (
        "You are an expert in Azure Logic Apps. Explain the error in simple terms, suggest likely causes, "
        "and give recommended actions. Return JSON with keys: summary, what_happened, likely_causes (list), "
        "recommended_actions (list), error_category."
    )
    user_prompt = f"Workflow: {workflow}\nError code: {error_code}\nError message: {error_msg}"
    response = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
    if not response:
        raise HTTPException(500, "LLM explanation failed")
    return response


@router.post("/monitor/generate-fix/{incident_id}")
async def generate_fix_patch(incident_id: str):
    """
    Generate a structured fix patch (JSON) using LLM and workflow definition.
    Stores AI_FIX_PATCH in HANA.

    Args:
        incident_id: Incident identifier.

    Returns:
        dict: Fix plan (summary + steps).
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_MESSAGE, ERROR_CATEGORY FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, error_msg, error_category = row
    cursor.close()

    # Use subscription_id from record, fall back to settings
    effective_sub_id = sub_id or settings.AZURE_SUBSCRIPTION_ID

    # Fetch workflow definition — optional; proceed without it if ARM is unavailable
    definition = {}
    try:
        if effective_sub_id and settings.AZURE_TENANT_ID and settings.AZURE_CLIENT_ID:
            token = get_arm_token(settings.AZURE_TENANT_ID, settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET)
            workflow = await asyncio.to_thread(
                get_workflow, token, effective_sub_id, settings.AZURE_RESOURCE_GROUP, workflow_name
            )
            definition = workflow.get("properties", {}).get("definition", {})
    except Exception as e:
        logger.warning("Could not fetch workflow definition for fix generation (continuing without it): %s", e)

    llm = AICoreLLMClient.from_env()
    system_prompt = (
        "You are a Logic Apps fix generator. Based on the error and workflow definition, "
        "produce a structured fix plan. Return JSON with keys: summary, steps (list of objects with step_number, title, description, sub_steps, note)."
    )
    user_prompt = f"""
Error category: {error_category}
Error message: {error_msg}
Workflow definition snippet: {json.dumps(definition, default=str)[:2000]}
"""
    fix_patch = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
    if not fix_patch:
        fix_patch = {
            "summary": f"Fix for {error_category}",
            "steps": [
                {
                    "step_number": 1,
                    "title": "Review and adjust",
                    "description": "Manual review required because automated fix generation failed.",
                    "sub_steps": ["Inspect action inputs", "Validate payload", "Check API contract"],
                    "note": "No automated patch generated"
                }
            ]
        }

    cursor = client.conn.cursor()
    cursor.execute(
        f"UPDATE {client.full_table} SET AI_FIX_PATCH = ? WHERE INCIDENT_ID = ?",
        (json.dumps(fix_patch), incident_id)
    )
    client.conn.commit()
    cursor.close()
    return fix_patch


@router.post("/monitor/apply-fix/{incident_id}")
async def apply_message_fix(incident_id: str, req: ApplyFixRequest):
    """
    Apply fix by triggering the orchestrator (if auto‑fix is allowed).
    Updates status to AUTO_FIXED or FIX_FAILED.

    If the incident record does not contain a failed_action_name, this endpoint
    will attempt to retrieve it by calling the Observer with the original Azure run_id
    (stored in the RUN_ID column).

    Args:
        incident_id: Incident identifier.
        req: Request body (trigger_type, proposed_fix, force).

    Returns:
        dict: New status and summary.
    """
    from services.agents.orchestrator import Orchestrator
    from routers.settings import get_error_policy
    from services.agents.observer import Observer
    import json as _json
    import asyncio as _asyncio
    from services.agents.fixer.Fixer_agent import FixerAgent

    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_CATEGORY, ERROR_MESSAGE, RESOURCE_GROUP, RUN_ID "
        f"FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, error_category, _error_msg, resource_group_db, run_id = row
    cursor.close()

    if not workflow_name or str(workflow_name).lower() in ("none", "unknown", ""):
        return {
            "status": "SKIPPED",
            "summary": (
                "Cannot apply fix: the incident record has no workflow_name. "
                "The Logic App name must be known to retrieve and patch the workflow in Azure. "
                "Update the incident with the correct workflow name and retry."
            ),
        }

    # Check remediation policy (force bypasses it)
    policy = get_error_policy(error_category or "UNKNOWN_ERROR")
    logger.info("apply-fix: incident=%s error=%s policy=%s force=%s", incident_id, error_category, policy, req.force)

    if not req.force:
        if policy == "AWAITING_APPROVAL":
            cursor = client.conn.cursor()
            cursor.execute(f"UPDATE {client.full_table} SET STATUS = 'AWAITING_APPROVAL' WHERE INCIDENT_ID = ?", (incident_id,))
            client.conn.commit()
            cursor.close()
            return {"status": "AWAITING_APPROVAL", "summary": f"Policy for {error_category} requires human approval. Click Apply Fix to override and deploy."}
        if policy == "RETRY":
            cursor = client.conn.cursor()
            cursor.execute(f"UPDATE {client.full_table} SET STATUS = 'RETRIED', AUTO_FIX_ATTEMPTED = TRUE, AUTO_FIX_SUCCESS = TRUE WHERE INCIDENT_ID = ?", (incident_id,))
            client.conn.commit()
            cursor.close()
            return {"status": "RETRIED", "summary": f"Policy for {error_category} triggers automatic retry."}
        if policy == "TICKET_CREATED":
            cursor = client.conn.cursor()
            cursor.execute(f"UPDATE {client.full_table} SET STATUS = 'TICKET_CREATED', AUTO_FIX_ATTEMPTED = TRUE, AUTO_FIX_SUCCESS = FALSE WHERE INCIDENT_ID = ?", (incident_id,))
            client.conn.commit()
            cursor.close()
            return {"status": "TICKET_CREATED", "summary": f"Policy for {error_category} escalates to a ticket for manual review."}
    else:
        logger.info("apply-fix: force=True — bypassing policy %s, applying fix directly", policy)

    effective_sub_id = sub_id or settings.AZURE_SUBSCRIPTION_ID
    effective_rg = resource_group_db or settings.AZURE_RESOURCE_GROUP

    # Guard: Azure ARM credentials required
    if not (settings.AZURE_TENANT_ID and settings.AZURE_CLIENT_ID
            and settings.AZURE_CLIENT_SECRET and effective_sub_id):
        logger.info("apply-fix: ARM credentials not configured — queuing for manual approval")
        cursor = client.conn.cursor()
        cursor.execute(
            f"UPDATE {client.full_table} SET STATUS = 'AWAITING_APPROVAL', AUTO_FIX_ATTEMPTED = TRUE WHERE INCIDENT_ID = ?",
            (incident_id,)
        )
        client.conn.commit()
        cursor.close()
        return {
            "status": "AWAITING_APPROVAL",
            "summary": "Azure ARM credentials not configured. Review the AI fix plan and apply manually.",
        }

    # Read AI data from HANA
    cursor = client.conn.cursor()
    cursor.execute(
        f"""SELECT AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE, AI_FIX_PATCH,
                   RCA_ROOT_CAUSE, ERROR_MESSAGE
            FROM {client.full_table} WHERE INCIDENT_ID = ?""",
        (incident_id,)
    )
    ai_row = cursor.fetchone()
    cursor.close()

    ai_diagnosis = (ai_row[0] or "") if ai_row else ""
    ai_proposed_fix = (ai_row[1] or "") if ai_row else ""
    ai_confidence = float(ai_row[2] or 0.0) if ai_row else 0.0
    ai_fix_patch_raw = (ai_row[3] or "null") if ai_row else "null"
    rca_root_cause = (ai_row[4] or "unknown") if ai_row else "unknown"
    error_message = (ai_row[5] or "") if ai_row else ""

    # Try to extract failed_action_name from AI fix patch first
    failed_action_name = None
    try:
        fix_patch_data = _json.loads(ai_fix_patch_raw) if ai_fix_patch_raw and ai_fix_patch_raw != "null" else {}
        if isinstance(fix_patch_data, dict):
            failed_action_name = (
                fix_patch_data.get("failed_action_name")
                or fix_patch_data.get("affected_component")
            )
    except Exception:
        pass

    # If still missing, use Observer with the original run_id (stored in RUN_ID column)
    if not failed_action_name and run_id and not run_id.startswith("ORBLOGICAPPS-"):
        logger.info("Failed action name missing — calling Observer for run_id=%s", run_id)
        try:
            observer = Observer(settings)
            obs_result = await _asyncio.to_thread(
                observer.analyze_failed_run,
                effective_sub_id,
                effective_rg,
                workflow_name,
                run_id
            )
            if obs_result.get("status") == "failed_action_found":
                failed_action_name = obs_result.get("failed_action_name")
                logger.info("Observer resolved failed_action_name=%s", failed_action_name)
                # Persist it back to HANA for future use
                cursor2 = client.conn.cursor()
                cursor2.execute(
                    f"UPDATE {client.full_table} SET FAILED_ACTION_NAME = ? WHERE INCIDENT_ID = ?",
                    (failed_action_name, incident_id)
                )
                client.conn.commit()
                cursor2.close()
            else:
                logger.warning("Observer could not determine failed action for run_id=%s", run_id)
        except Exception as e:
            logger.error("Observer call failed for incident %s: %s", incident_id, e)

    if not failed_action_name:
        logger.error("Could not resolve failed_action_name for incident %s", incident_id)
        return {
            "status": "FIX_FAILED",
            "summary": (
                "The failed action name could not be determined automatically. "
                "Please review the workflow run history manually and then retry with the correct action name, "
                "or use the Orchestrator API directly."
            )
        }

    # Build RCA result and workflow context
    rca_result = {
        "root_cause": rca_root_cause,
        "suggested_fix": ai_proposed_fix or req.proposed_fix or "",
        "confidence": ai_confidence,
        "solution": ai_diagnosis,
        "exact_issue": error_message,
    }

    workflow_context = {
        "workflow_name": workflow_name,
        "run_id": incident_id,
        "subscription_id": effective_sub_id,
        "resource_group": effective_rg,
        "failed_action_name": failed_action_name,
        "backup_dir": None,
        "error_type": error_category,
        "suggested_fix": rca_result["suggested_fix"],
    }

    # Run Fixer
    def _run_fixer():
        fixer = FixerAgent(settings)
        return fixer.fix(rca_result, workflow_context)

    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"UPDATE {client.full_table} SET STATUS = 'FIX_IN_PROGRESS', AUTO_FIX_ATTEMPTED = TRUE WHERE INCIDENT_ID = ?",
            (incident_id,)
        )
        client.conn.commit()
        cursor.close()

        fix_result = await _asyncio.wait_for(
            _asyncio.to_thread(_run_fixer),
            timeout=180.0,
        )
    except _asyncio.TimeoutError:
        logger.error("Fixer timed out for %s", incident_id)
        fix_result = {"success": False, "error": "Fix timed out after 3 minutes."}
    except Exception as exc:
        logger.error("Fixer failed for %s: %s", incident_id, exc, exc_info=True)
        fix_result = {"success": False, "error": str(exc)}

    success = bool(fix_result.get("success"))
    new_status = "AUTO_FIXED" if success else "FIX_FAILED"
    fix_strategy = fix_result.get("fix_strategy") or {}
    summary = (
        fix_strategy.get("strategy_description")
        or fix_result.get("error")
        or ("Fix applied and deployed successfully." if success else "Fix failed — see logs.")
    )

    cursor = client.conn.cursor()
    cursor.execute(
        f"UPDATE {client.full_table} SET STATUS = ?, AUTO_FIX_SUCCESS = ?, FIX_STRATEGY = ? WHERE INCIDENT_ID = ?",
        (new_status, success, summary, incident_id)
    )
    client.conn.commit()
    cursor.close()
    return {"status": new_status, "summary": summary}


@router.get("/monitor/fix-status/{incident_id}")
async def get_fix_status(incident_id: str):
    """
    Poll the status of a fix attempt.

    Args:
        incident_id: Incident identifier.

    Returns:
        dict: Status, fix summary, and progress steps.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT STATUS, AUTO_FIX_SUCCESS, FIX_STRATEGY FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    status, success, fix_summary = row
    cursor.close()

    steps_done = ["Submit", "Get Workflow", "Validate", "Patch", "Deploy"] if success else []
    return {
        "status": status,
        "fix_summary": fix_summary,
        "current_step": "Completed" if success else "Failed",
        "step_index": 5 if success else 0,
        "total_steps": 5,
        "steps_done": steps_done,
    }



# New endpoints for frontend dashboard


@router.get("/logs/overview")
async def get_logs_overview(top: int = Query(1000, ge=1, le=5000)):
    """Return aggregated metrics for logs dashboard (KPIs, distributions, timeline, recent errors)."""
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    table = client.full_table

    # KPIs
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total_logs = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM {table}")
    total_flows = cursor.fetchone()[0] or 0
    cursor.execute(f"SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM {table} WHERE STATUS IN ('FIX_FAILED','FAILED')")
    error_flows = cursor.fetchone()[0] or 0
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE STATUS IN ('AUTO_FIXED','FIX_VERIFIED')")
    fixed_flows = cursor.fetchone()[0] or 0
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ERROR_MESSAGE IS NOT NULL")
    total_error_messages = cursor.fetchone()[0]

    # Status breakdown
    cursor.execute(f"SELECT STATUS, COUNT(*) FROM {table} GROUP BY STATUS ORDER BY COUNT(*) DESC")
    status_breakdown = [{"status": row[0], "count": row[1]} for row in cursor.fetchall()]

    # Error distribution
    cursor.execute(f"SELECT ERROR_CATEGORY, COUNT(*) FROM {table} WHERE ERROR_CATEGORY IS NOT NULL GROUP BY ERROR_CATEGORY ORDER BY COUNT(*) DESC")
    error_distribution = [{"error_type": row[0] or "UNKNOWN", "count": row[1]} for row in cursor.fetchall()]

    # Top iFlows (workflows with most failures)
    cursor.execute(f"SELECT WORKFLOW_NAME, COUNT(*) FROM {table} GROUP BY WORKFLOW_NAME ORDER BY COUNT(*) DESC LIMIT 10")
    top_iflows = [{"iflow_name": row[0], "failure_count": row[1]} for row in cursor.fetchall()]

    # Timeline (group by day)
    cursor.execute(f"""
        SELECT CAST(CREATED_AT AS DATE) AS log_date, COUNT(*) as count
        FROM {table}
        WHERE CREATED_AT IS NOT NULL
        GROUP BY CAST(CREATED_AT AS DATE)
        ORDER BY CAST(CREATED_AT AS DATE) DESC
        LIMIT 30
        """)
    timeline = [{"time": str(row[0]), "count": row[1]} for row in cursor.fetchall()]

    # Recent error messages (limit top)
    cursor.execute(f"""
        SELECT WORKFLOW_NAME, ERROR_CODE, ERROR_MESSAGE, CREATED_AT, SUBSCRIPTION_ID, STATUS, INCIDENT_ID
        FROM {table}
        WHERE ERROR_MESSAGE IS NOT NULL
        ORDER BY CREATED_AT DESC
        LIMIT {top}
    """)
    error_messages = []
    for row in cursor.fetchall():
        error_messages.append({
            "integrationScenario": row[0],
            "errorType": row[1],
            "errorMessage": row[2],
            "time": row[3].isoformat() if row[3] else None,
            "resourceId": None,
            "status": row[5],
            "runId": row[6],
        })
    cursor.close()

    return {
        "kpi": {
            "total_flows": total_flows,
            "error_flows": error_flows,
            "fixed_flows": fixed_flows,
            "total_logs": total_logs,
            "total_error_messages": total_error_messages,
        },
        "status_breakdown": status_breakdown,
        "error_distribution": error_distribution,
        "top_iflows": top_iflows,
        "timeline": timeline,
        "error_messages": error_messages,
    }


@router.get("/incidents")
async def get_incidents():
    """Return a simplified list of incidents for the logs page."""
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(f"""
        SELECT INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME, ERROR_CODE, ERROR_MESSAGE, CREATED_AT
        FROM {client.full_table}
        ORDER BY CREATED_AT DESC
        LIMIT 500
    """)
    rows = cursor.fetchall()
    incidents = []
    for row in rows:
        incidents.append({
            "incidentId": row[0],
            "subscriptionId": row[1],
            "integrationScenario": row[2],
            "errorType": row[3],
            "errorMessage": row[4],
            "time": row[5].isoformat() if row[5] else None,
        })
    cursor.close()
    return incidents


# Stubs for tickets and approvals (kept for compatibility)
@router.get("/tickets")
async def get_tickets():
    return {"tickets": []}


@router.post("/tickets/{ticket_id}/update")
async def update_ticket(_ticket_id: str, _req: UpdateTicketRequest):
    return {"status": "updated"}


@router.get("/approvals/pending")
async def get_pending_approvals():
    return {"pending": []}


@router.post("/approvals/{incident_id}/approve")
async def approve_incident(_incident_id: str, _req: ApproveIncidentRequest):
    return {"status": "processed"}


@router.get("/aem/status")
async def aem_status():
    from routers.event_mesh import event_mesh_status
    return await event_mesh_status()


@router.get("/aem/incidents")
async def aem_incidents(limit: int = 100):
    client = get_hana_client()
    if not client or not client._ensure_connected():
        return {"incidents": []}
    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"""SELECT INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME, ERROR_CODE,
                       ERROR_MESSAGE, CREATED_AT, STATUS, ERROR_CATEGORY
                FROM {client.full_table}
                ORDER BY CREATED_AT DESC
                LIMIT ?""",
            (limit,)
        )
        rows = cursor.fetchall()
        cols = [d[0].lower() for d in cursor.description]
        cursor.close()
        return {"incidents": [dict(zip(cols, row)) for row in rows]}
    except Exception as exc:
        logger.warning("aem_incidents query failed: %s", exc)
        return {"incidents": []}


@router.get("/mcp/tools")
async def mcp_tools():
    return {"total": 0, "servers": {}}