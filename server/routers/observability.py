# server/routers/observability.py
"""
Observability endpoints for incident tracking and AI‑assisted analysis.
Uses SAP AI Core LLM and HANA knowledge base for explanations and fix generation.
"""
import json
import logging
import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any

from db.hana_client import get_global_client   #  Use singleton
from config import get_settings
from services.auth import get_arm_token
from services.workflow_service import get_workflow
from services.agents.knowledge.knowledge_base import KnowledgeAgent
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


# Use singleton client
def get_hana_client():
    """Return the singleton HANA client for observability."""
    return get_global_client()


@router.get("/monitor/messages")
async def get_monitor_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    """List incidents with pagination and filtering."""
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
        messages.append(
            {
                "message_guid": d["INCIDENT_ID"],
                "iflow_display": d["WORKFLOW_NAME"],
                "title": d["WORKFLOW_NAME"],
                "status": d["STATUS"],
                "log_start": d["CREATED_AT"].isoformat() if d["CREATED_AT"] else None,
                "updatedAt": d["UPDATED_AT"].isoformat() if d["UPDATED_AT"] else None,
                "error_type": d["ERROR_CATEGORY"],
            }
        )
    cursor.close()
    return {"messages": messages, "total": total, "summary": summary}



@router.get("/monitor/message/{incident_id}")
async def get_monitor_message_detail(incident_id: str):
    """Get full details of a specific incident."""
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

    # Restore can_generate_fix field
    can_generate_fix = rec.get("AUTO_FIX_ATTEMPTED") is not None and not rec.get("AUTO_FIX_SUCCESS")


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
    }



@router.post("/monitor/analyze/{incident_id}")
async def analyze_message(incident_id: str):
    """
    Trigger AI analysis for an incident using SAP AI Core LLM and HANA knowledge base.
    Updates AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE in HANA.
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

    # Use LLM to generate diagnosis and proposed fix (offloaded to thread)
    llm = AICoreLLMClient.from_env()
    system_prompt = (
        "You are an Azure Logic Apps expert. Analyze the error and provide diagnosis, proposed fix, and confidence (0-1). "
        "Return ONLY JSON with keys: diagnosis, proposed_fix, confidence."
    )
    user_prompt = f"Workflow: {workflow_name}\nError code: {error_code}\nError message: {error_msg}"
    result = await asyncio.to_thread(llm.complete_json, system_prompt=system_prompt, user_prompt=user_prompt)

    if not result:
        raise HTTPException(500, "LLM analysis failed")

    diagnosis = result.get("diagnosis", "")
    proposed_fix = result.get("proposed_fix", "")
    confidence = float(result.get("confidence", 0.7))

    # Optional: enhance with HANA knowledge base (also offloaded)
    try:
        knowledge = KnowledgeAgent(settings)
        similar = await asyncio.to_thread(knowledge.search, f"{error_code} {error_msg}", 2)
        if similar:
            kb_text = "\n".join([chunk["text"] for chunk in similar])
            enhancement_prompt = f"Refine the diagnosis and fix using this knowledge:\n{kb_text}\n\nCurrent diagnosis: {diagnosis}\nCurrent fix: {proposed_fix}"
            enhanced = await asyncio.to_thread(
                llm.complete_json,
                system_prompt="You are a technical writer. Improve the diagnosis and fix using the knowledge. Return JSON with keys: diagnosis, proposed_fix.",
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
    cursor.execute(update_sql, (diagnosis, proposed_fix, confidence, diagnosis, incident_id))
    client.conn.commit()
    cursor.close()
    return {"status": "analyzed", "diagnosis": diagnosis, "confidence": confidence}



@router.post("/monitor/explain/{incident_id}")
async def explain_error(incident_id: str):
    """Get human‑readable explanation of the error using LLM."""
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
    response = await asyncio.to_thread(llm.complete_json, system_prompt=system_prompt, user_prompt=user_prompt)
    if not response:
        raise HTTPException(500, "LLM explanation failed")
    return response


@router.post("/monitor/generate-fix/{incident_id}")
async def generate_fix_patch(incident_id: str):
    """
    Generate a structured fix patch (JSON) using LLM and workflow definition.
    Stores AI_FIX_PATCH in HANA.
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

    # Fetch current workflow definition (offloaded)
    token = get_arm_token(settings.AZURE_TENANT_ID, settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET)
    workflow = await asyncio.to_thread(
        get_workflow, token, sub_id, settings.AZURE_RESOURCE_GROUP, workflow_name
    )
    definition = workflow.get("properties", {}).get("definition", {})

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
    fix_patch = await asyncio.to_thread(llm.complete_json, system_prompt=system_prompt, user_prompt=user_prompt)
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
    """
    from services.agents.orchestrator import Orchestrator

    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_CATEGORY, ERROR_MESSAGE FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, _error_category, _error_msg = row
    cursor.close()

    # Use orchestrator to remediate
    orch = Orchestrator(settings)
    result = await orch.remediate(
        workflow_name=workflow_name,
        run_id=incident_id,
        subscription_id=sub_id,
        resource_group=settings.AZURE_RESOURCE_GROUP,
        backup_dir=None,
    )

    success = result.get("status") == "remediated"
    new_status = "AUTO_FIXED" if success else "FIX_FAILED"

    cursor = client.conn.cursor()
    cursor.execute(
        f"UPDATE {client.full_table} SET STATUS = ?, AUTO_FIX_ATTEMPTED = TRUE, AUTO_FIX_SUCCESS = ? WHERE INCIDENT_ID = ?",
        (new_status, success, incident_id)
    )
    client.conn.commit()
    cursor.close()
    return {"status": new_status, "summary": result.get("fix_strategy", {}).get("strategy_description", "")}


@router.get("/monitor/fix-status/{incident_id}")
async def get_fix_status(incident_id: str):
    """Poll the status of a fix attempt."""
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

    steps_done = ["Submit", "Get iFlow", "Validate", "Patch", "Deploy"] if success else []
    return {
        "status": status,
        "fix_summary": fix_summary,
        "current_step": "Completed" if success else "Failed",
        "step_index": 5 if success else 0,
        "total_steps": 5,
        "steps_done": steps_done,
    }


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
    return {"event_mesh_enabled": True, "messages_retrieved": 0, "total_incidents": 0, "queue_depth": 0}



@router.get("/aem/incidents")
async def aem_incidents(limit: int = 100):
    return {"incidents": []}



@router.get("/mcp/tools")
async def mcp_tools():
    return {"total": 0, "servers": {}}
