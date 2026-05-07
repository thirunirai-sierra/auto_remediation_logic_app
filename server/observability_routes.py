# observability_routes.py
import sys
from pathlib import Path
# Add parent directory to path so we can import modules from root
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import json
import re
import requests
from datetime import datetime

from config import get_settings
from auth import get_arm_token
from api import get_workflow
from agent.classifier.analyzer import analyze_error
from agent.rca_agent import generate_rca
from workflow_agent import run_remediation
from server.hana_observability import get_hana_client
from hana_observability import get_global_client
router = APIRouter(prefix="/api", tags=["observability"])
settings = get_settings()

# Request/response models
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

def get_client():
     return get_global_client()  


# ---------- Message List ----------
@router.get("/monitor/messages")
async def get_monitor_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    client = get_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "Observability database not available")
    
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

# ---------- Message Detail ----------
@router.get("/monitor/message/{incident_id}")
async def get_monitor_message_detail(incident_id: str):
    client = get_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "Database unavailable")
    
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

# ---------- Analyze (AI) ----------
@router.post("/monitor/analyze/{incident_id}")
async def analyze_message(incident_id: str):
    client = get_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "Database unavailable")
    
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT WORKFLOW_NAME, ERROR_MESSAGE, ERROR_CODE, SUBSCRIPTION_ID FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, error_msg, error_code, sub_id = row
    cursor.close()
    
    error_json = {"message": error_msg, "code": error_code}
    flow_context = {"workflow_name": workflow_name, "run_id": incident_id}
    
    analysis = analyze_error(error_json, settings, flow_context=flow_context)
    rca = generate_rca({"error": error_msg}, flow_context=flow_context)
    diagnosis = f"{analysis.get('root_cause', '')} {rca.get('root_cause', '')}".strip()
    proposed_fix = analysis.get('recommendation', '')
    confidence = rca.get('confidence', 0.7)
    
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

# ---------- Explain Error (LLM) ----------
def _call_llm_explanation(error_msg, error_code, workflow):
    url = f"{settings.azure_openai_endpoint}/openai/deployments/{settings.azure_openai_deployment}/chat/completions?api-version={settings.azure_openai_api_version}"
    system = "You are an expert in Azure Logic Apps. Explain the error in simple terms, suggest likely causes, and give recommended actions. Return JSON with keys: summary, what_happened, likely_causes (list), recommended_actions (list), error_category."
    user = f"Error: {error_msg}\nCode: {error_code}\nWorkflow: {workflow}"
    response = requests.post(url, headers={"api-key": settings.azure_openai_api_key}, json={"messages": [{"role": "system", "content": system}, {"role": "user", "content": user}], "temperature": 0.3, "max_tokens": 800})
    content = response.json()["choices"][0]["message"]["content"]
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if match:
        return json.loads(match.group())
    return {"summary": content[:500], "error_category": "UNKNOWN"}

@router.post("/monitor/explain/{incident_id}")
async def explain_error(incident_id: str):
    client = get_client()
    if not client:
        raise HTTPException(503, "Database unavailable")
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT ERROR_MESSAGE, ERROR_CODE, WORKFLOW_NAME FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    error_msg, error_code, workflow = row
    cursor.close()
    return _call_llm_explanation(error_msg, error_code, workflow)

# ---------- Generate Fix Patch ----------
def _generate_fix_patch_llm(definition, error_msg, error_category):
    # Placeholder – replace with actual LLM call that returns structured fix plan
    return {
        "summary": f"Fix for {error_category}",
        "steps": [{"step_number": 1, "title": "Update expression", "description": "Add null check to contains()", "sub_steps": ["Change contains() to coalesce(...)"], "note": "Test after deployment"}]
    }

@router.post("/monitor/generate-fix/{incident_id}")
async def generate_fix_patch(incident_id: str):
    client = get_client()
    if not client:
        raise HTTPException(503, "Database unavailable")
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_MESSAGE, ERROR_CATEGORY FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, error_msg, error_category = row
    cursor.close()
    
    token = get_arm_token(settings.tenant_id, settings.client_id, settings.client_secret)
    wf = get_workflow(token, sub_id, settings.resource_group, workflow_name)
    definition = wf.get("properties", {}).get("definition", {})
    
    fix_patch = _generate_fix_patch_llm(definition, error_msg, error_category)
    cursor = client.conn.cursor()
    cursor.execute(f"UPDATE {client.full_table} SET AI_FIX_PATCH = ? WHERE INCIDENT_ID = ?", (json.dumps(fix_patch), incident_id))
    client.conn.commit()
    cursor.close()
    return fix_patch

# ---------- Apply Fix ----------
@router.post("/monitor/apply-fix/{incident_id}")
async def apply_message_fix(incident_id: str, req: ApplyFixRequest):
    client = get_client()
    if not client:
        raise HTTPException(503, "Database unavailable")
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_CATEGORY, ERROR_MESSAGE FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, error_category, error_msg = row
    cursor.close()
    
    result = run_remediation(
        subscription_id=sub_id,
        resource_group=settings.resource_group,
        workflow_name=workflow_name,
        run_id=incident_id,
        settings=settings,
        backup_dir=None,
        trigger_name=None,
    )
    new_status = "AUTO_FIXED" if result.get("status") == "remediated" else "FIX_FAILED"
    cursor = client.conn.cursor()
    cursor.execute(f"UPDATE {client.full_table} SET STATUS = ?, AUTO_FIX_ATTEMPTED = TRUE, AUTO_FIX_SUCCESS = ? WHERE INCIDENT_ID = ?", (new_status, new_status == "AUTO_FIXED", incident_id))
    client.conn.commit()
    cursor.close()
    return {"status": new_status, "summary": result.get("fix_applied", "")}

# ---------- Fix Status Polling ----------
@router.get("/monitor/fix-status/{incident_id}")
async def get_fix_status(incident_id: str):
    client = get_client()
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT STATUS, AUTO_FIX_SUCCESS, FIX_STRATEGY FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    status, success, fix_summary = row
    cursor.close()
    return {
        "status": status,
        "fix_summary": fix_summary,
        "current_step": "Completed" if success else "Failed",
        "step_index": 5 if success else 0,
        "total_steps": 5,
        "steps_done": ["Submit", "Get iFlow", "Validate", "Patch", "Deploy"] if success else []
    }

# ---------- Tickets (Stubs) ----------
@router.get("/tickets")
async def get_tickets():
    # TODO: Replace with real ticket logic
    return {"tickets": []}

@router.post("/tickets/{ticket_id}/update")
async def update_ticket(ticket_id: str, req: UpdateTicketRequest):
    # TODO: Implement ticket update
    return {"status": "updated"}

# ---------- Approvals (Stubs) ----------
@router.get("/approvals/pending")
async def get_pending_approvals():
    # TODO: Replace with real approval logic
    return {"pending": []}

@router.post("/approvals/{incident_id}/approve")
async def approve_incident(incident_id: str, req: ApproveIncidentRequest):
    # TODO: Implement approval processing
    return {"status": "processed"}

# ---------- Event Mesh Stubs (for compatibility) ----------
@router.get("/aem/status")
async def aem_status():
    return {"event_mesh_enabled": True, "messages_retrieved": 0, "total_incidents": 0, "queue_depth": 0}

@router.get("/aem/incidents")
async def aem_incidents(limit: int = 100):
    return {"incidents": []}

@router.get("/mcp/tools")
async def mcp_tools():
    return {"total": 0, "servers": {}}