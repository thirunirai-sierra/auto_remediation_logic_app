"""
Observability endpoints for incident tracking and AI‑assisted analysis.
Uses SAP AI Core LLM and HANA knowledge base for explanations and fix generation.
"""

import json,logging,asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from services.event_mesh.pipeline import start_pipeline
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
    comment: Optional[str] = None


class RejectIncidentRequest(BaseModel):
    comment: Optional[str] = None
    reason: Optional[str] = None

_PIPELINE_IN_FLIGHT_STATUSES = (
    "PIPELINE_IN_PROGRESS", "PIPELINE_STARTED", "PIPELINE_OBSERVER",
    "PIPELINE_CLASSIFIER", "PIPELINE_RCA", "PIPELINE_FIXER", "PIPELINE_VERIFIER",
    "FIX_IN_PROGRESS",
)
# Broad set (pipeline guards, etc.)
_IN_PROGRESS_STATUSES = _PIPELINE_IN_FLIGHT_STATUSES + (
    "RCA_IN_PROGRESS", "FIX_ATTEMPTED", "CLASSIFIED", "RCA_COMPLETE",
    "FIX_APPLIED_PENDING_VERIFICATION", "PROCESSING", "ANALYZING", "APPROVED",
)
# KPI/list: only statuses that mean a fix is actively running, updated recently.
_ACTIVE_PROCESSING_STATUSES = _PIPELINE_IN_FLIGHT_STATUSES + (
    "RCA_IN_PROGRESS", "FIX_APPLIED_PENDING_VERIFICATION", "ANALYZING", "APPROVED",
)
_PROCESSING_STALE_MINUTES = 30
_FAILED_STATUSES = frozenset({
    "FAILED", "FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME",
    "RCA_FAILED", "PIPELINE_ERROR", "DETECTED", "ARTIFACT_MISSING", "REJECTED",
})
_SUCCESS_STATUSES = frozenset({
    "AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED", "SUCCESS",
    "HUMAN_INITIATED_FIX", "FIX_DEPLOYED",
})
_RETRY_STATUSES = frozenset({
    "RETRY", "PENDING_APPROVAL", "TICKET_CREATED", "AWAITING_APPROVAL",
})


def _normalize_status_key(status: Optional[str]) -> str:
    return (status or "").strip().upper().replace(" ", "_")


def _is_processing_status_key(status_key: str) -> bool:
    return status_key in _IN_PROGRESS_STATUSES or (
        status_key.startswith("PIPELINE_") and status_key != "PIPELINE_ERROR"
    )


def _status_in_set_sql(statuses: tuple) -> str:
    placeholders = ", ".join("?" for _ in statuses)
    return f"REPLACE(UPPER(TRIM(STATUS)), ' ', '_') IN ({placeholders})"


def _active_processing_params() -> tuple[list, int]:
    return list(_ACTIVE_PROCESSING_STATUSES), -_PROCESSING_STALE_MINUTES * 60


def _active_processing_predicate() -> str:
    """SQL fragment: in-flight status AND updated within the stale window."""
    return (
        f"(({_status_in_set_sql(_ACTIVE_PROCESSING_STATUSES)} OR "
        f"(REPLACE(UPPER(TRIM(STATUS)), ' ', '_') LIKE 'PIPELINE_%' "
        f"AND REPLACE(UPPER(TRIM(STATUS)), ' ', '_') <> 'PIPELINE_ERROR')) "
        f"AND UPDATED_AT >= ADD_SECONDS(CURRENT_TIMESTAMP, ?))"
    )


def _count_active_processing(client) -> int:
    statuses, stale_seconds = _active_processing_params()
    cursor = client.conn.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM {client.full_table} WHERE {_active_processing_predicate()}",
        (*statuses, stale_seconds),
    )
    row = cursor.fetchone()
    cursor.close()
    return int(row[0] or 0) if row else 0


def _build_status_group_filter(status_group: str) -> tuple[str, list]:
    """Map summary-bucket names (FAILED, PROCESSING, …) to a SQL predicate."""
    groups = [g.strip().upper() for g in status_group.split(",") if g.strip()]
    if not groups:
        return "", []

    parts: list[str] = []
    params: list = []

    for group in groups:
        if group == "PROCESSING":
            parts.append(_active_processing_predicate())
            statuses, stale_seconds = _active_processing_params()
            params.extend(statuses)
            params.append(stale_seconds)
        elif group == "FAILED":
            parts.append(f"({_status_in_set_sql(tuple(_FAILED_STATUSES))})")
            params.extend(_FAILED_STATUSES)
        elif group == "SUCCESS":
            parts.append(f"({_status_in_set_sql(tuple(_SUCCESS_STATUSES))})")
            params.extend(_SUCCESS_STATUSES)
        elif group == "RETRY":
            parts.append(f"({_status_in_set_sql(tuple(_RETRY_STATUSES))})")
            params.extend(_RETRY_STATUSES)
        else:
            parts.append("REPLACE(UPPER(TRIM(STATUS)), ' ', '_') = ?")
            params.append(group)

    return f"({' OR '.join(parts)})", params


def _format_root_cause_for_display(rec: dict) -> Optional[str]:
    """Best available RCA text for list/trace views."""
    for key in ("RCA_ROOT_CAUSE", "AI_DIAGNOSIS"):
        val = rec.get(key)
        if val and str(val).strip():
            return str(val).strip()
    err = rec.get("ERROR_MESSAGE")
    if err and str(err).strip():
        return str(err).strip()[:2000]
    return None


def _compute_status_summary(client) -> dict:
    """Aggregate incident counts into Observability/Dashboard KPI buckets."""
    cursor = client.conn.cursor()
    cursor.execute(f"SELECT STATUS, COUNT(*) FROM {client.full_table} GROUP BY STATUS")
    rows = cursor.fetchall()
    cursor.close()

    buckets = {"FAILED": 0, "SUCCESS": 0, "PROCESSING": 0, "RETRY": 0, "pending_approval": 0}
    for status, count in rows:
        st = _normalize_status_key(status)
        n = int(count or 0)
        if st in _FAILED_STATUSES:
            buckets["FAILED"] += n
        elif st in _SUCCESS_STATUSES:
            buckets["SUCCESS"] += n
        elif st in _RETRY_STATUSES:
            buckets["RETRY"] += n
        if st == "AWAITING_APPROVAL":
            buckets["pending_approval"] += n
    buckets["PROCESSING"] = _count_active_processing(client)
    return buckets


def get_hana_client():
    """Return the singleton HANA client."""
    return get_global_client()

# Incident listing and detail

@router.get("/monitor/messages")
async def get_monitor_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    status_group: Optional[str] = Query(
        None,
        description="Comma-separated summary buckets: FAILED, SUCCESS, PROCESSING, RETRY",
    ),
    search: Optional[str] = Query(None),
):
    """
    List incidents with pagination and filtering.

    Args:
        limit: Max number of records to return.
        offset: Number of records to skip.
        status: Filter by exact status (case‑insensitive).
        status_group: Filter by KPI bucket(s) using the same rules as summary cards.
        search: Search workflow name, incident id, or error message.

    Returns:
        dict: Contains 'messages' list, 'total' count, and 'summary' KPI buckets.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    cursor = client.conn.cursor()
    conditions = []
    params = []
    if status_group:
        group_clause, group_params = _build_status_group_filter(status_group)
        if group_clause:
            conditions.append(group_clause)
            params.extend(group_params)
    elif status and status.upper() != "ALL":
        conditions.append("REPLACE(UPPER(TRIM(STATUS)), ' ', '_') = ?")
        params.append(_normalize_status_key(status))
    if search:
        conditions.append("(WORKFLOW_NAME LIKE ? OR ERROR_MESSAGE LIKE ? OR INCIDENT_ID LIKE ?)")
        params.append(f"%{search}%")
        params.append(f"%{search}%")
        params.append(f"%{search}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    count_sql = f"SELECT COUNT(*) FROM {client.full_table} {where}"
    cursor.execute(count_sql, params)
    total = cursor.fetchone()[0]

    data_sql = f"""
        SELECT INCIDENT_ID, WORKFLOW_NAME, STATUS, ERROR_CATEGORY, CREATED_AT, UPDATED_AT,
               ERROR_MESSAGE, RCA_ROOT_CAUSE, AI_DIAGNOSIS
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
            "root_cause": _format_root_cause_for_display(d),
        })
    summary = _compute_status_summary(client)
    cursor.close()
    return {"messages": messages, "total": total, "summary": summary}


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
    _cur = client.conn.cursor()
    _cur.execute(
        f"UPDATE {client.full_table} SET STATUS = 'ANALYZING' WHERE INCIDENT_ID = ?",
        (incident_id,)
    )
    client.conn.commit()
    _cur.close()
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

    # Use the HANA client's upsert method to safely update the record
    try:
        client.upsert_observability_record({
        "incident_id": incident_id,
        "ai_diagnosis": diagnosis,
        "ai_proposed_fix": proposed_fix,
        "ai_confidence": float(confidence),
        "rca_root_cause": diagnosis,
        "status": "ANALYZED",          # ← explicit; never falls through to default
    })
    except Exception as e:
        logger.error("Failed to update incident %s: %s", incident_id, e)
        raise HTTPException(500, f"Database update failed: {e}")

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
        f"SELECT WORKFLOW_NAME, SUBSCRIPTION_ID, ERROR_MESSAGE, ERROR_CATEGORY, STATUS "
        f"FROM {client.full_table} WHERE INCIDENT_ID = ?",
        (incident_id,),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Incident not found")
    workflow_name, sub_id, error_msg, error_category, previous_status = row
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
        (json.dumps(fix_patch), incident_id),
    )
    client.conn.commit()
    cursor.close()

    status_update = _update_status_after_fix_generation(
        client,
        incident_id,
        previous_status or "",
        error_category or "",
    )

    return {
        **fix_patch,
        "incident_status": status_update["status"],
        "status_changed": status_update["status_changed"],
        "policy": status_update["policy"],
        "previous_status": status_update["previous_status"],
        "requires_approval": status_update["status"] == "AWAITING_APPROVAL",
    }


@router.post("/monitor/apply-fix/{incident_id}")
async def apply_message_fix(incident_id: str, req: ApplyFixRequest):
    """
    Start manual remediation via the Event Mesh agent pipeline (async).

    Publishes to the observer queue → classifier → rca → fixer → verifier.
    Returns PIPELINE_STARTED immediately; poll GET /monitor/fix-status/{incident_id}
    for the final outcome.

    Args:
        incident_id: Incident identifier (ORBLOGICAPPS-…).
        req: Request body (trigger_type, proposed_fix, force).

    Returns:
        dict: PIPELINE_STARTED with correlation_id, or error status.
    """
    from routers.settings import get_error_policy
    from services.agents.observer import Observer
    from services.event_mesh.pipeline import start_pipeline
    import json as _json
    import asyncio as _asyncio

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
    workflow_name, sub_id, error_category, _error_msg, resource_group_db, azure_run_id = row
    cursor.close()

    if not azure_run_id or str(azure_run_id).startswith("ORBLOGICAPPS-"):
        return {
            "status": "FIX_FAILED",
            "summary": (
                "Cannot start pipeline: incident has no valid Azure RUN_ID. "
                "Re-ingest the failure from Log Analytics so RUN_ID is populated."
            ),
        }

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
            pending_since = datetime.now(timezone.utc).isoformat()
            cursor = client.conn.cursor()
            cursor.execute(
                f"UPDATE {client.full_table} SET STATUS = 'AWAITING_APPROVAL', PENDING_SINCE = ? WHERE INCIDENT_ID = ?",
                (pending_since, incident_id),
            )
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
        pending_since = datetime.now(timezone.utc).isoformat()
        cursor = client.conn.cursor()
        cursor.execute(
            f"UPDATE {client.full_table} SET STATUS = 'AWAITING_APPROVAL', AUTO_FIX_ATTEMPTED = TRUE, PENDING_SINCE = ? "
            f"WHERE INCIDENT_ID = ?",
            (pending_since, incident_id),
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

    # Optional pre-check: Observer resolves failed action for logging (pipeline re-runs Observer)
    if not failed_action_name:
        logger.info("Failed action name missing — calling Observer for run_id=%s", azure_run_id)
        try:
            observer = Observer(settings)
            obs_result = await _asyncio.to_thread(
                observer.analyze_failed_run,
                effective_sub_id,
                effective_rg,
                workflow_name,
                azure_run_id,
            )
            if obs_result.get("status") == "failed_action_found":
                failed_action_name = obs_result.get("failed_action_name")
                logger.info("Observer resolved failed_action_name=%s", failed_action_name)
            else:
                logger.warning(
                    "Observer could not determine failed action for run_id=%s — pipeline will retry",
                    azure_run_id,
                )
        except Exception as e:
            logger.error("Observer call failed for incident %s: %s", incident_id, e)

    if not failed_action_name:
        logger.warning(
            "Could not resolve failed_action_name for incident %s — starting pipeline anyway",
            incident_id,
        )

    # Start Event Mesh pipeline (observer → classifier → rca → fixer → verifier)
    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"UPDATE {client.full_table} SET STATUS = 'PIPELINE_STARTED', AUTO_FIX_ATTEMPTED = TRUE "
            f"WHERE INCIDENT_ID = ?",
            (incident_id,),
        )
        client.conn.commit()
        cursor.close()

        pipeline_result = await start_pipeline(
            workflow_name=workflow_name,
            run_id=azure_run_id,
            subscription_id=effective_sub_id,
            resource_group=effective_rg,
            source="manual_fix",
            incident_id=incident_id,
        )
        logger.info("Pipeline started for incident %s: %s", incident_id, pipeline_result)

        if not pipeline_result.get("started"):
            reason = pipeline_result.get("reason", "Event Mesh pipeline disabled or failed to publish")
            cursor = client.conn.cursor()
            cursor.execute(
                f"UPDATE {client.full_table} SET STATUS = 'FIX_FAILED', AUTO_FIX_SUCCESS = FALSE "
                f"WHERE INCIDENT_ID = ?",
                (incident_id,),
            )
            client.conn.commit()
            cursor.close()
            return {"status": "FIX_FAILED", "summary": reason, "pipeline": pipeline_result}

        return {
            "status": "PIPELINE_STARTED",
            "incident_id": incident_id,
            "summary": (
                f"Manual fix initiated via Event Mesh pipeline. "
                f"Correlation ID: {pipeline_result.get('correlation_id')}. "
                f"Poll fix-status for the final outcome."
            ),
            "failed_action_name": failed_action_name,
            "azure_run_id": azure_run_id,
            "pipeline": pipeline_result,
        }
    except Exception as exc:
        logger.error("Failed to start pipeline for %s: %s", incident_id, exc, exc_info=True)
        try:
            cursor = client.conn.cursor()
            cursor.execute(
                f"UPDATE {client.full_table} SET STATUS = 'FIX_FAILED' WHERE INCIDENT_ID = ?",
                (incident_id,),
            )
            client.conn.commit()
            cursor.close()
        except Exception:
            pass
        return {
            "status": "FIX_FAILED",
            "summary": f"Could not start Event Mesh pipeline: {exc}",
        }


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

    status_upper = (status or "").upper()
    pipeline_progress = {
        "PIPELINE_STARTED": (1, "Get Workflow", ["Submit"]),
        "PIPELINE_OBSERVER": (1, "Get Workflow", ["Submit"]),
        "PIPELINE_CLASSIFIER": (2, "Validate", ["Submit", "Get Workflow"]),
        "PIPELINE_RCA": (3, "Validate", ["Submit", "Get Workflow", "Validate"]),
        "PIPELINE_FIXER": (4, "Patch", ["Submit", "Get Workflow", "Validate", "Patch"]),
        "PIPELINE_IN_PROGRESS": (2, "Validate", ["Submit", "Get Workflow"]),
        "FIX_IN_PROGRESS": (2, "Get Workflow", ["Submit"]),
    }
    if status_upper in pipeline_progress:
        step_index, current_step, steps_done = pipeline_progress[status_upper]
        return {
            "status": status,
            "fix_summary": fix_summary or f"Event Mesh pipeline — {current_step}",
            "current_step": current_step,
            "step_index": step_index,
            "total_steps": 5,
            "steps_done": steps_done,
            "pipeline_running": True,
        }

    steps_done = ["Submit", "Get Workflow", "Validate", "Patch", "Deploy"] if success else []
    terminal = status_upper in ("AUTO_FIXED", "REMEDIATED", "FIX_SUCCEEDED", "FIX_VERIFIED")
    return {
        "status": status,
        "fix_summary": fix_summary,
        "current_step": "Completed" if terminal else ("Failed" if status_upper == "FIX_FAILED" else status),
        "step_index": 5 if terminal else 0,
        "total_steps": 5,
        "steps_done": steps_done,
        "pipeline_running": False,
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
        SELECT INCIDENT_ID, RUN_ID, SUBSCRIPTION_ID, WORKFLOW_NAME, ERROR_CODE, ERROR_MESSAGE,
               STATUS, CREATED_AT, UPDATED_AT, LAST_SEEN, OCCURRENCE_COUNT,
               AI_CONFIDENCE, RCA_CONFIDENCE
        FROM {client.full_table}
        ORDER BY CREATED_AT DESC
        LIMIT 500
    """)
    rows = cursor.fetchall()
    incidents = []
    for row in rows:
        ai_conf, rca_conf = row[11], row[12]
        confidence = ai_conf if ai_conf is not None else rca_conf
        last_seen = row[9] or row[8] or row[7]
        incidents.append({
            "incidentId": row[0],
            "runId": row[1],
            "subscriptionId": row[2],
            "integrationScenario": row[3],
            "errorType": row[4],
            "errorMessage": row[5],
            "status": row[6],
            "time": row[7].isoformat() if row[7] else None,
            "lastSeen": last_seen.isoformat() if last_seen else None,
            "occurrenceCount": int(row[10] or 1),
            "rcaConfidence": float(confidence) if confidence is not None else None,
        })
    cursor.close()
    return incidents


# Approvals (human sign-off for AI-proposed fixes)
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_post_fix_status(error_category: str) -> tuple[Optional[str], str]:
    """
    Decide incident STATUS after fix plan generation based on remediation policy
    and global auto-fix setting.

    Returns:
        (new_status or None, policy_decision_label)
        None means keep the current status (typically ANALYZED).
    """
    from routers.settings import get_error_policy, get_value

    error_type = (error_category or "UNKNOWN_ERROR").strip().upper()
    policy = get_error_policy(error_type)
    auto_fix_enabled = bool(get_value("AUTO_FIX_ENABLED"))

    if not auto_fix_enabled:
        return "AWAITING_APPROVAL", "AWAITING_APPROVAL (AUTO_FIX_ENABLED=false)"

    if policy == "AWAITING_APPROVAL":
        return "AWAITING_APPROVAL", policy
    if policy == "TICKET_CREATED":
        return "TICKET_CREATED", policy

    return None, policy


def _update_status_after_fix_generation(
    client,
    incident_id: str,
    previous_status: str,
    error_category: str,
) -> dict:
    """Persist post-generation status transition when policy requires human action."""
    prev = (previous_status or "").strip().upper() or "UNKNOWN"
    new_status, policy_decision = _resolve_post_fix_status(error_category)

    if not new_status:
        logger.info(
            "generate-fix: incident=%s previous_status=%s new_status=%s policy=%s (unchanged)",
            incident_id,
            prev,
            prev,
            policy_decision,
        )
        return {
            "incident_id": incident_id,
            "previous_status": prev,
            "status": prev,
            "policy": policy_decision,
            "status_changed": False,
        }

    if prev == new_status:
        logger.info(
            "generate-fix: incident=%s previous_status=%s new_status=%s policy=%s (already set)",
            incident_id,
            prev,
            new_status,
            policy_decision,
        )
        return {
            "incident_id": incident_id,
            "previous_status": prev,
            "status": new_status,
            "policy": policy_decision,
            "status_changed": False,
        }

    pending_since = _utc_now_iso() if new_status == "AWAITING_APPROVAL" else None
    cursor = client.conn.cursor()
    if pending_since:
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET STATUS = ?, PENDING_SINCE = ?, UPDATED_AT = CURRENT_TIMESTAMP
                WHERE INCIDENT_ID = ?""",
            (new_status, pending_since, incident_id),
        )
    else:
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET STATUS = ?, UPDATED_AT = CURRENT_TIMESTAMP
                WHERE INCIDENT_ID = ?""",
            (new_status, incident_id),
        )
    client.conn.commit()
    cursor.close()

    logger.info(
        "generate-fix: incident=%s previous_status=%s new_status=%s policy=%s",
        incident_id,
        prev,
        new_status,
        policy_decision,
    )
    return {
        "incident_id": incident_id,
        "previous_status": prev,
        "status": new_status,
        "policy": policy_decision,
        "status_changed": True,
        "pending_since": pending_since,
    }


def _format_ts(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _fetch_approval_row(client, incident_id: str) -> Optional[dict]:
    cursor = client.conn.cursor()
    cursor.execute(
        f"""SELECT INCIDENT_ID, WORKFLOW_NAME, IFLOW_ID, ERROR_CATEGORY, ERROR_TYPE,
                   ERROR_MESSAGE, RCA_ROOT_CAUSE, ROOT_CAUSE, AI_PROPOSED_FIX, PROPOSED_FIX,
                   AI_CONFIDENCE, RCA_CONFIDENCE, STATUS, CREATED_AT, PENDING_SINCE,
                   UPDATED_AT, MESSAGE_GUID
            FROM {client.full_table}
            WHERE INCIDENT_ID = ?""",
        (incident_id,),
    )
    row = cursor.fetchone()
    cols = [desc[0] for desc in cursor.description] if row else []
    cursor.close()
    if not row:
        return None
    return dict(zip(cols, row))


def _approval_row_to_payload(rec: dict) -> dict:
    confidence = rec.get("AI_CONFIDENCE")
    if confidence is None:
        confidence = rec.get("RCA_CONFIDENCE")
    try:
        confidence = float(confidence or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0

    root_cause = rec.get("RCA_ROOT_CAUSE") or rec.get("ROOT_CAUSE") or ""
    proposed_fix = rec.get("AI_PROPOSED_FIX") or rec.get("PROPOSED_FIX") or ""
    error_type = rec.get("ERROR_CATEGORY") or rec.get("ERROR_TYPE") or "UNKNOWN_ERROR"
    iflow_id = rec.get("WORKFLOW_NAME") or rec.get("IFLOW_ID") or "Unknown"
    pending_since = (
        _format_ts(rec.get("PENDING_SINCE"))
        or _format_ts(rec.get("UPDATED_AT"))
        or _format_ts(rec.get("CREATED_AT"))
    )

    return {
        "incident_id": rec["INCIDENT_ID"],
        "iflow_id": iflow_id,
        "error_type": error_type,
        "error_message": rec.get("ERROR_MESSAGE") or "",
        "root_cause": root_cause,
        "proposed_fix": proposed_fix,
        "rca_confidence": confidence,
        "status": rec.get("STATUS") or "AWAITING_APPROVAL",
        "created_at": _format_ts(rec.get("CREATED_AT")),
        "pending_since": pending_since,
        "message_guid": rec.get("MESSAGE_GUID") or rec["INCIDENT_ID"],
    }


def _require_awaiting_approval(client, incident_id: str) -> dict:
    rec = _fetch_approval_row(client, incident_id)
    if not rec:
        raise HTTPException(404, f"Incident {incident_id} not found")
    status = (rec.get("STATUS") or "").upper()
    if status != "AWAITING_APPROVAL":
        raise HTTPException(
            409,
            f"Incident {incident_id} is not awaiting approval (current status: {status or 'unknown'})",
        )
    return rec


@router.get("/approvals/pending")
async def get_pending_approvals():
    """Return all incidents awaiting human approval, newest first."""
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"""SELECT INCIDENT_ID, WORKFLOW_NAME, IFLOW_ID, ERROR_CATEGORY, ERROR_TYPE,
                       ERROR_MESSAGE, RCA_ROOT_CAUSE, ROOT_CAUSE, AI_PROPOSED_FIX, PROPOSED_FIX,
                       AI_CONFIDENCE, RCA_CONFIDENCE, STATUS, CREATED_AT, PENDING_SINCE,
                       UPDATED_AT, MESSAGE_GUID
                FROM {client.full_table}
                WHERE UPPER(STATUS) = 'AWAITING_APPROVAL'
                ORDER BY CREATED_AT DESC""",
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        cursor.close()
        pending = [_approval_row_to_payload(dict(zip(cols, row))) for row in rows]
        logger.info("Fetched %d pending approval(s)", len(pending))
        return {"pending": pending}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("get_pending_approvals failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Failed to fetch pending approvals") from exc


@router.post("/approvals/{incident_id}/approve")
async def approve_incident_endpoint(incident_id: str, req: ApproveIncidentRequest):
    """Approve a pending fix, record timestamp, and trigger the fix pipeline."""
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    _require_awaiting_approval(client, incident_id)
    approved_at = _utc_now_iso()
    comment = (req.comment or "Approved via API").strip()

    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET STATUS = 'APPROVED', APPROVED_AT = ?, COMMENT = ?, UPDATED_AT = CURRENT_TIMESTAMP
                WHERE INCIDENT_ID = ? AND UPPER(STATUS) = 'AWAITING_APPROVAL'""",
            (approved_at, comment, incident_id),
        )
        if cursor.rowcount == 0:
            cursor.close()
            raise HTTPException(409, f"Incident {incident_id} is no longer awaiting approval")
        client.conn.commit()
        cursor.close()
        logger.info("Incident %s approved at %s", incident_id, approved_at)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to record approval for %s: %s", incident_id, exc, exc_info=True)
        raise HTTPException(500, "Failed to record approval") from exc

    fix_result: dict = {"status": "APPROVED", "summary": "Approval recorded."}
    try:
        fix_result = await apply_message_fix(
            incident_id,
            ApplyFixRequest(trigger_type="approval", force=True, proposed_fix=None),
        )
        logger.info(
            "Fix pipeline triggered after approval for %s: %s",
            incident_id,
            fix_result.get("status"),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fix pipeline failed after approval for %s: %s", incident_id, exc, exc_info=True)
        fix_result = {
            "status": "APPROVED",
            "summary": f"Approved but fix pipeline failed to start: {exc}",
        }

    return {
        "status": "approved",
        "incident_id": incident_id,
        "approved_at": approved_at,
        "comment": comment,
        "fix": fix_result,
    }


@router.post("/approvals/{incident_id}/reject")
async def reject_incident_endpoint(incident_id: str, req: RejectIncidentRequest):
    """Reject a pending fix and record rejection timestamp and reason."""
    client = get_hana_client()
    if not client or not client._ensure_connected():
        raise HTTPException(503, "HANA not available")

    _require_awaiting_approval(client, incident_id)
    rejected_at = _utc_now_iso()
    reason = (req.reason or req.comment or "Rejected via API").strip()

    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET STATUS = 'REJECTED', REJECTED_AT = ?, COMMENT = ?, UPDATED_AT = CURRENT_TIMESTAMP
                WHERE INCIDENT_ID = ? AND UPPER(STATUS) = 'AWAITING_APPROVAL'""",
            (rejected_at, reason, incident_id),
        )
        if cursor.rowcount == 0:
            cursor.close()
            raise HTTPException(409, f"Incident {incident_id} is no longer awaiting approval")
        client.conn.commit()
        cursor.close()
        logger.info("Incident %s rejected at %s: %s", incident_id, rejected_at, reason)
        return {
            "status": "rejected",
            "incident_id": incident_id,
            "rejected_at": rejected_at,
            "reason": reason,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to reject incident %s: %s", incident_id, exc, exc_info=True)
        raise HTTPException(500, "Failed to reject incident") from exc


# Stubs for tickets (kept for compatibility)
@router.get("/tickets")
async def get_tickets():
    return {"tickets": []}


@router.post("/tickets/{ticket_id}/update")
async def update_ticket(_ticket_id: str, _req: UpdateTicketRequest):
    return {"status": "updated"}


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