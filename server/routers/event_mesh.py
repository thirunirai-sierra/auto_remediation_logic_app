"""
SAP Event Mesh — HTTP ingest endpoint.
Receives Logic App failure events pushed by SAP Event Mesh webhook subscription.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from config import get_settings
from db.hana_client import get_global_client
from routers.api_ingest import categorize_error
from services.remediation_tracker import get_tracker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/event-mesh", tags=["Event Mesh"])
settings = get_settings()


class EventMeshPayload(BaseModel):
    workflow_name:   Optional[str] = None
    run_id:          Optional[str] = None
    incident_id:     Optional[str] = None
    subscription_id: Optional[str] = None
    resource_group:  Optional[str] = None
    error_code:      Optional[str] = None
    error_message:   Optional[str] = None
    error_category:  Optional[str] = None
    timestamp:       Optional[str] = None

    class Config:
        extra = "allow"


def _store(event: Dict[str, Any]) -> bool:
    run_id        = event.get("run_id") or event.get("incident_id")
    workflow_name = event.get("workflow_name")
    if not run_id or not workflow_name:
        logger.warning("Event Mesh event missing run_id/workflow_name — skipped: %s", event)
        return False

    hana    = get_global_client()
    tracker = get_tracker()
    if not hana or not hana._ensure_connected():
        logger.error("HANA not available — cannot store Event Mesh event")
        return False

    error_code     = str(event.get("error_code") or "unknown")
    error_message  = str(event.get("error_message") or "")[:2000]
    error_category = event.get("error_category") or categorize_error(error_message, error_code)
    subscription_id = event.get("subscription_id") or settings.AZURE_SUBSCRIPTION_ID or ""
    resource_group  = event.get("resource_group")  or settings.AZURE_RESOURCE_GROUP  or ""
    timestamp       = event.get("timestamp") or datetime.now(timezone.utc).isoformat()

    rec_meta = tracker.get_run_record(run_id) if run_id else None
    auto_att = rec_meta.auto_fix_attempted if rec_meta else False
    auto_suc = rec_meta.auto_fix_success   if rec_meta else False
    retries  = rec_meta.retry_count        if rec_meta else 0
    status   = "Fix Succeeded" if auto_suc else ("Fix Attempted" if auto_att else "Ticket Created")

    record = {
        "incident_id":        run_id,
        "subscription_id":    subscription_id,
        "resource_group":     resource_group,
        "workflow_name":      workflow_name,
        "error_code":         error_code,
        "error_message":      error_message,
        "error_category":     error_category,
        "status":             status,
        "rca_root_cause":     None,
        "fix_strategy":       None,
        "created_at":         timestamp,
        "updated_at":         datetime.utcnow().isoformat(),
        "auto_fix_attempted": auto_att,
        "auto_fix_success":   auto_suc,
        "retry_count":        retries,
    }

    try:
        inserted, failed = hana.batch_upsert_observability([record])
        if failed:
            logger.error("HANA upsert failed for Event Mesh run_id=%s", run_id)
            return False
        logger.info("Event Mesh → HANA: run_id=%s workflow=%s category=%s", run_id, workflow_name, error_category)
        return True
    except Exception as exc:
        logger.exception("Event Mesh store failed for run_id=%s: %s", run_id, exc)
        return False


@router.get("/webhook")
async def event_mesh_handshake():
    """SAP Event Mesh webhook handshake — called by EM to verify the endpoint."""
    return {"handshake": True}


@router.post("/webhook")
async def event_mesh_webhook(
    request: Request,
    x_em_topic: Optional[str] = Header(None, alias="x-em-topic"),
    x_em_queue: Optional[str] = Header(None, alias="x-em-queue"),
):
    """Receive Logic App failure events pushed by SAP Event Mesh."""
    try:
        raw  = await request.body()
        body = json.loads(raw) if raw else {}
    except Exception as exc:
        raise HTTPException(400, f"Invalid JSON body: {exc}")

    topic  = x_em_topic or x_em_queue or "unknown"
    logger.info("Event Mesh webhook received from topic=%s", topic)

    events = body if isinstance(body, list) else [body]
    stored = sum(1 for e in events if _store(e))

    return {
        "status":    "ok",
        "received":  len(events),
        "stored":    stored,
        "topic":     topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/ingest")
async def event_mesh_ingest(payload: EventMeshPayload):
    """Typed ingest for producers that POST structured JSON directly."""
    event = payload.model_dump(exclude_none=False)
    if not event.get("run_id") and event.get("incident_id"):
        event["run_id"] = event["incident_id"]
    if not event.get("run_id") or not event.get("workflow_name"):
        raise HTTPException(422, "run_id (or incident_id) and workflow_name are required")
    if not _store(event):
        raise HTTPException(500, "Failed to store event in HANA")
    return {
        "status":       "stored",
        "run_id":       event["run_id"],
        "workflow_name": event["workflow_name"],
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


@router.get("/status")
async def event_mesh_status():
    """Live status: HANA connectivity + incident counts."""
    hana      = get_global_client()
    connected = hana is not None and hana._ensure_connected()
    total, recent = 0, 0
    if connected:
        try:
            cursor = hana.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {hana.full_table}")
            total = cursor.fetchone()[0]
            cursor.execute(
                f"SELECT COUNT(*) FROM {hana.full_table} WHERE CREATED_AT >= ADD_SECONDS(NOW(), -3600)"
            )
            recent = cursor.fetchone()[0]
            cursor.close()
        except Exception as exc:
            logger.warning("event_mesh_status count failed: %s", exc)
    return {
        "event_mesh_enabled": True,
        "hana_connected":     connected,
        "total_incidents":    total,
        "messages_retrieved": recent,
        "queue_depth":        0,
        "webhook_active":     True,
        "webhook_url":        "/api/event-mesh/webhook",
        "ingest_url":         "/api/event-mesh/ingest",
    }
