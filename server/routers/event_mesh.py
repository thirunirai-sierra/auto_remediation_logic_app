"""
SAP Event Mesh — failure ingest, five agent queues, and pipeline webhooks.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from config import get_settings
from db.hana_client import get_global_client
from routers.api_ingest import categorize_error
from services.event_mesh.bus import get_bus
from services.event_mesh.messages import PipelineEnvelope
from services.event_mesh.pipeline import start_pipeline
from services.event_mesh.queues import (
    AGENT_NAMES,
    get_agent_for_queue,
    get_queue_for_agent,
    queue_definitions,
)
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


@router.options("/webhook")
async def event_mesh_webhook_options(
    request: Request,
    webhook_request_origin: Optional[str] = Header(None, alias="WebHook-Request-Origin"),
):
    """
    Respond to the Event Mesh handshake OPTIONS request.
    """
    response_headers = {
        "WebHook-Allowed-Origin": webhook_request_origin or "*",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, WebHook-Request-Origin",
    }
    return Response(status_code=200, headers=response_headers)


def _is_pipeline_envelope(event: Dict[str, Any]) -> bool:
    """Agent pipeline message (vs raw failure ingest)."""
    return bool(
        event.get("workflow_name")
        and event.get("run_id")
        and (event.get("correlation_id") or event.get("current_agent"))
    )


@router.post("/webhook")
async def event_mesh_webhook(
    request: Request,
    x_em_topic: Optional[str] = Header(None, alias="x-em-topic"),
    x_em_queue: Optional[str] = Header(None, alias="x-em-queue"),
):
    """
  Receive events from SAP Event Mesh.
  - Agent pipeline messages (correlation_id + run_id) → routed to one of 5 agent queues.
  - Failure ingest payloads → stored in HANA.
    """
    try:
        raw = await request.body()
        body = json.loads(raw) if raw else {}
    except Exception as exc:
        raise HTTPException(400, f"Invalid JSON body: {exc}")

    queue_name = x_em_queue or x_em_topic or "unknown"
    logger.info("Event Mesh webhook queue/topic=%s", queue_name)

    events: List[Dict[str, Any]] = body if isinstance(body, list) else [body]
    bus = get_bus()
    pipeline_accepted = 0
    stored = 0

    for event in events:
        if _is_pipeline_envelope(event):
            agent = get_agent_for_queue(queue_name) or event.get("current_agent")
            if not agent or agent not in AGENT_NAMES:
                agent = event.get("current_agent") or "observer"
            await bus.deliver_from_webhook(get_queue_for_agent(agent) if agent in AGENT_NAMES else queue_name, event)
            pipeline_accepted += 1
        elif _store(event):
            stored += 1

    return {
        "status": "ok",
        "received": len(events),
        "pipeline_enqueued": pipeline_accepted,
        "stored": stored,
        "queue": queue_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


class PipelineStartRequest(BaseModel):
    workflow_name: str
    run_id: str
    subscription_id: str
    resource_group: str


@router.get("/queues")
async def list_agent_queues():
    """Five Event Mesh queues — one per agent."""
    bus = get_bus()
    return {
        "count": len(AGENT_NAMES),
        "pipeline_enabled": getattr(settings, "EVENT_MESH_PIPELINE_ENABLED", True),
        "queues": queue_definitions(),
        "local_depths": bus.queue_depths(),
    }


@router.post("/pipeline/start")
async def pipeline_start(req: PipelineStartRequest):
    """Start remediation: publish to observer queue → classifier → rca → fixer → verifier."""
    return await start_pipeline(
        req.workflow_name,
        req.run_id,
        req.subscription_id,
        req.resource_group,
        source="api",
    )


@router.post("/consume/{agent}")
async def consume_agent_queue(agent: str, request: Request):
    """
    SAP Event Mesh queue subscription target (per agent).
    Same as webhook but agent is explicit in the URL path.
    """
    if agent not in AGENT_NAMES:
        raise HTTPException(404, f"Unknown agent. Use one of: {AGENT_NAMES}")
    try:
        raw = await request.body()
        body = json.loads(raw) if raw else {}
    except Exception as exc:
        raise HTTPException(400, f"Invalid JSON: {exc}")

    events = body if isinstance(body, list) else [body]
    bus = get_bus()
    accepted = 0
    for event in events:
        await bus.deliver_from_webhook(get_queue_for_agent(agent), event)
        accepted += 1
    return {"accepted": accepted, "agent": agent, "queue": get_queue_for_agent(agent)}


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
    bus = get_bus()
    depths = bus.queue_depths()
    return {
        "event_mesh_enabled": True,
        "pipeline_enabled": getattr(settings, "EVENT_MESH_PIPELINE_ENABLED", True),
        "hana_connected": connected,
        "total_incidents": total,
        "messages_retrieved": recent,
        "queue_depths": depths,
        "queue_depth": sum(depths.values()),
        "webhook_active": True,
        "webhook_url": "/api/event-mesh/webhook",
        "ingest_url": "/api/event-mesh/ingest",
        "pipeline_start": "/api/event-mesh/pipeline/start",
        "queues_url": "/api/event-mesh/queues",
    }