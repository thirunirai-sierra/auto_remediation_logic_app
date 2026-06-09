"""
Orchestrate multi-agent flow via Event Mesh queues + REST APIs.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from config import get_settings
from db.hana_client import get_global_client
from services.event_mesh.bus import get_bus
from services.event_mesh.messages import PipelineEnvelope
from services.event_mesh.pipeline_runner import run_agent_step
from services.event_mesh.queues import get_next_agent, get_queue_for_agent
from services.itsm_service import ensure_ticket_for_incident

logger = logging.getLogger(__name__)


async def start_pipeline(
    workflow_name: str,
    run_id: str,
    subscription_id: str,
    resource_group: str,
    source: str = "orchestrator",
    incident_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Publish first message to observer queue."""
    settings = get_settings()
    if not getattr(settings, "EVENT_MESH_PIPELINE_ENABLED", True):
        return {"started": False, "reason": "EVENT_MESH_PIPELINE_ENABLED is false"}

    envelope = PipelineEnvelope(
        workflow_name=workflow_name,
        run_id=run_id,
        subscription_id=subscription_id,
        resource_group=resource_group,
        incident_id=incident_id,
        source=source,
        current_agent="observer",
    )
    bus = get_bus()
    pub = await bus.publish("observer", envelope)
    return {
        "started": True,
        "mode": "event_mesh",
        "first_agent": "observer",
        "first_queue": get_queue_for_agent("observer"),
        "correlation_id": envelope.correlation_id,
        **pub,
    }


_AGENT_PIPELINE_STATUS = {
    "observer": "PIPELINE_OBSERVER",
    "classifier": "PIPELINE_CLASSIFIER",
    "rca": "PIPELINE_RCA",
    "fixer": "PIPELINE_FIXER",
    "verifier": "PIPELINE_VERIFIER",
}


async def _update_pipeline_status(envelope: PipelineEnvelope, status: str) -> None:
    """Persist in-flight pipeline step so UI polling can advance the progress rail."""
    client = get_global_client()
    if not client:
        return
    try:
        client.upsert_observability_record({
            "run_id": envelope.run_id,
            "workflow_name": envelope.workflow_name,
            "subscription_id": envelope.subscription_id,
            "resource_group": envelope.resource_group,
            "status": status,
            "auto_fix_attempted": True,
        })
    except Exception as exc:
        logger.warning("[PIPELINE] status update failed: %s", exc)


async def process_queue_message(agent: str, envelope: PipelineEnvelope) -> None:
    """
    Worker entry: run agent via pipeline runner, then publish to next queue or finalize.
    """
    await _update_pipeline_status(envelope, _AGENT_PIPELINE_STATUS.get(agent, "PIPELINE_IN_PROGRESS"))
    envelope = await run_agent_step(agent, envelope)

    if envelope.status in ("skipped", "failed") and agent != "verifier":
        await _finalize_hana(envelope)
        logger.info(
            "[PIPELINE] stopped at %s correlation=%s status=%s",
            agent,
            envelope.correlation_id,
            envelope.status,
        )
        return

    nxt = get_next_agent(agent)
    if nxt:
        bus = get_bus()
        await bus.publish(nxt, envelope)
        logger.info(
            "[PIPELINE] %s → %s queue=%s correlation=%s",
            agent,
            nxt,
            get_queue_for_agent(nxt),
            envelope.correlation_id,
        )
    else:
        await _finalize_hana(envelope)
        logger.info(
            "[PIPELINE] complete correlation=%s status=%s",
            envelope.correlation_id,
            envelope.status,
        )


async def _finalize_hana(envelope: PipelineEnvelope) -> None:
    client = get_global_client()
    if not client:
        return
    fixer = envelope.fixer or {}
    verifier = envelope.verifier or {}
    rca = envelope.rca or {}
    fix_ok = bool(fixer.get("success"))

    if envelope.status == "skipped":
        final = "SKIPPED"
    elif envelope.status == "failed" or not fix_ok:
        final = "FIX_FAILED"
    elif fix_ok and verifier.get("verified"):
        final = "AUTO_FIXED"
    elif fix_ok:
        final = "AUTO_FIXED"
    else:
        final = envelope.status.upper()

    record: Dict[str, Any] = {
        "run_id": envelope.run_id,
        "workflow_name": envelope.workflow_name,
        "subscription_id": envelope.subscription_id,
        "resource_group": envelope.resource_group,
        "status": final,
        "auto_fix_attempted": True,
        "auto_fix_success": fix_ok,
        "rca_root_cause": rca.get("root_cause"),
        "fix_strategy": (fixer.get("fix_strategy") or {}).get("strategy_description")
        if isinstance(fixer.get("fix_strategy"), dict)
        else fixer.get("fix_strategy"),
    }
    if envelope.incident_id:
        record["incident_id"] = envelope.incident_id

    try:
        client.upsert_observability_record(record)
        resolved = envelope.incident_id or client.get_or_create_incident_id(envelope.run_id)
        logger.info(
            "[PIPELINE] HANA finalized incident=%s run=%s status=%s fix_ok=%s",
            resolved,
            envelope.run_id,
            final,
            fix_ok,
        )
    except Exception as exc:
        logger.warning("[PIPELINE] HANA finalize failed: %s", exc)
        return

    if final in {"FIX_FAILED", "FAILED", "NEEDS_MANUAL_REVIEW"}:
        try:
            settings = get_settings()
            incident_id = envelope.incident_id or client.get_or_create_incident_id(envelope.run_id)
            ticket_result = ensure_ticket_for_incident(client, incident_id, final, settings)
            if ticket_result.get("created"):
                logger.info(
                    "[PIPELINE] ITSM ticket created incident=%s ticket=%s",
                    incident_id,
                    ticket_result.get("ticket_number") or ticket_result.get("ticket_id"),
                )
            elif ticket_result.get("existing"):
                logger.info("[PIPELINE] ITSM ticket already exists for incident=%s", incident_id)
            elif ticket_result.get("error"):
                logger.warning(
                    "[PIPELINE] ITSM ticket creation failed for %s: %s",
                    incident_id,
                    ticket_result["error"],
                )
            elif ticket_result.get("skipped"):
                logger.debug(
                    "[PIPELINE] ITSM ticket skipped for %s: %s",
                    incident_id,
                    ticket_result.get("reason"),
                )
        except Exception as exc:
            logger.warning("[PIPELINE] ITSM hook failed for run=%s: %s", envelope.run_id, exc)


async def run_agent_pipeline_endpoint(agent: str, envelope: PipelineEnvelope) -> Dict[str, Any]:
    """HTTP handler body: execute one step; optionally chain publish if X-Publish-Next: true."""
    updated = await run_agent_step(agent, envelope)
    return {
        "agent": agent,
        "correlation_id": updated.correlation_id,
        "status": updated.status,
        "error": updated.error,
        "envelope": updated.to_event_payload(),
    }
