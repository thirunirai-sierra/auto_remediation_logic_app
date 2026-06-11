from __future__ import annotations

import asyncio
import json
import logging
import uuid
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import get_settings
from routers import api_ingest, observability, knowledge, workflow, settings as settings_router, agents, dashboard
from services.agents.orchestrator import Orchestrator
from services.workflow_service import get_workflow
from services.auth import get_arm_token
from db.hana_client import get_global_client
from routers.api_ingest import query_log_analytics_range, categorize_error
from routers import event_mesh as event_mesh_router
from services.itsm_service import ensure_ticket_for_incident
from monitoring.llm_monitor import log_agent_invoke
import monitoring.llm_monitor as _m; print("MONITOR FILE:", _m.__file__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

_shutdown_event = asyncio.Event()
_monitor_task   = None

# ─────────────────────────────────────────────────────────────────────────────
#  In-memory skip cache
#  Stores run_ids that have already been fully processed in this session so
#  we never re-process them even if Log Analytics still returns them during
#  the lookback window.
#  Structure: { run_id: {"status": str, "processed_at": datetime} }
# ─────────────────────────────────────────────────────────────────────────────
_processed_runs: dict[str, dict] = {}

# Statuses that mean "nothing left to do for this run"
_TERMINAL_STATUSES = {
    "AUTO_FIXED", "REMEDIATED", "SKIPPED", "FAILED",
    "FIX_SUCCEEDED", "FIX_ATTEMPTED",
    # Ingestion-only terminal states (auto-remediation disabled)
    "DETECTED", "DETECTED_ONLY",
}
_IN_FLIGHT_STATUSES = {
    "PIPELINE_IN_PROGRESS",
    "PIPELINE_STARTED",
    "PIPELINE_OBSERVER",
    "PIPELINE_CLASSIFIER",
    "PIPELINE_RCA",
    "PIPELINE_FIXER",
    "PIPELINE_VERIFIER",
    "FIX_IN_PROGRESS",
}


def _is_already_processed(run_id: str) -> bool:
    """
    Return True if this run_id is in the in-memory cache with a terminal status
    OR if HANA already has a terminal STATUS for the mapped INCIDENT_ID.
    """
    # 1. Fast in-memory check
    entry = _processed_runs.get(run_id)
    st_mem = entry.get("status", "").upper() if entry else ""
    if st_mem in _TERMINAL_STATUSES or st_mem in _IN_FLIGHT_STATUSES:
        logger.debug(
            "[SKIP] run_id=%s already processed in-memory (status=%s)",
            run_id, st_mem,
        )
        return True

    # 2. HANA persistent check — look up via RUN_INCIDENT_MAP then STATUS column
    client = get_global_client()
    if not client:
        return False
    try:
        cursor = client.conn.cursor()
        cursor.execute(
            f"SELECT INCIDENT_ID FROM {client.map_table} WHERE RUN_ID = ?",
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return False

        incident_id = row[0]
        cursor.execute(
            f"SELECT STATUS FROM {client.full_table} WHERE INCIDENT_ID = ?",
            (incident_id,),
        )
        row = cursor.fetchone()
        cursor.close()

        st = str(row[0]).upper() if row else ""
        if st in _TERMINAL_STATUSES or st in _IN_FLIGHT_STATUSES:
            # Populate cache so next cycle is instant
            _processed_runs[run_id] = {
                "status": row[0],
                "processed_at": datetime.now(timezone.utc),
            }
            logger.info(
                "[SKIP] run_id=%s already terminal in HANA (status=%s, incident=%s)",
                run_id, row[0], incident_id,
            )
            return True
    except Exception as e:
        logger.warning("[SKIP] Could not check HANA status for run_id=%s: %s", run_id, e)

    return False


def _mark_processed(run_id: str, status: str) -> None:
    """Record this run_id in the in-memory cache."""
    _processed_runs[run_id] = {
        "status": status.upper(),
        "processed_at": datetime.now(timezone.utc),
    }


def _extract_subscription_id(resource_id: str) -> str:
    """Extract subscription ID from Azure resource ID string."""
    if not resource_id:
        return ""
    m = re.search(r"/subscriptions/([^/]+)", resource_id, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_resource_group(resource_id: str) -> str:
    """Extract resource group from Azure resource ID string."""
    if not resource_id:
        return ""
    m = re.search(r"/resourceGroups/([^/]+)", resource_id, re.IGNORECASE)
    return m.group(1) if m else ""


async def continuous_monitor(settings):
    """
    Background task that polls Azure Log Analytics for failed Logic App runs,
    stores incidents in HANA, and optionally triggers remediation.

    Skip logic
    ----------
    A run_id is skipped if it already has a terminal STATUS in HANA
    (AUTO_FIXED, REMEDIATED, SKIPPED, FAILED, FIX_SUCCEEDED, FIX_ATTEMPTED,
    DETECTED, DETECTED_ONLY) or if it was processed during this session
    (in-memory cache).
    """
    logger.info("=" * 80)
    logger.info("Continuous monitor started – polling every 60 seconds")
    logger.info("=" * 80)
    logger.info("Lookback hours: %d", settings.LOOKBACK_HOURS)

    orchestrator     = Orchestrator(settings)
    auto_fix_enabled = getattr(settings, "ENABLE_AUTO_MONITOR", True)
    logger.info(
        "Auto-remediation is %s",
        "ENABLED" if auto_fix_enabled else "DISABLED (ingestion only)",
    )

    # ------------------------------------------------------------------
    async def process_failure(
        workflow_name: str,
        run_id: str,
        error_msg: str,
        error_code: str,
        resource_id: str,
        time_generated,   # datetime | str | None from Log Analytics
    ):
        """
        Store failure in HANA with all available fields, then optionally remediate.
        """
        cycle_id = str(uuid.uuid4())[:8]
        logger.info(
            "[MONITOR-%s] Processing failure: %s / %s", cycle_id, workflow_name, run_id
        )

        settings_obj = get_settings()

        # ── Derive fields ─────────────────────────────────────────────
        subscription_id = (
            _extract_subscription_id(resource_id)
            or getattr(settings_obj, "AZURE_SUBSCRIPTION_ID", "") or ""
        )
        resource_group = (
            _extract_resource_group(resource_id)
            or getattr(settings_obj, "AZURE_RESOURCE_GROUP", "") or ""
        )
        error_category = categorize_error(error_msg, error_code)
        error_type     = error_category.lower()

        # Normalise time_generated to an ISO string
        if isinstance(time_generated, datetime):
            event_time_iso = time_generated.isoformat()
        elif isinstance(time_generated, str) and time_generated:
            event_time_iso = time_generated
        else:
            event_time_iso = datetime.now(timezone.utc).isoformat()

        ingested_at_iso = datetime.now(timezone.utc).isoformat()

        # ── Fetch artifact metadata from Azure ────────────────────────
        artifact_meta = {}
        try:
            token = get_arm_token(
                settings_obj.AZURE_TENANT_ID,
                settings_obj.AZURE_CLIENT_ID,
                settings_obj.AZURE_CLIENT_SECRET,
            )
            wf_detail = await asyncio.to_thread(
                get_workflow, token,
                subscription_id or settings_obj.AZURE_SUBSCRIPTION_ID,
                resource_group  or settings_obj.AZURE_RESOURCE_GROUP,
                workflow_name,
            )
            props = wf_detail.get("properties", {})
            artifact_meta = {
                "name":         wf_detail.get("name"),
                "artifact_id":  wf_detail.get("id"),
                "version":      props.get("version"),
                "package":      props.get("package", "default"),
                "deployed_on":  props.get("createdTime"),
                "deployed_by":  "unknown",
                "runtime_node": wf_detail.get("location"),
                "status":       props.get("provisioningState"),
            }
        except Exception as e:
            logger.warning("[MONITOR-%s] Failed to fetch artifact meta: %s", cycle_id, e)

        # ── Initial history entry ─────────────────────────────────────
        initial_history = [
            {
                "step":        "Detected",
                "description": "CPI error detected and incident created.",
                "status":      "completed",
                "timestamp":   ingested_at_iso,
            }
        ]

        # ── Build the full record ─────────────────────────────────────
        initial_record = {
            "run_id":            run_id,
            "subscription_id":   subscription_id,
            "resource_group":    resource_group,
            "workflow_name":     workflow_name,
            "error_code":        error_code or "unknown",
            "error_message":     (error_msg or "")[:2000],
            "error_category":    error_category,
            "error_type":        error_type,
            "status":            "DETECTED",
            "auto_fix_attempted": False,
            "auto_fix_success":   False,
            "retry_count":        0,
            "event_time":        event_time_iso,
            "ingested_at":       ingested_at_iso,
            "resource_id":       resource_id or "",
            "artifact_json":     json.dumps(artifact_meta) if artifact_meta else None,
            "history_entries":   json.dumps(initial_history),
            "source_type":            "AzureDiagnostics",
            "integration_flow_name":  workflow_name,
            "occurrence_count":       1,
            "last_seen":              ingested_at_iso,
        }

        client = get_global_client()
        if client:
            try:
                client.upsert_observability_record(initial_record)
                logger.info(
                    "[MONITOR-%s] Initial record stored. run_id=%s", cycle_id, run_id
                )
            except Exception as e:
                logger.error(
                    "[MONITOR-%s] Failed to store initial record: %s", cycle_id, e
                )

        # ── Ingestion-only mode ───────────────────────────────────────
        if not auto_fix_enabled:
            logger.info(
                "[MONITOR-%s] Auto-remediation disabled – stored only", cycle_id
            )
            # Mark as terminal so the same run is not re-processed next cycle
            _mark_processed(run_id, "DETECTED")
            return {"status": "detected_only"}

        # ── Remediation path ──────────────────────────────────────────
        sub = subscription_id or settings_obj.AZURE_SUBSCRIPTION_ID
        rg  = resource_group  or settings_obj.AZURE_RESOURCE_GROUP

        if getattr(settings_obj, "EVENT_MESH_PIPELINE_ENABLED", True):
            from services.event_mesh.pipeline import start_pipeline

            if client:
                try:
                    client.upsert_observability_record({
                        "run_id": run_id,
                        "status": "PIPELINE_IN_PROGRESS",
                    })
                except Exception as e:
                    logger.error(
                        "[MONITOR-%s] Failed to set pipeline status: %s", cycle_id, e
                    )

            logger.info(
                "[MONITOR-%s] Starting Event Mesh pipeline (5 queues) …", cycle_id
            )
            result = await start_pipeline(
                workflow_name, run_id, sub, rg, source="monitor"
            )
            log_agent_invoke(result)
            _mark_processed(run_id, "PIPELINE_IN_PROGRESS")
            return result

        logger.info("[MONITOR-%s] Calling orchestrator (inline) …", cycle_id)
        result = await orchestrator.remediate(
            workflow_name=workflow_name,
            run_id=run_id,
            subscription_id=sub,
            resource_group=rg,
        )
        log_agent_invoke(result)
        rem_status = result.get("status", "unknown")
        logger.info("[MONITOR-%s] Result for %s: %s", cycle_id, run_id, rem_status)

        if client:
            final_status = "AUTO_FIXED" if rem_status == "remediated" else rem_status.upper()
            fix_strategy = result.get("fix_strategy")
            if isinstance(fix_strategy, dict):
                fix_strategy = fix_strategy.get("strategy_description", str(fix_strategy))

            update_record = {
                "run_id":            run_id,
                "status":            final_status,
                "auto_fix_attempted": True,
                "auto_fix_success":   (rem_status == "remediated"),
                "fix_strategy":      fix_strategy,
                "rca_root_cause":    result.get("root_cause"),
            }
            try:
                client.upsert_observability_record(update_record)
                ticket_result = ensure_ticket_for_incident(client, run_id, final_status, settings)
                if ticket_result.get("error"):
                    logger.error("Failed to create ITSM ticket for %s: %s", run_id, ticket_result["error"])
            except Exception as e:
                logger.error(
                    "[MONITOR-%s] Failed to update final status: %s", cycle_id, e
                )

        _mark_processed(
            run_id,
            "AUTO_FIXED" if rem_status == "remediated" else rem_status.upper(),
        )

        if rem_status == "remediated":
            logger.info("[MONITOR-%s] SUCCESS: Auto-remediation completed", cycle_id)
        return result
    # ------------------------------------------------------------------

    while not _shutdown_event.is_set():
        try:
            if not settings.LOG_ANALYTICS_WORKSPACE_ID:
                logger.warning("LOG_ANALYTICS_WORKSPACE_ID not set – monitor disabled")
                await asyncio.sleep(60)
                continue

            end_time   = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=settings.LOOKBACK_HOURS)
            logger.info(
                "[MONITOR] Querying Log Analytics from %s to %s",
                start_time.isoformat(), end_time.isoformat(),
            )

            rows = await asyncio.to_thread(query_log_analytics_range, start_time, end_time)

            if not rows:
                logger.info("[MONITOR] No failures found")
            else:
                logger.info("[MONITOR] Found %d failures", len(rows))

                # De-duplicate by (workflow, run_id) and skip already-done runs
                seen: set[tuple] = set()
                unique_failures: list = []

                for row in rows:
                    wf  = row.get("workflow_name") or row.get("resource_workflowName_s") or ""
                    rid = row.get("resource_runId_s") or ""
                    if not wf or not rid:
                        continue
                    key = (wf, rid)
                    if key in seen:
                        continue
                    seen.add(key)

                    if _is_already_processed(rid):
                        logger.info(
                            "[MONITOR] SKIP run_id=%s workflow=%s (already terminal)",
                            rid, wf,
                        )
                        continue

                    unique_failures.append((
                        wf,
                        rid,
                        row.get("error_message_s", ""),
                        row.get("error_code_s", ""),
                        row.get("_ResourceId", ""),
                        row.get("TimeGenerated"),
                    ))

                logger.info(
                    "[MONITOR] Processing %d unique new failures", len(unique_failures)
                )
                successful    = 0
                failed_count  = 0
                skipped_count = 0

                for i, (wf, rid, err_msg, err_code, res_id, time_gen) in enumerate(
                    unique_failures
                ):
                    logger.info("[MONITOR] ========================================")
                    logger.info(
                        "[MONITOR] Processing failure %d/%d: %s / %s",
                        i + 1, len(unique_failures), wf, rid,
                    )
                    logger.info("[MONITOR] ========================================")

                    try:
                        result = await process_failure(
                            wf, rid, err_msg, err_code, res_id, time_gen
                        )
                        status = (
                            result.get("status", "unknown")
                            if isinstance(result, dict)
                            else "unknown"
                        )

                        if status == "remediated":
                            successful += 1
                            logger.info(
                                "[MONITOR] Task %d completed successfully", i + 1
                            )
                        elif status in ("detected_only", "skipped"):
                            # Expected non-error outcome — not a failure
                            skipped_count += 1
                            logger.info(
                                "[MONITOR] Task %d finished with status: %s",
                                i + 1, status,
                            )
                        else:
                            failed_count += 1
                            logger.info(
                                "[MONITOR] Task %d finished with status: %s",
                                i + 1, status,
                            )
                    except Exception as e:
                        failed_count += 1
                        logger.error("[MONITOR] Task %d crashed: %s", i + 1, e)

                    if i < len(unique_failures) - 1:
                        logger.info(
                            "[MONITOR] Waiting 10 seconds before next failure …"
                        )
                        await asyncio.sleep(10)

                logger.info(
                    "[MONITOR] Cycle complete: %d processed, %d remediated, "
                    "%d stored/skipped, %d failed",
                    len(unique_failures), successful, skipped_count, failed_count,
                )

        except Exception as e:
            logger.error("[MONITOR] Cycle failed: %s", e, exc_info=True)

        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _monitor_task
    logger.info("=" * 80)
    logger.info("Starting Logic Apps Auto-Remediation API Server")
    logger.info("=" * 80)
    settings = get_settings()

    _monitor_task = asyncio.create_task(continuous_monitor(settings))
    logger.info("Background monitor started (ingestion always active)")

    bus = None
    if getattr(settings, "EVENT_MESH_PIPELINE_ENABLED", True):
        from services.event_mesh.bus import get_bus
        from services.event_mesh.pipeline import process_queue_message

        bus = get_bus()
        await bus.start_workers(process_queue_message)
        logger.info("Event Mesh pipeline workers started (5 queues)")

    yield

    logger.info("=" * 80)
    logger.info("Shutting down API Server and monitor...")
    logger.info("=" * 80)
    _shutdown_event.set()
    if bus:
        await bus.stop_workers()
    if _monitor_task:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Logic Apps Auto-Remediation API",
    version="3.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_ingest.router,    prefix="/api/ingest",  tags=["ingestion"])
app.include_router(observability.router, prefix="/api",         tags=["observability"])
app.include_router(knowledge.router,     prefix="/knowledge",   tags=["knowledge"])
app.include_router(workflow.router,      prefix="/workflows",   tags=["workflows"])
app.include_router(dashboard.router)
app.include_router(agents.router)
app.include_router(settings_router.router)
app.include_router(event_mesh_router.router)


@app.get("/api/monitor/status")
async def api_monitor_status():
    """Return the status of the continuous background monitor."""
    global _monitor_task
    return {
        "is_running":            _monitor_task is not None and not _monitor_task.done(),
        "poll_interval_seconds": 60,
        "cached_processed_runs": len(_processed_runs),
    }


@app.get("/")
async def root():
    return {"service": "Logic Apps Auto-Remediation", "version": "3.0.0"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")