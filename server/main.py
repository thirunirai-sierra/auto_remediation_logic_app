from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dashboard
from config import get_settings
from routers import api_ingest, observability, knowledge, workflow, settings as settings_router
from services.agents.orchestrator import Orchestrator
from services.workflow_service import get_workflow
from services.auth import get_arm_token
from db.hana_client import get_global_client
from routers import agents
from routers import event_mesh as event_mesh_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

_shutdown_event = asyncio.Event()
_monitor_task = None


async def continuous_monitor(settings):
    from routers.api_ingest import query_log_analytics_range

    logger.info("=" * 80)
    logger.info("Continuous monitor started – polling every 60 seconds")
    logger.info("=" * 80)
    logger.info("Lookback hours: %d", settings.LOOKBACK_HOURS)

    orchestrator = Orchestrator(settings)
    semaphore = asyncio.Semaphore(5)

    async def remediate_one(workflow_name: str, run_id: str, error_msg: str, error_code: str):
        async with semaphore:
            cycle_id = str(uuid.uuid4())[:8]
            logger.info("[MONITOR-%s] Processing failure: %s / %s", cycle_id, workflow_name, run_id)
            try:
                # --- Fetch artifact metadata from Azure before remediation ---
                artifact_meta = {}
                try:
                    token = await asyncio.to_thread(
                        get_arm_token,
                        settings.AZURE_TENANT_ID,
                        settings.AZURE_CLIENT_ID,
                        settings.AZURE_CLIENT_SECRET
                    )
                    workflow = await asyncio.to_thread(
                        get_workflow, token,
                        settings.AZURE_SUBSCRIPTION_ID,
                        settings.AZURE_RESOURCE_GROUP,
                        workflow_name
                    )
                    props = workflow.get("properties", {})
                    artifact_meta = {
                        "name": workflow.get("name"),
                        "artifact_id": workflow.get("id"),
                        "version": props.get("version"),
                        "package": props.get("package", "default"),
                        "deployed_on": props.get("createdTime"),
                        "deployed_by": "unknown",
                        "runtime_node": workflow.get("location"),
                        "status": props.get("provisioningState"),
                    }
                except Exception as e:
                    logger.warning("Failed to fetch artifact meta for %s: %s", workflow_name, e)

                # --- Initial history entry: detected ---
                initial_history = [{
                    "step": "Detected",
                    "description": "CPI error detected and incident created.",
                    "status": "completed",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }]

                # Store initial record (or update if already exists)
                client = get_global_client()
                if client:
                    try:
                        client.upsert_observability_record({
                            "incident_id": run_id,
                            "workflow_name": workflow_name,
                            "error_message": error_msg,
                            "error_code": error_code,
                            "status": "DETECTED",
                            "artifact_json": json.dumps(artifact_meta),
                            "history_entries": json.dumps(initial_history),
                        })
                    except Exception as e:
                        logger.error("Failed to insert initial record for %s: %s", run_id, e)

                # --- Run remediation ---
                result = await orchestrator.remediate(
                    workflow_name=workflow_name,
                    run_id=run_id,
                    subscription_id=settings.AZURE_SUBSCRIPTION_ID,
                    resource_group=settings.AZURE_RESOURCE_GROUP,
                )
                status = result.get("status", "unknown")
                logger.info("[MONITOR-%s] Result for %s: %s", cycle_id, run_id, status)

                # --- Update DB with final status and fix details ---
                if client:
                    final_status = "AUTO_FIXED" if status == "remediated" else status.upper()
                    fix_strategy = result.get("fix_strategy")
                    if isinstance(fix_strategy, dict):
                        fix_strategy = fix_strategy.get("strategy_description", str(fix_strategy))
                    try:
                        client.upsert_observability_record({
                            "incident_id": run_id,
                            "status": final_status,
                            "auto_fix_attempted": True,
                            "auto_fix_success": (status == "remediated"),
                            "fix_strategy": fix_strategy,
                            "rca_root_cause": result.get("root_cause"),
                            "suggested_fix": result.get("suggested_fix"),
                        })
                    except Exception as e:
                        logger.error("Failed to update final status for %s: %s", run_id, e)

                if status == "remediated":
                    logger.info("[MONITOR-%s] SUCCESS: Auto-remediation completed", cycle_id)
                return result
            except Exception as e:
                logger.error("[MONITOR-%s] Exception: %s", cycle_id, e, exc_info=True)
                return {"status": "error"}

    while not _shutdown_event.is_set():
        try:
            if not settings.LOG_ANALYTICS_WORKSPACE_ID:
                logger.warning("LOG_ANALYTICS_WORKSPACE_ID not set – monitor disabled")
                await asyncio.sleep(60)
                continue

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=settings.LOOKBACK_HOURS)
            logger.info("[MONITOR] Querying Log Analytics from %s to %s", start_time.isoformat(), end_time.isoformat())
            rows = await asyncio.to_thread(query_log_analytics_range, start_time, end_time)

            if not rows:
                logger.info("[MONITOR] No new failures found")
            else:
                logger.info("[MONITOR] Found %d failures", len(rows))
                seen = set()
                tasks = []
                for row in rows:
                    wf  = row.get("workflow_name") or row.get("resource_workflowName_s")
                    rid = row.get("resource_runId_s")
                    if not wf or not rid:
                        continue
                    key = (wf, rid)
                    if key in seen:
                        continue
                    seen.add(key)
                    tasks.append(remediate_one(wf, rid, row.get("error_message_s", ""), row.get("error_code_s", "")))
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "remediated")
                    logger.info("[MONITOR] Cycle complete: %d processed, %d successful", len(tasks), successful)
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
    yield
    logger.info("=" * 80)
    logger.info("Shutting down API Server and monitor...")
    logger.info("=" * 80)
    _shutdown_event.set()
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])
app.include_router(api_ingest.router, prefix="/api/ingest", tags=["ingestion"])
app.include_router(observability.router, prefix="/api", tags=["observability"])
app.include_router(knowledge.router, prefix="/knowledge", tags=["knowledge"])
app.include_router(workflow.router, prefix="/workflows", tags=["workflows"])
app.include_router(dashboard.router)
app.include_router(agents.router)
app.include_router(settings_router.router)
app.include_router(event_mesh_router.router)

@app.get("/api/monitor/status")
async def api_monitor_status():
    """Return the status of the continuous monitor."""
    global _monitor_task
    is_running = _monitor_task is not None and not _monitor_task.done()
    return {"is_running": is_running, "poll_interval_seconds": 60}

@app.get("/")
async def root():
    return {"service": "Logic Apps Auto-Remediation", "version": "3.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")