from __future__ import annotations

import asyncio,json,logging,uuid,os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import get_settings
from routers import api_ingest, observability, knowledge, workflow,agents,dashboard
from services.agents.orchestrator import Orchestrator
from services.workflow_service import get_workflow
from services.auth import get_arm_token
from db.hana_client import get_global_client
from routers.api_ingest import query_log_analytics_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

_shutdown_event = asyncio.Event()
_monitor_task = None


async def continuous_monitor(settings):
    """
    Background task that polls Azure Log Analytics for failed Logic App runs
    and triggers remediation for each unique failure.

    The monitor runs indefinitely, sleeping 60 seconds between cycles.
    Each failure is processed sequentially with a 10‑second gap.

    Args:
        settings: Application settings (used for Log Analytics workspace ID,
                  lookback hours, and Azure credentials).
    """

    logger.info("=" * 80)
    logger.info("Continuous monitor started – polling every 60 seconds")
    logger.info("=" * 80)
    logger.info("Lookback hours: %d", settings.LOOKBACK_HOURS)

    orchestrator = Orchestrator(settings)

    async def remediate_one(workflow_name: str, run_id: str, error_msg: str, error_code: str):
        """
        Process a single failure: store initial record, run orchestrator,
        and update the database with the final status.

        Args:
            workflow_name: Name of the failed Logic App.
            run_id: Run ID of the failure.
            error_msg: Error message (from Log Analytics).
            error_code: Error code (from Log Analytics).

        Returns:
            Result dictionary from the orchestrator.
        """
        cycle_id = str(uuid.uuid4())[:8]
        logger.info("[MONITOR-%s] Processing failure: %s / %s", cycle_id, workflow_name, run_id)
        try:
            # --- Fetch artifact metadata from Azure before remediation ---
            artifact_meta = {}
            try:
                token = get_arm_token(
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
            rows = query_log_analytics_range(start_time, end_time)

            if not rows:
                logger.info("[MONITOR] No new failures found")
            else:
                logger.info("[MONITOR] Found %d failures", len(rows))
                seen = set()
                unique_failures = []
                for row in rows:
                    wf = row.get("resource_workflowName_s")
                    rid = row.get("resource_runId_s")
                    if not wf or not rid:
                        continue
                    key = (wf, rid)
                    if key in seen:
                        continue
                    seen.add(key)
                    unique_failures.append((wf, rid, row.get("error_message_s", ""), row.get("error_code_s", "")))

                logger.info("[MONITOR] Processing %d unique failures sequentially", len(unique_failures))

                successful = 0
                failed = 0

                for i, (wf, rid, err_msg, err_code) in enumerate(unique_failures):
                    logger.info("[MONITOR] ========================================")
                    logger.info("[MONITOR] Processing failure %d/%d: %s / %s", i+1, len(unique_failures), wf, rid)
                    logger.info("[MONITOR] ========================================")

                    try:
                        result = await remediate_one(wf, rid, err_msg, err_code)
                        if isinstance(result, dict) and result.get("status") == "remediated":
                            successful += 1
                            logger.info("[MONITOR] Task %d completed successfully", i+1)
                        else:
                            failed += 1
                            logger.info("[MONITOR] Task %d failed: %s", i+1, result.get("status", "unknown") if isinstance(result, dict) else "unknown")
                    except Exception as e:
                        failed += 1
                        logger.error("[MONITOR] Task %d crashed: %s", i+1, e)

                    if i < len(unique_failures) - 1:
                        logger.info("[MONITOR] Waiting 10 seconds before next failure...")
                        await asyncio.sleep(10)

                logger.info("[MONITOR] Cycle complete: %d processed, %d successful, %d failed",
                            len(unique_failures), successful, failed)

        except Exception as e:
            logger.error("[MONITOR] Cycle failed: %s", e, exc_info=True)

        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Starts the background continuous monitor on startup and ensures
    it is cancelled gracefully on shutdown.

    Args:
        app: The FastAPI application instance.
    """
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(api_ingest.router, prefix="/api/ingest", tags=["ingestion"])
app.include_router(observability.router, prefix="/api", tags=["observability"])
app.include_router(knowledge.router, prefix="/knowledge", tags=["knowledge"])
app.include_router(workflow.router, prefix="/workflows", tags=["workflows"])
app.include_router(dashboard.router)
app.include_router(agents.router)


@app.get("/api/monitor/status")
async def api_monitor_status():
    """
    Return the status of the continuous background monitor.

    Returns:
        dict: Contains:
            - is_running (bool): True if monitor task is active.
            - poll_interval_seconds (int): Fixed poll interval (60 seconds).
    """
    global _monitor_task
    is_running = _monitor_task is not None and not _monitor_task.done()
    return {"is_running": is_running, "poll_interval_seconds": 60}


@app.get("/")
async def root():
    """
    Root endpoint returning service information.

    Returns:
        dict: Service name and version.
    """
    return {"service": "Logic Apps Auto-Remediation", "version": "3.0.0"}





if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")