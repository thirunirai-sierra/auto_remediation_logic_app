# server/main.py
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from routers import api_ingest, observability, knowledge, workflow
from services.agents.orchestrator import Orchestrator

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
                result = await orchestrator.remediate(
                    workflow_name=workflow_name,
                    run_id=run_id,
                    subscription_id=settings.AZURE_SUBSCRIPTION_ID,
                    resource_group=settings.AZURE_RESOURCE_GROUP,
                )
                status = result.get("status", "unknown")
                logger.info("[MONITOR-%s] Result for %s: %s", cycle_id, run_id, status)
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
                tasks = []
                for row in rows:
                    wf = row.get("resource_workflowName_s")
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(api_ingest.router, prefix="/api/ingest", tags=["ingestion"])
app.include_router(observability.router, prefix="/api", tags=["observability"])
app.include_router(knowledge.router, prefix="/knowledge", tags=["knowledge"])
app.include_router(workflow.router, prefix="/workflows", tags=["workflows"])

@app.get("/")
async def root():
    return {"service": "Logic Apps Auto-Remediation", "version": "3.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")