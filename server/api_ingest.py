# api_ingest.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from remediation_tracker import get_tracker
from hana_observability import get_global_client
from config import get_settings
import logging
from azure.monitor.query import LogsQueryClient
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ingest", tags=["ingestion"])
settings = get_settings()

# Use singleton client
hana_client = get_global_client()

if hana_client:
    # Optional: ensure watermark table exists
    try:
        cursor = hana_client.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM SYS.TABLES
            WHERE SCHEMA_NAME = CURRENT_SCHEMA
            AND TABLE_NAME = 'INGEST_WATERMARK'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                CREATE COLUMN TABLE INGEST_WATERMARK (
                    PIPELINE_NAME NVARCHAR(64) PRIMARY KEY,
                    LAST_SUCCESSFUL_END_UTC TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO INGEST_WATERMARK
                VALUES ('LogicAppsMonitor', '2026-05-01 00:00:00')
            """)
            hana_client.conn.commit()
            logger.info("✅ Watermark table created")
        else:
            cursor.execute("SELECT COUNT(*) FROM INGEST_WATERMARK WHERE PIPELINE_NAME = 'LogicAppsMonitor'")
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO INGEST_WATERMARK VALUES ('LogicAppsMonitor', '2026-05-01 00:00:00')")
                hana_client.conn.commit()
        cursor.close()
    except Exception as e:
        logger.warning(f"Watermark table init: {e}")
else:
    logger.warning("HANA client not available – ingestion endpoints will fail")

CHECKPOINT_FILE = "last_backfill_checkpoint.txt"

def get_last_checkpoint() -> datetime:
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return datetime.fromisoformat(f.read().strip())
    except:
        return datetime.now() - timedelta(days=30)

def save_checkpoint(dt: datetime):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(dt.isoformat())

def categorize_error(error_message: str, error_code: str) -> str:
    msg = (error_message or "").lower()
    code = str(error_code)
    if "401" in code or "unauthorized" in msg:
        return "AUTH_CONFIG_ERROR"
    if "404" in code or "not found" in msg:
        return "MAPPING_ERROR"
    if "ssl" in msg or "certificate" in msg:
        return "SSL_ERROR"
    if "timeout" in msg:
        return "TIMEOUT_ERROR"
    if "null" in msg or "contains" in msg or "endsWith" in msg:
        return "NULL_REFERENCE_ERROR"
    if "add" in msg or "div" in msg or "numeric" in msg:
        return "DATA_VALIDATION"
    if "parse_json" in msg or "schema" in msg:
        return "SCHEMA_ERROR"
    return "UNKNOWN_ERROR"

def ingest_records(results: list, tracker) -> int:
    if not results or not hana_client:
        return 0
    records = []
    for run in results:
        run_id = run.get("resource_runId_s") or run.get("run_id")
        wf_name = run.get("resource_workflowName_s") or run.get("workflow_name")
        error_msg = run.get("error_message_s", "")
        error_code = run.get("error_code_s", "unknown")
        original_timestamp = run.get("TimeGenerated")

        rec = tracker.get_run_record(run_id) if run_id else None
        if rec:
            auto_attempted = rec.auto_fix_attempted
            auto_success = rec.auto_fix_success
            retry_count = rec.retry_count
            status = "Fix Succeeded" if auto_success else ("Fix Attempted" if auto_attempted else "Ticket Created")
            root_cause = rec.error_type
            fix_strategy = rec.status
        else:
            auto_attempted = False
            auto_success = False
            retry_count = 0
            status = "Ticket Created"
            root_cause = None
            fix_strategy = None

        records.append({
            "incident_id": run_id,
            "subscription_id": settings.subscription_id,
            "workflow_name": wf_name,
            "error_code": error_code,
            "error_message": error_msg[:2000],
            "error_category": categorize_error(error_msg, error_code),
            "status": status,
            "rca_root_cause": root_cause,
            "fix_strategy": fix_strategy,
            "created_at": original_timestamp,
            "updated_at": datetime.now().isoformat(),
            "auto_fix_attempted": auto_attempted,
            "auto_fix_success": auto_success,
            "retry_count": retry_count
        })

    success, _ = hana_client.batch_upsert(records)
    return success

def query_log_analytics_range(start: datetime, end: datetime) -> list:
    cred = ClientSecretCredential(
        tenant_id=settings.tenant_id,
        client_id=settings.client_id,
        client_secret=settings.client_secret
    )
    logs_client = LogsQueryClient(cred)
    query = f"""
    AzureDiagnostics
    | where ResourceProvider == "MICROSOFT.LOGIC"
    | where Category == "WorkflowRuntime"
    | where status_s == "Failed"
    | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
    | project 
        TimeGenerated,
        resource_runId_s,
        resource_workflowName_s,
        error_code_s,
        error_message_s
    """
    response = logs_client.query_workspace(
        workspace_id=settings.log_analytics_workspace_id,
        query=query,
        timespan=(start, end)
    )
    rows = response.tables[0].rows if response.tables else []
    results = []
    for row in rows:
        results.append({
            "TimeGenerated": row[0],
            "resource_runId_s": row[1],
            "resource_workflowName_s": row[2],
            "error_code_s": row[3],
            "error_message_s": row[4] if len(row) > 4 else ""
        })
    return results

@router.post("/incremental")
async def ingest_incremental():
    if not hana_client:
        raise HTTPException(503, "HANA client not available")
    cursor = hana_client.conn.cursor()
    cursor.execute("SELECT LAST_SUCCESSFUL_END_UTC FROM INGEST_WATERMARK WHERE PIPELINE_NAME = 'LogicAppsMonitor'")
    row = cursor.fetchone()
    watermark = row[0] if row else datetime.now() - timedelta(days=7)
    cursor.close()
    start_time = watermark - timedelta(minutes=15)
    end_time = datetime.now()
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    results = query_log_analytics_range(start_time, end_time)
    if not results:
        return {"fetched": 0, "message": "No new failures", "watermark": watermark.isoformat()}
    max_time = max(row["TimeGenerated"] for row in results)
    tracker = get_tracker()
    inserted = ingest_records(results, tracker)
    if max_time > watermark:
        cursor = hana_client.conn.cursor()
        cursor.execute(
            "UPDATE INGEST_WATERMARK SET LAST_SUCCESSFUL_END_UTC = ? WHERE PIPELINE_NAME = 'LogicAppsMonitor'",
            (max_time,)
        )
        hana_client.conn.commit()
        cursor.close()
    return {
        "fetched": len(results),
        "inserted": inserted,
        "failed": len(results) - inserted,
        "new_watermark": max_time.isoformat(),
        "timestamp": datetime.now().isoformat()
    }

@router.post("/backfill")
async def backfill(days: int = Query(30, description="Number of days to backfill (max 30)")):
    if not hana_client:
        raise HTTPException(503, "HANA client not available")
    days = min(days, 30)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    last_checkpoint = get_last_checkpoint()
    if last_checkpoint > start_date:
        start_date = last_checkpoint
        logger.info(f"Resuming backfill from last checkpoint: {start_date}")
    else:
        logger.info(f"Starting full backfill for last {days} days")
    total_inserted = 0
    chunk_size_days = 7
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_size_days), end_date)
        logger.info(f"Querying from {current.date()} to {chunk_end.date()}...")
        try:
            results = query_log_analytics_range(current, chunk_end)
            if results:
                tracker = get_tracker()
                inserted = ingest_records(results, tracker)
                total_inserted += inserted
                logger.info(f"  -> Ingestion: {inserted} records")
            else:
                logger.info("  -> No failures found")
        except Exception as e:
            logger.error(f"Query failed for {current.date()} to {chunk_end.date()}: {e}")
        current = chunk_end
        save_checkpoint(current)
    save_checkpoint(end_date)
    return {
        "status": "backfill completed",
        "total_inserted": total_inserted,
        "last_checkpoint": end_date.isoformat()
    }

@router.get("/health")
async def ingest_health():
    return {"status": "ok", "hana_connected": hana_client is not None}