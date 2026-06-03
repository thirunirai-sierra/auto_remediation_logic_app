# server/routers/api_ingest.py
"""
Ingestion endpoints for Logic Apps failure telemetry.
"""
import logging
import re
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Query

from azure.monitor.query import LogsQueryClient
from azure.identity import ClientSecretCredential
from db.hana_client import get_global_client
from config import get_settings
from services.remediation_tracker import get_tracker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ingest", tags=["ingestion"])
settings = get_settings()

# Use singleton HANA client
hana_client = get_global_client()
if hana_client:
    logger.info("HANA observability client ready")
else:
    logger.warning("HANA client not available – ingestion endpoints will fail")


def query_log_analytics_range(start: datetime, end: datetime) -> list:
    """Query Azure Log Analytics for failed Logic App runs (timezone-aware)."""
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    cred = ClientSecretCredential(
        tenant_id=settings.AZURE_TENANT_ID,
        client_id=settings.AZURE_CLIENT_ID,
        client_secret=settings.AZURE_CLIENT_SECRET,
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
        error_message_s,
        _ResourceId,
        SubscriptionId
    """
    response = logs_client.query_workspace(
        workspace_id=settings.LOG_ANALYTICS_WORKSPACE_ID,
        query=query,
        timespan=(start, end),
    )
    if not response.tables:
        return []

    columns = response.tables[0].columns
    rows    = response.tables[0].rows
    results = []
    for row in rows:
        rd = dict(zip(columns, row))

        wf_name = rd.get("resource_workflowName_s") or ""
        if not wf_name:
            resource_id = str(rd.get("_ResourceId") or "")
            m = re.search(r"/workflows/([^/]+)", resource_id, re.IGNORECASE)
            if m:
                wf_name = m.group(1)
                logger.info("Resolved workflow_name='%s' from _ResourceId", wf_name)

        run_id = rd.get("resource_runId_s") or ""
        if not run_id or not wf_name:
            logger.warning("Skipping row — missing run_id=%r or workflow_name=%r", run_id, wf_name)
            continue

        results.append({
            "TimeGenerated":           rd.get("TimeGenerated"),
            "resource_runId_s":        run_id,
            "resource_workflowName_s": rd.get("resource_workflowName_s") or "",
            "workflow_name":           wf_name,
            "error_code_s":            rd.get("error_code_s") or "unknown",
            "error_message_s":         rd.get("error_message_s") or "",
            "_ResourceId":             rd.get("_ResourceId") or "",
            "SubscriptionId":          rd.get("SubscriptionId") or "",
        })
    return results


def categorize_error(error_message: str, error_code: str) -> str:
    """Classify the error into a broad category for the ERROR_CATEGORY column."""
    msg  = (error_message or "").lower()
    code = str(error_code or "")
    if "401" in code or "unauthorized" in msg:
        return "AUTH_CONFIG_ERROR"
    if "404" in code or "not found" in msg:
        return "MAPPING_ERROR"
    if "ssl" in msg or "certificate" in msg:
        return "SSL_ERROR"
    if "timeout" in msg:
        return "TIMEOUT_ERROR"
    if "null" in msg or "contains" in msg or "endswith" in msg:
        return "NULL_REFERENCE_ERROR"
    if "add" in msg or "div" in msg or "numeric" in msg:
        return "DATA_VALIDATION"
    if "parse_json" in msg or "schema" in msg:
        return "SCHEMA_ERROR"
    if "actionfailed" in code.lower() or "action failed" in msg:
        return "ACTION_FAILED"
    if "bad_request" in code.lower() or "400" in code:
        return "BAD_REQUEST"
    return "UNKNOWN_ERROR"


def _extract_subscription_id(resource_id: str, fallback: str = "") -> str:
    """Pull subscription UUID out of an Azure resource ID."""
    if resource_id:
        m = re.search(r"/subscriptions/([^/]+)", resource_id, re.IGNORECASE)
        if m:
            return m.group(1)
    return fallback


def _extract_resource_group(resource_id: str, fallback: str = "") -> str:
    """Pull resource-group name out of an Azure resource ID."""
    if resource_id:
        m = re.search(r"/resourceGroups/([^/]+)", resource_id, re.IGNORECASE)
        if m:
            return m.group(1)
    return fallback


def ingest_records(results: list, tracker) -> int:
    """
    Insert or update observability records in HANA.

    Key rules
    ---------
    * ``run_id``  — always the raw Azure run ID (goes into RUN_ID column).
      hana_client generates the ORBLOGICAPPS-YYYYMMDD-XXXXXX INCIDENT_ID.
    * All fields available from Log Analytics are mapped; nothing is left empty
      when the source data contains it.
    """
    if not results or not hana_client:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    records = []

    for run in results:
        run_id    = run.get("resource_runId_s") or run.get("run_id") or ""
        wf_name   = run.get("workflow_name") or run.get("resource_workflowName_s") or ""
        if not run_id or not wf_name:
            logger.warning(
                "ingest_records: skipping — missing run_id=%r workflow_name=%r", run_id, wf_name
            )
            continue

        resource_id = run.get("_ResourceId") or ""
        error_msg   = run.get("error_message_s") or ""
        error_code  = run.get("error_code_s") or "unknown"
        error_cat   = categorize_error(error_msg, error_code)
        error_type  = error_cat.lower()

        # Extract Azure identity fields from resource ID
        subscription_id = _extract_subscription_id(
            resource_id, fallback=getattr(settings, "AZURE_SUBSCRIPTION_ID", "")
        )
        resource_group = _extract_resource_group(
            resource_id, fallback=getattr(settings, "AZURE_RESOURCE_GROUP", "")
        )

        # Normalise TimeGenerated
        time_generated = run.get("TimeGenerated")
        if isinstance(time_generated, datetime):
            event_time_iso = time_generated.isoformat()
        elif isinstance(time_generated, str) and time_generated:
            event_time_iso = time_generated
        else:
            event_time_iso = now_iso

        # Pull remediation tracker data if available
        rec           = tracker.get_run_record(run_id) if run_id else None
        auto_attempted = rec.auto_fix_attempted if rec else False
        auto_success   = rec.auto_fix_success   if rec else False
        retry_count    = rec.retry_count         if rec else 0
        root_cause     = rec.error_type          if rec else None
        fix_strategy   = rec.status              if rec else None

        if rec:
            if auto_success:
                status = "FIX_SUCCEEDED"
            elif auto_attempted:
                status = "FIX_ATTEMPTED"
            else:
                status = "TICKET_CREATED"
        else:
            status = "TICKET_CREATED"

        records.append({
            # ── ID fields ──────────────────────────────────────────────
            # NEVER put run_id into "incident_id" — hana_client generates it
            "run_id":           run_id,

            # ── Core identity ──────────────────────────────────────────
            "subscription_id":  subscription_id,
            "resource_group":   resource_group,
            "workflow_name":    wf_name,

            # ── Error detail ───────────────────────────────────────────
            "error_code":       error_code,
            "error_message":    error_msg[:2000],
            "error_category":   error_cat,
            "error_type":       error_type,

            # ── Status ─────────────────────────────────────────────────
            "status":           status,
            "rca_root_cause":   root_cause,
            "fix_strategy":     fix_strategy,
            "auto_fix_attempted": auto_attempted,
            "auto_fix_success":   auto_success,
            "retry_count":        retry_count,

            # ── Timestamps ─────────────────────────────────────────────
            "event_time":       event_time_iso,
            "ingested_at":      now_iso,

            # ── Azure resource ─────────────────────────────────────────
            "resource_id":      resource_id,

            # ── Observability fields ───────────────────────────────────
            "log_start":             event_time_iso,
            "last_seen":             now_iso,
            "occurrence_count":      1,
            "source_type":           "AzureDiagnostics",
            "integration_flow_name": wf_name,
            "correlation_id":        resource_id,   # best proxy available
        })

    inserted, failed = hana_client.batch_upsert_observability(records)
    logger.info("ingest_records: %d inserted/updated, %d failed", inserted, failed)
    return inserted


# ── Watermark helpers ──────────────────────────────────────────────────────────

def init_watermark_table():
    """Ensure the INGEST_WATERMARK table exists with an initial row."""
    if not hana_client or not hana_client._ensure_connected():
        logger.warning("Cannot init watermark table: HANA client not available")
        return

    cursor = hana_client.conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM SYS.TABLES
            WHERE SCHEMA_NAME = CURRENT_SCHEMA
            AND TABLE_NAME = 'INGEST_WATERMARK'
        """)
        exists = cursor.fetchone()[0] > 0
        if not exists:
            cursor.execute("""
                CREATE COLUMN TABLE INGEST_WATERMARK (
                    PIPELINE_NAME           NVARCHAR(64) PRIMARY KEY,
                    LAST_SUCCESSFUL_END_UTC TIMESTAMP
                )
            """)
            hana_client.conn.commit()
            logger.info("Watermark table created")

        cursor.execute(
            "SELECT COUNT(*) FROM INGEST_WATERMARK WHERE PIPELINE_NAME = 'LogicAppsMonitor'"
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO INGEST_WATERMARK (PIPELINE_NAME, LAST_SUCCESSFUL_END_UTC)
                VALUES ('LogicAppsMonitor', '2026-05-01 00:00:00')
            """)
            hana_client.conn.commit()
            logger.info("Initial watermark row inserted")
    except Exception as e:
        logger.error("Watermark table init failed: %s", e)
        if hana_client.conn:
            hana_client.conn.rollback()
    finally:
        cursor.close()


if hana_client and hana_client.conn:
    init_watermark_table()
else:
    logger.warning("Skipping watermark table init – HANA client not connected")


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/incremental")
async def ingest_incremental():
    """Incremental ingestion of failures since last watermark."""
    if not hana_client or not hana_client.conn:
        raise HTTPException(503, "HANA client not available or not connected")

    cursor = hana_client.conn.cursor()
    cursor.execute(
        "SELECT LAST_SUCCESSFUL_END_UTC FROM INGEST_WATERMARK WHERE PIPELINE_NAME = 'LogicAppsMonitor'"
    )
    row = cursor.fetchone()
    watermark = row[0] if row else datetime.now(timezone.utc) - timedelta(days=7)
    cursor.close()

    start_time = watermark - timedelta(minutes=15)
    end_time   = datetime.now(timezone.utc)
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    results = query_log_analytics_range(start_time, end_time)
    if not results:
        return {
            "fetched":   0,
            "message":   "No new failures",
            "watermark": watermark.isoformat(),
        }

    max_time = max(row["TimeGenerated"] for row in results)
    tracker  = get_tracker()
    inserted = ingest_records(results, tracker)

    if max_time > watermark:
        cursor = hana_client.conn.cursor()
        cursor.execute(
            "UPDATE INGEST_WATERMARK SET LAST_SUCCESSFUL_END_UTC = ? WHERE PIPELINE_NAME = 'LogicAppsMonitor'",
            (max_time,),
        )
        hana_client.conn.commit()
        cursor.close()

    return {
        "fetched":       len(results),
        "inserted":      inserted,
        "failed":        len(results) - inserted,
        "new_watermark": max_time.isoformat(),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }


@router.post("/backfill")
async def backfill(
    days: int = Query(30, description="Number of days to backfill (max 30)")
):
    """Backfill failures for a given number of days."""
    if not hana_client or not hana_client.conn:
        raise HTTPException(503, "HANA client not available or not connected")

    days       = min(days, 30)
    end_date   = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    checkpoint_file = "last_backfill_checkpoint.txt"

    def get_last_checkpoint():
        try:
            with open(checkpoint_file) as f:
                return datetime.fromisoformat(f.read().strip())
        except Exception:
            return datetime.now(timezone.utc) - timedelta(days=30)

    def save_checkpoint(dt: datetime):
        with open(checkpoint_file, "w") as f:
            f.write(dt.isoformat())

    last_checkpoint = get_last_checkpoint()
    if last_checkpoint > start_date:
        start_date = last_checkpoint
        logger.info("Resuming backfill from last checkpoint: %s", start_date)
    else:
        logger.info("Starting full backfill for last %d days", days)

    total_inserted  = 0
    chunk_size_days = 7
    current         = start_date

    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_size_days), end_date)
        logger.info("Querying from %s to %s", current.date(), chunk_end.date())
        try:
            results = query_log_analytics_range(current, chunk_end)
            if results:
                tracker  = get_tracker()
                inserted = ingest_records(results, tracker)
                total_inserted += inserted
                logger.info("Ingested %d records", inserted)
            else:
                logger.info("No failures found in this chunk")
        except Exception as e:
            logger.error("Query failed for %s to %s: %s", current.date(), chunk_end.date(), e)
        current = chunk_end
        save_checkpoint(current)

    save_checkpoint(end_date)
    return {
        "status":          "backfill completed",
        "total_inserted":  total_inserted,
        "last_checkpoint": end_date.isoformat(),
    }


@router.get("/health")
async def ingest_health():
    return {
        "status":        "ok",
        "hana_connected": hana_client is not None and hana_client.conn is not None,
    }