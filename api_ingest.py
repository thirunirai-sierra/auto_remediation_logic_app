# api_ingest.py
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from typing import Optional
from datetime import datetime, timedelta, timezone
from multi_flow_runner import collect_failed_run_errors
from remediation_tracker import get_tracker
from hana_observability import get_hana_client
from config import get_settings
import uvicorn
import os
import logging

from azure.monitor.query import LogsQueryClient
from azure.identity import ClientSecretCredential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
settings = get_settings()
hana_client = get_hana_client(settings)

if hana_client:
    hana_client.create_table()
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
    print("✅ HANA client ready")
else:
    print("❌ HANA client not available")

CHECKPOINT_FILE = "last_backfill_checkpoint.txt"

def get_last_checkpoint() -> datetime:
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return datetime.fromisoformat(f.read().strip())
    except:
        return datetime.now() - timedelta(days=7)

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

@app.post("/ingest")
async def ingest_failures():
    if not hana_client:
        raise HTTPException(503, "HANA client not available")

    cursor = hana_client.conn.cursor()
    cursor.execute("SELECT LAST_SUCCESSFUL_END_UTC FROM INGEST_WATERMARK WHERE PIPELINE_NAME = 'LogicAppsMonitor'")
    row = cursor.fetchone()
    watermark = row[0] if row else datetime.now() - timedelta(days=7)
    cursor.close()

    start_time = watermark - timedelta(minutes=15)
    end_time = datetime.now()

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
    | where TimeGenerated between (datetime({start_time.isoformat()}) .. datetime({end_time.isoformat()}))
    | project 
        TimeGenerated,
        resource_runId_s,
        resource_workflowName_s,
        error_code_s,
        error_message_s
    """

    try:
        response = logs_client.query_workspace(
            workspace_id=settings.log_analytics_workspace_id,
            query=query,
            timespan=(start_time, end_time)
        )
    except Exception as e:
        logger.error(f"Log Analytics query failed: {e}")
        raise HTTPException(500, f"Log Analytics error: {str(e)}")

    rows = response.tables[0].rows if response.tables else []
    if not rows:
        return {"fetched": 0, "message": "No new failures", "watermark": watermark.isoformat()}

    # Fix: make watermark timezone-aware (UTC) to compare with row[0]
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    results = []
    max_time = watermark
    for row in rows:
        ts = row[0]
        if ts > max_time:
            max_time = ts
        results.append({
            "TimeGenerated": ts.isoformat(),
            "resource_runId_s": row[1],
            "resource_workflowName_s": row[2],
            "error_code_s": row[3],
            "error_message_s": row[4] if len(row) > 4 else ""
        })

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

@app.post("/backfill")
async def backfill(background_tasks: BackgroundTasks):
    if not hana_client:
        raise HTTPException(503, "HANA client not available")

    last_success = get_last_checkpoint()
    start_date = last_success
    end_date = datetime.now()
    if (end_date - start_date).days > 30:
        start_date = end_date - timedelta(days=30)
        logger.warning(f"Backfill limited to last 30 days.")

    total_inserted = 0
    current = start_date

    while current < end_date:
        next_day = min(current + timedelta(days=1), end_date)
        query = f"""
        AzureDiagnostics
        | where ResourceProvider == "MICROSOFT.LOGIC"
        | where Category == "WorkflowRuntime"
        | where status_s == "Failed"
        | where TimeGenerated between (datetime({current.isoformat()}) .. datetime({next_day.isoformat()}))
        | project
            TimeGenerated,
            resource_runId_s,
            resource_workflowName_s,
            error_code_s,
            error_message_s
        """
        try:
            cred = ClientSecretCredential(settings.tenant_id, settings.client_id, settings.client_secret)
            logs_client = LogsQueryClient(cred)
            response = logs_client.query_workspace(
                workspace_id=settings.log_analytics_workspace_id,
                query=query,
                timespan=(current, next_day)
            )
            rows = response.tables[0].rows if response.tables else []
        except Exception as e:
            logger.error(f"Query failed for {current.date()}: {e}")
            current = next_day
            continue

        if rows:
            results = []
            for row in rows:
                results.append({
                    "TimeGenerated": row[0].isoformat(),
                    "resource_runId_s": row[1],
                    "resource_workflowName_s": row[2],
                    "error_code_s": row[3],
                    "error_message_s": row[4] if len(row) > 4 else ""
                })
            tracker = get_tracker()
            inserted = ingest_records(results, tracker)
            total_inserted += inserted
            logger.info(f"Day {current.date()}: ingested {inserted}")
        else:
            logger.info(f"Day {current.date()}: no failures")

        current = next_day

    save_checkpoint(current)
    return {
        "status": "backfill completed",
        "total_inserted": total_inserted,
        "last_checkpoint": current.isoformat()
    }

@app.get("/health")
def health():
    return {"status": "ok", "hana_connected": hana_client is not None}

@app.get("/observability/dashboard")
async def observability_dashboard(
    hours: int = Query(24),
    subscription_id: Optional[str] = Query(None)
):
    if not hana_client:
        raise HTTPException(503, "HANA client not available")
    end = datetime.now()
    start = end - timedelta(hours=hours)
    stats = hana_client.get_dashboard_stats(start, end, subscription_id)
    stats["lookback_hours"] = hours
    stats["lookback_start"] = start.isoformat()
    stats["lookback_end"] = end.isoformat()
    return stats

@app.get("/observability/incidents")
async def list_incidents(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    workflow_name: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    error_category: Optional[str] = Query(None)
):
    if not hana_client or not hana_client._ensure_connected():
        raise HTTPException(503, "HANA client not available")
    cursor = hana_client.conn.cursor()
    conditions = []
    params = []
    if workflow_name:
        conditions.append('WORKFLOW_NAME = ?')
        params.append(workflow_name)
    if status_filter:
        conditions.append('STATUS = ?')
        params.append(status_filter)
    if error_category:
        conditions.append('ERROR_CATEGORY = ?')
        params.append(error_category)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    count_sql = f'SELECT COUNT(*) FROM {hana_client.full_table} {where_clause}'
    cursor.execute(count_sql, params)
    total = cursor.fetchone()[0]
    data_sql = f'''
        SELECT 
            INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME,
            ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
            STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
            CREATED_AT, UPDATED_AT, AUTO_FIX_ATTEMPTED,
            AUTO_FIX_SUCCESS, RETRY_COUNT
        FROM {hana_client.full_table}
        {where_clause}
        ORDER BY CREATED_AT DESC
        LIMIT ? OFFSET ?
    '''
    cursor.execute(data_sql, params + [limit, offset])
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    incidents = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "incidents": incidents
    }

@app.get("/observability/error-categories")
async def error_categories():
    if not hana_client or not hana_client._ensure_connected():
        raise HTTPException(503, "HANA client not available")
    cursor = hana_client.conn.cursor()
    cursor.execute(f'''
        SELECT ERROR_CATEGORY, COUNT(*) 
        FROM {hana_client.full_table}
        GROUP BY ERROR_CATEGORY
        ORDER BY 2 DESC
    ''')
    categories = [{"category": row[0], "count": row[1]} for row in cursor.fetchall()]
    cursor.close()
    return {"categories": categories}

@app.get("/observability/all")
async def get_all_incidents(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    if not hana_client or not hana_client._ensure_connected():
        raise HTTPException(503, "HANA client not available")
    cursor = hana_client.conn.cursor()
    sql = f'''
        SELECT 
            INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME,
            ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
            STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
            CREATED_AT, UPDATED_AT, AUTO_FIX_ATTEMPTED,
            AUTO_FIX_SUCCESS, RETRY_COUNT
        FROM {hana_client.full_table}
        ORDER BY CREATED_AT DESC
        LIMIT ? OFFSET ?
    '''
    cursor.execute(sql, (limit, offset))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    incidents = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    return {
        "returned": len(incidents),
        "limit": limit,
        "offset": offset,
        "data": incidents
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)