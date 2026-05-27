# server/routers/dashboard.py
"""
Root-level endpoints for frontend dashboard (no /api prefix).
Provides KPIs, error distribution, timeline, and recent incidents.
"""

from fastapi import APIRouter, Query
from db.hana_client import get_global_client
from config import get_settings
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()


def get_hana_client():
    """Return the singleton HANA client."""
    return get_global_client()


@router.get("/logs/overview")
async def logs_overview(top: int = Query(1000, ge=1, le=5000)):
    """
    Aggregated metrics for the logs dashboard.

    Returns KPIs, status breakdown, error distribution, top failing workflows,
    timeline (by day), and recent error messages.

    Args:
        top (int): Maximum number of recent error messages to return. Default 1000.

    Returns:
        dict: Dashboard statistics and data.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        return {
            "kpi": {
                "total_flows": 0, "error_flows": 0, "fixed_flows": 0,
                "total_logs": 0, "total_error_messages": 0,
            },
            "status_breakdown": [],
            "error_distribution": [],
            "top_iflows": [],
            "timeline": [],
            "error_messages": [],
        }

    cursor = client.conn.cursor()
    table = client.full_table

    try:
        # KPIs
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_logs = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM {table}")
        total_flows = cursor.fetchone()[0] or 0

        cursor.execute(f"SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM {table} WHERE STATUS IN ('FIX_FAILED','FAILED')")
        error_flows = cursor.fetchone()[0] or 0

        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE STATUS IN ('AUTO_FIXED','FIX_VERIFIED')")
        fixed_flows = cursor.fetchone()[0] or 0

        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ERROR_MESSAGE IS NOT NULL")
        total_error_messages = cursor.fetchone()[0]

        # Status breakdown
        cursor.execute(f"SELECT STATUS, COUNT(*) FROM {table} GROUP BY STATUS ORDER BY COUNT(*) DESC")
        status_breakdown = [{"status": row[0], "count": row[1]} for row in cursor.fetchall()]

        # Error distribution
        cursor.execute(
            f"SELECT ERROR_CATEGORY, COUNT(*) FROM {table} WHERE ERROR_CATEGORY IS NOT NULL "
            "GROUP BY ERROR_CATEGORY ORDER BY COUNT(*) DESC"
        )
        error_distribution = [{"error_type": row[0] or "UNKNOWN", "count": row[1]} for row in cursor.fetchall()]

        # Top failing workflows
        cursor.execute(
            f"SELECT WORKFLOW_NAME, COUNT(*) FROM {table} GROUP BY WORKFLOW_NAME ORDER BY COUNT(*) DESC LIMIT 10"
        )
        top_iflows = [{"iflow_name": row[0], "failure_count": row[1]} for row in cursor.fetchall()]

        # Timeline (daily)
        cursor.execute(f"""
            SELECT CAST(CREATED_AT AS DATE) AS log_date, COUNT(*) as count
            FROM {table}
            WHERE CREATED_AT IS NOT NULL
            GROUP BY CAST(CREATED_AT AS DATE)
            ORDER BY CAST(CREATED_AT AS DATE) DESC
            LIMIT 30
        """)
        timeline = [{"time": str(row[0]), "count": row[1]} for row in cursor.fetchall()]

        # Recent error messages
        cursor.execute(f"""
            SELECT WORKFLOW_NAME, ERROR_CODE, ERROR_MESSAGE, CREATED_AT,
                   SUBSCRIPTION_ID, STATUS, INCIDENT_ID
            FROM {table}
            WHERE ERROR_MESSAGE IS NOT NULL
            ORDER BY CREATED_AT DESC
            LIMIT {top}
        """)
        error_messages = []
        for row in cursor.fetchall():
            error_messages.append({
                "integrationScenario": row[0],
                "errorType": row[1],
                "errorMessage": row[2],
                "time": row[3].isoformat() if row[3] else None,
                "resourceId": None,
                "status": row[5],
                "runId": row[6],
            })

        return {
            "kpi": {
                "total_flows": total_flows,
                "error_flows": error_flows,
                "fixed_flows": fixed_flows,
                "total_logs": total_logs,
                "total_error_messages": total_error_messages,
            },
            "status_breakdown": status_breakdown,
            "error_distribution": error_distribution,
            "top_iflows": top_iflows,
            "timeline": timeline,
            "error_messages": error_messages,
        }

    except Exception as e:
        logger.error("Error in logs_overview: %s", e, exc_info=True)
        return {
            "kpi": {
                "total_flows": 0, "error_flows": 0, "fixed_flows": 0,
                "total_logs": 0, "total_error_messages": 0,
            },
            "status_breakdown": [],
            "error_distribution": [],
            "top_iflows": [],
            "timeline": [],
            "error_messages": [],
        }
    finally:
        cursor.close()


@router.get("/incidents")
async def incidents():
    """
    Simplified list of incidents for the logs page.

    Returns the 500 most recent incidents with basic fields.

    Returns:
        list: List of incident dictionaries.
    """
    client = get_hana_client()
    if not client or not client._ensure_connected():
        return []

    try:
        cursor = client.conn.cursor()
        cursor.execute(f"""
            SELECT INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME, ERROR_CODE,
                   ERROR_MESSAGE, CREATED_AT
            FROM {client.full_table}
            ORDER BY CREATED_AT DESC
            LIMIT 500
        """)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({
                "incidentId": row[0],
                "subscriptionId": row[1],
                "integrationScenario": row[2],
                "errorType": row[3],
                "errorMessage": row[4],
                "time": row[5].isoformat() if row[5] else None,
            })
        cursor.close()
        return result
    except Exception as e:
        logger.error("Error in incidents: %s", e, exc_info=True)
        return []


@router.get("/dashboard")
async def dashboard():
    """
    Simple health / status endpoint for the dashboard.

    Returns:
        dict: Service status.
    """
    return {"status": "ok", "message": "Dashboard endpoint ready"}