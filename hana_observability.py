"""
HANA Observability Module – Direct INSERT/UPDATE using fully qualified table name.
"""
import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from hdbcli import dbapi
    HDBCLI_AVAILABLE = True
except ImportError:
    HDBCLI_AVAILABLE = False
    logger.warning("hdbcli not installed. Install: pip install hdbcli")

class HANAObservabilityClient:
    def __init__(self, host: str, port: int, user: str, password: str, schema: str, table: str = "LOGIC_APPS_OBSERVABILITY"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.schema = schema
        self.table = table
        # Fully qualified, quoted table name – this never changes
        self.full_table = f'"{self.schema}"."{self.table}"'
        self.conn = None
        self._connect()

    def _connect(self) -> bool:
        if not HDBCLI_AVAILABLE:
            return False
        for attempt in range(3):
            try:
                self.conn = dbapi.connect(
                    address=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    encrypt=True,
                    sslValidateCertificate=False,
                    timeout=30
                )
                logger.info(f"Connected to HANA {self.host}:{self.port}")
                return True
            except Exception as e:
                logger.warning(f"Connection attempt {attempt+1} failed: {e}")
                time.sleep(2)
        return False

    def _ensure_connected(self) -> bool:
        if self.conn is None:
            return self._connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM DUMMY")
            cursor.close()
            return True
        except:
            self.conn = None
            return self._connect()

    def create_table(self) -> bool:
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            # Check existence
            cursor.execute(f"SELECT 1 FROM SYS.TABLES WHERE SCHEMA_NAME = '{self.schema}' AND TABLE_NAME = '{self.table}'")
            if cursor.fetchone():
                logger.info(f"Table {self.full_table} already exists")
                cursor.close()
                return True
            # Create table
            create_sql = f"""
                CREATE COLUMN TABLE {self.full_table} (
                    INCIDENT_ID          NVARCHAR(64) PRIMARY KEY,
                    SUBSCRIPTION_ID      NVARCHAR(64),
                    WORKFLOW_NAME        NVARCHAR(256),
                    ERROR_CODE           NVARCHAR(128),
                    ERROR_MESSAGE        NCLOB,
                    ERROR_CATEGORY       NVARCHAR(64),
                    STATUS               NVARCHAR(32),
                    RCA_ROOT_CAUSE       NCLOB,
                    FIX_STRATEGY         NVARCHAR(256),
                    CREATED_AT           TIMESTAMP,
                    UPDATED_AT           TIMESTAMP,
                    AUTO_FIX_ATTEMPTED   BOOLEAN,
                    AUTO_FIX_SUCCESS     BOOLEAN,
                    RETRY_COUNT          SMALLINT
                )
            """
            cursor.execute(create_sql)
            self.conn.commit()
            logger.info(f"✓ Created table {self.full_table}")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Table creation failed: {e}")
            return False

    def batch_upsert(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records or not self._ensure_connected():
            return 0, len(records)

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        inserted = 0
        failed = 0

        for rec in records:
            try:
                sql = f"""
                    INSERT INTO {self.full_table} (
                        INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME,
                        ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                        STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                        CREATED_AT, UPDATED_AT,
                        AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    rec.get("incident_id"),
                    rec.get("subscription_id"),
                    rec.get("workflow_name"),
                    rec.get("error_code", "unknown"),
                    (rec.get("error_message") or "")[:2000],
                    rec.get("error_category", "UNKNOWN_ERROR"),
                    rec.get("status", "Ticket Created"),
                    (rec.get("rca_root_cause") or "")[:4000] if rec.get("rca_root_cause") else None,
                    (rec.get("fix_strategy") or "")[:256] if rec.get("fix_strategy") else None,
                    now,
                    now,
                    rec.get("auto_fix_attempted", False),
                    rec.get("auto_fix_success", False),
                    rec.get("retry_count", 0)
                ))
                inserted += 1
            except dbapi.IntegrityError:
                # Primary key violation – update
                update_sql = f"""
                    UPDATE {self.full_table}
                    SET SUBSCRIPTION_ID = ?,
                        WORKFLOW_NAME = ?,
                        ERROR_CODE = ?,
                        ERROR_MESSAGE = ?,
                        ERROR_CATEGORY = ?,
                        STATUS = ?,
                        RCA_ROOT_CAUSE = ?,
                        FIX_STRATEGY = ?,
                        UPDATED_AT = ?,
                        AUTO_FIX_ATTEMPTED = ?,
                        AUTO_FIX_SUCCESS = ?,
                        RETRY_COUNT = ?
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(update_sql, (
                    rec.get("subscription_id"),
                    rec.get("workflow_name"),
                    rec.get("error_code", "unknown"),
                    (rec.get("error_message") or "")[:2000],
                    rec.get("error_category", "UNKNOWN_ERROR"),
                    rec.get("status", "Ticket Created"),
                    (rec.get("rca_root_cause") or "")[:4000] if rec.get("rca_root_cause") else None,
                    (rec.get("fix_strategy") or "")[:256] if rec.get("fix_strategy") else None,
                    now,
                    rec.get("auto_fix_attempted", False),
                    rec.get("auto_fix_success", False),
                    rec.get("retry_count", 0),
                    rec.get("incident_id")
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"Failed to process {rec.get('incident_id')}: {e}")
                failed += 1

        self.conn.commit()
        cursor.close()
        logger.info(f"Processed {len(records)} records: {inserted} inserted/updated, {failed} failed")
        return inserted, failed

    def get_dashboard_stats(self, start_date: datetime, end_date: datetime, subscription_id: Optional[str] = None) -> Dict[str, Any]:
        if not self._ensure_connected():
            return {"error": "HANA connection failed"}

        where = [f"UPDATED_AT >= '{start_date.isoformat()}'", f"UPDATED_AT <= '{end_date.isoformat()}'"]
        if subscription_id:
            where.append(f"SUBSCRIPTION_ID = '{subscription_id}'")
        where_clause = " AND ".join(where)

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.full_table} WHERE {where_clause}")
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT 
                    SUM(CASE WHEN AUTO_FIX_ATTEMPTED = TRUE THEN 1 ELSE 0 END),
                    SUM(CASE WHEN AUTO_FIX_SUCCESS = TRUE THEN 1 ELSE 0 END)
                FROM {self.full_table} WHERE {where_clause}
            """)
            attempted, succeeded = cursor.fetchone()
            attempted = attempted or 0
            succeeded = succeeded or 0
            auto_fix_rate = (succeeded / attempted * 100) if attempted > 0 else 0

            cursor.execute(f"""
                SELECT ERROR_CATEGORY, COUNT(*) FROM {self.full_table}
                WHERE {where_clause} GROUP BY ERROR_CATEGORY ORDER BY 2 DESC
            """)
            error_dist = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute(f"""
                SELECT STATUS, COUNT(*) FROM {self.full_table}
                WHERE {where_clause} GROUP BY STATUS ORDER BY 2 DESC
            """)
            status_dist = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute(f"""
                SELECT WORKFLOW_NAME, COUNT(*) FROM {self.full_table}
                WHERE {where_clause} GROUP BY WORKFLOW_NAME ORDER BY 2 DESC LIMIT 10
            """)
            top_workflows = [(row[0], row[1]) for row in cursor.fetchall()]

            cursor.close()
            return {
                "total_incidents": total,
                "auto_fix_attempted": attempted,
                "auto_fix_succeeded": succeeded,
                "auto_fix_rate_percent": round(auto_fix_rate, 2),
                "error_distribution": error_dist,
                "status_distribution": status_dist,
                "top_failing_workflows": top_workflows,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        except Exception as e:
            logger.error(f"Dashboard stats error: {e}")
            return {"error": str(e)}

    def close(self):
        if self.conn:
            self.conn.close()


def get_hana_client(settings):
    if not HDBCLI_AVAILABLE:
        return None
    host = getattr(settings, 'hana_host', None) or os.getenv('HANA_HOST')
    port = getattr(settings, 'hana_port', 443) or int(os.getenv('HANA_PORT', 443))
    user = getattr(settings, 'hana_user', None) or os.getenv('HANA_USER')
    pwd = getattr(settings, 'hana_password', None) or os.getenv('HANA_PASSWORD')
    schema = getattr(settings, 'hana_schema', None) or os.getenv('HANA_SCHEMA')
    if not all([host, user, pwd, schema]):
        logger.warning("HANA credentials missing")
        return None
    # Use observability‑specific setting, fallback to env var or default
    table = getattr(settings, 'hana_observability_table', None) or os.getenv('HANA_OBSERVABILITY_TABLE', 'LOGIC_APPS_OBSERVABILITY')
    return HANAObservabilityClient(host, port, user, pwd, schema, table)
    """Factory function to create HANA client from settings object."""
    if not HDBCLI_AVAILABLE:
        return None
    host = getattr(settings, 'hana_host', None) or os.getenv('HANA_HOST')
    port = getattr(settings, 'hana_port', 443) or int(os.getenv('HANA_PORT', 443))
    user = getattr(settings, 'hana_user', None) or os.getenv('HANA_USER')
    pwd = getattr(settings, 'hana_password', None) or os.getenv('HANA_PASSWORD')
    schema = getattr(settings, 'hana_schema', None) or os.getenv('HANA_SCHEMA')
    if not all([host, user, pwd, schema]):
        logger.warning("HANA credentials missing")
        return None
    table = getattr(settings, 'hana_table', None) or os.getenv('HANA_TABLE', 'LOGIC_APPS_OBSERVABILITY')
    return HANAObservabilityClient(host, port, user, pwd, schema, table)