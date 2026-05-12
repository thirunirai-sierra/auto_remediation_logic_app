"""
HANA Observability Module - Direct INSERT/UPDATE using fully qualified table name.
"""
import os
import logging
import time
from datetime import datetime
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
                    timeout=30,
                )
                logger.info(f"Connected to HANA {self.host}:{self.port}")
                return True
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
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
        except Exception:
            self.conn = None
            return self._connect()

    def migrate_table(self) -> bool:
        new_columns = [
            ("AI_DIAGNOSIS", "NCLOB"),
            ("AI_PROPOSED_FIX", "NCLOB"),
            ("AI_CONFIDENCE", "DOUBLE"),
            ("AI_FIX_PATCH", "NCLOB"),
            ("FIELD_CHANGES", "NCLOB"),
            ("HISTORY_ENTRIES", "NCLOB"),
            ("PROPERTIES_JSON", "NCLOB"),
            ("ARTIFACT_JSON", "NCLOB"),
            ("ERROR_DETAILS_JSON", "NCLOB"),
        ]
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            for col_name, col_type in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE {self.full_table} ADD ({col_name} {col_type})")
                    logger.info(f"Added column {col_name} to {self.full_table}")
                except Exception as e:
                    error_msg = str(e).lower()
                    if "already exists" in error_msg or "duplicate column" in error_msg:
                        logger.debug(f"Column {col_name} already exists, skipping")
                    else:
                        logger.warning(f"Could not add column {col_name}: {e}")
            self.conn.commit()
            logger.info("Table migration completed")
            return True
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
        finally:
            cursor.close()

    def create_table(self) -> bool:
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT 1 FROM {self.full_table} LIMIT 1")
            exists = True
        except Exception:
            exists = False
        finally:
            cursor.close()

        if exists:
            logger.info(f"Table {self.full_table} already exists")
            self.migrate_table()
            return True

        cursor = self.conn.cursor()
        try:
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
                    RETRY_COUNT          SMALLINT,
                    AI_DIAGNOSIS         NCLOB,
                    AI_PROPOSED_FIX      NCLOB,
                    AI_CONFIDENCE        DOUBLE,
                    AI_FIX_PATCH         NCLOB,
                    FIELD_CHANGES        NCLOB,
                    HISTORY_ENTRIES      NCLOB,
                    PROPERTIES_JSON      NCLOB,
                    ARTIFACT_JSON        NCLOB,
                    ERROR_DETAILS_JSON   NCLOB
                )
            """
            cursor.execute(create_sql)
            self.conn.commit()
            logger.info(f"Created table {self.full_table}")
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
                        AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                        AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                        AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                        PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(
                    sql,
                    (
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
                        rec.get("retry_count", 0),
                        rec.get("ai_diagnosis"),
                        rec.get("ai_proposed_fix"),
                        rec.get("ai_confidence"),
                        rec.get("ai_fix_patch"),
                        rec.get("field_changes"),
                        rec.get("history_entries"),
                        rec.get("properties_json"),
                        rec.get("artifact_json"),
                        rec.get("error_details_json"),
                    ),
                )
                inserted += 1
            except dbapi.IntegrityError:
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
                        RETRY_COUNT = ?,
                        AI_DIAGNOSIS = COALESCE(AI_DIAGNOSIS, ?),
                        AI_PROPOSED_FIX = COALESCE(AI_PROPOSED_FIX, ?),
                        AI_CONFIDENCE = COALESCE(AI_CONFIDENCE, ?),
                        AI_FIX_PATCH = COALESCE(AI_FIX_PATCH, ?),
                        FIELD_CHANGES = COALESCE(FIELD_CHANGES, ?),
                        HISTORY_ENTRIES = COALESCE(HISTORY_ENTRIES, ?),
                        PROPERTIES_JSON = COALESCE(PROPERTIES_JSON, ?),
                        ARTIFACT_JSON = COALESCE(ARTIFACT_JSON, ?),
                        ERROR_DETAILS_JSON = COALESCE(ERROR_DETAILS_JSON, ?)
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(
                    update_sql,
                    (
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
                        rec.get("ai_diagnosis"),
                        rec.get("ai_proposed_fix"),
                        rec.get("ai_confidence"),
                        rec.get("ai_fix_patch"),
                        rec.get("field_changes"),
                        rec.get("history_entries"),
                        rec.get("properties_json"),
                        rec.get("artifact_json"),
                        rec.get("error_details_json"),
                        rec.get("incident_id"),
                    ),
                )
                inserted += 1
            except Exception as e:
                logger.error(f"Failed to process {rec.get('incident_id')}: {e}")
                failed += 1

        self.conn.commit()
        cursor.close()
        logger.info(f"Processed {len(records)} records: {inserted} inserted/updated, {failed} failed")
        return inserted, failed

    def close(self):
        if self.conn:
            self.conn.close()


_global_client = None


def get_global_client():
    global _global_client
    if _global_client is not None:
        if not _global_client._ensure_connected():
            _global_client._connect()
        return _global_client

    if not HDBCLI_AVAILABLE:
        logger.error("hdbcli not available")
        return None

    from config import get_settings

    settings = get_settings()
    host = getattr(settings, "hana_host", None) or os.getenv("HANA_HOST")
    port = getattr(settings, "hana_port", 443) or int(os.getenv("HANA_PORT", 443))
    user = getattr(settings, "hana_user", None) or os.getenv("HANA_USER")
    pwd = getattr(settings, "hana_password", None) or os.getenv("HANA_PASSWORD")
    schema = getattr(settings, "hana_schema", None) or os.getenv("HANA_SCHEMA")
    if not all([host, user, pwd, schema]):
        logger.error("HANA credentials missing - cannot create client")
        return None

    table = getattr(settings, "hana_observability_table", None) or os.getenv("HANA_OBSERVABILITY_TABLE", "LOGIC_APPS_OBSERVABILITY")
    _global_client = HANAObservabilityClient(host, port, user, pwd, schema, table)
    _global_client.create_table()
    logger.info("Singleton HANA client created and table ensured")
    return _global_client
