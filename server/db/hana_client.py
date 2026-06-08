# server/db/hana_client.py
"""
Unified SAP HANA database client for observability and knowledge base operations.

This module provides:
- Connection management with automatic retry and reconnection
- Observability incident storage and analytics
- Knowledge base storage with vector embedding support
- Batch insert/update operations
- Dashboard aggregation queries
- Singleton client management for application-wide reuse

Key behaviour for INCIDENT_ID:
    Every unique RUN_ID is mapped to exactly one INCIDENT_ID
    (format: ORBLOGICAPPS-YYYYMMDD-XXXXXX).  The mapping is persisted in
    a dedicated table (RUN_INCIDENT_MAP) so the same RUN_ID always resolves
    to the same INCIDENT_ID, even across restarts.

Classes:
    HanaClient: Main database client for SAP HANA operations.

Functions:
    get_global_client: Returns a singleton HanaClient instance.
    get_hana_client: Deprecated compatibility wrapper.
"""
import json
import os
import re as _re
import time
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from hdbcli import dbapi
from config import get_settings

logger = logging.getLogger(__name__)

# Global singleton instance
_global_client = None


def _b(value) -> int:
    """
    Convert a truthy value to an integer 0/1 for HANA INTEGER/BOOLEAN columns.
    Using explicit 0/1 avoids 'Invalid parameter' errors with hdbcli.
    """
    return 1 if value else 0


def _to_str(value) -> Optional[str]:
    """
    Coerce a value to str for HANA NCLOB / NVARCHAR columns.

    HANA's hdbcli rejects dict, list, float, etc. for string columns — they
    must be None or a plain str.  This is the most common source of the
    'Invalid parameter [N]' error when the analyze endpoint passes structured
    objects (ai_fix_patch, field_changes, etc.) that came from JSON parsing.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    # dict / list / anything else → serialize to JSON string
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def _to_float(value) -> Optional[float]:
    """
    Coerce a value to float for HANA DOUBLE / DECIMAL columns.

    Handles strings like "0.85" that come from JSON-parsed AI responses,
    and clamps ai_confidence to [0.0, 1.0] so DECIMAL(5,4) never overflows.
    Returns None on failure so the column stays NULL rather than erroring.
    """
    if value is None:
        return None
    try:
        f = float(value)
        # Clamp to valid DECIMAL(5,4) confidence range
        return max(0.0, min(1.0, f))
    except (TypeError, ValueError):
        return None


class HanaClient:
    """
    Unified SAP HANA client for observability and knowledge base operations.

    Features:
        - Automatic connection retry and validation
        - RUN_ID → INCIDENT_ID mapping (one stable incident per run)
        - Observability incident tracking
        - AI-assisted diagnostics storage
        - Knowledge base chunk storage
        - Vector embedding management
        - Similarity search using cosine similarity
        - Dashboard analytics queries

    Attributes:
        host (str): HANA database host.
        port (int): HANA database port.
        user (str): Database username.
        password (str): Database password.
        schema (str): Active HANA schema.
        table (str): Observability table name.
        full_table (str): Fully-qualified observability table reference.
        map_table (str): Fully-qualified RUN_INCIDENT_MAP table reference.
        conn (Optional[dbapi.Connection]): Active database connection.
    """

    # Raw Azure / CPI run IDs are long purely alphanumeric strings that end
    # in "CU<digits>" – e.g. "08584217294698359236451708384CU12".
    _RAW_RUN_ID_PATTERN = _re.compile(r'^[0-9A-Za-z]{20,}CU\d+$')

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ):
        settings = get_settings()
        self.host = host or settings.HANA_HOST
        self.port = port or settings.HANA_PORT
        self.user = user or settings.HANA_USER
        self.password = password or settings.HANA_PASSWORD
        self.schema = schema or settings.HANA_SCHEMA
        self.table = table or settings.HANA_OBSERVABILITY_TABLE
        self.full_table = f'"{self.schema}"."{self.table}"'
        self.map_table = f'"{self.schema}"."RUN_INCIDENT_MAP"'
        self.conn: Optional[dbapi.Connection] = None
        self._connect()

    # ------------------------------------------------------------------ #
    #  Connection Management                                               #
    # ------------------------------------------------------------------ #

    def _connect(self) -> bool:
        if not self.host:
            logger.error("Cannot connect: HANA_HOST missing")
            return False

        for attempt in range(3):
            try:
                logger.info(
                    "Connecting to HANA %s:%s as %s (attempt %d/3)",
                    self.host, self.port, self.user, attempt + 1,
                )
                self.conn = dbapi.connect(
                    address=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    encrypt=True,
                    sslValidateCertificate=False,
                    timeout=30,
                )
                cursor = self.conn.cursor()
                cursor.execute(f'SET SCHEMA "{self.schema}"')
                cursor.close()
                logger.info("Connected to HANA %s:%s", self.host, self.port)
                return True
            except Exception as e:
                logger.error("HANA connection attempt %d failed: %s", attempt + 1, e)
                time.sleep(2)
        logger.error("All HANA connection attempts failed")
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

    # ------------------------------------------------------------------ #
    #  RUN_INCIDENT_MAP Table                                             #
    # ------------------------------------------------------------------ #

    def create_run_incident_map_table(self) -> bool:
        if not self._ensure_connected():
            logger.error("Cannot create RUN_INCIDENT_MAP table: no connection")
            return False

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT 1 FROM {self.map_table} LIMIT 1")
            logger.debug("RUN_INCIDENT_MAP table already exists")
        except Exception:
            try:
                cursor.execute(f"""
                    CREATE COLUMN TABLE {self.map_table} (
                        RUN_ID      NVARCHAR(512) PRIMARY KEY,
                        INCIDENT_ID NVARCHAR(64)  NOT NULL,
                        CREATED_AT  NVARCHAR(64)
                    )
                """)
                self.conn.commit()
                logger.info("Created RUN_INCIDENT_MAP table %s", self.map_table)
            except Exception as e:
                logger.error("Failed to create RUN_INCIDENT_MAP table: %s", e)
                self.conn.rollback()
                cursor.close()
                return False
        cursor.close()
        return True

    # ------------------------------------------------------------------ #
    #  Incident ID Generator                                               #
    # ------------------------------------------------------------------ #

    def _get_next_incident_id(self) -> str:
        today = datetime.now().strftime("%Y%m%d")
        prefix = f"ORBLOGICAPPS-{today}-"
        suffix_start = len(prefix) + 1   # SUBSTRING is 1-based in HANA

        cursor = self.conn.cursor()
        max_num = 0

        try:
            query = f"""
                SELECT MAX(CAST(SUBSTRING(INCIDENT_ID, {suffix_start}) AS BIGINT))
                FROM {self.full_table}
                WHERE INCIDENT_ID LIKE '{prefix}%'
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row[0] is not None:
                max_num = max(max_num, int(row[0]))
        except Exception:
            pass

        try:
            query = f"""
                SELECT MAX(CAST(SUBSTRING(INCIDENT_ID, {suffix_start}) AS BIGINT))
                FROM {self.map_table}
                WHERE INCIDENT_ID LIKE '{prefix}%'
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row[0] is not None:
                max_num = max(max_num, int(row[0]))
        except Exception:
            pass

        cursor.close()
        return f"{prefix}{(max_num + 1):06d}"

    def _normalize_record(self, record: Dict[str, Any]) -> None:
        """
        Detect and fix records where the upstream code placed the raw run ID
        into ``incident_id`` instead of ``run_id``.
        """
        raw_incident_id = record.get("incident_id") or record.get("INCIDENT_ID") or ""
        raw_run_id      = record.get("run_id")      or record.get("RUN_ID")      or ""

        if raw_incident_id and not raw_run_id:
            if self._RAW_RUN_ID_PATTERN.match(str(raw_incident_id)):
                logger.warning(
                    "incident_id '%s' looks like a raw run ID – moving it to run_id "
                    "and clearing incident_id so a proper ORBLOGICAPPS ID is generated.",
                    raw_incident_id,
                )
                record["run_id"]      = raw_incident_id
                record["incident_id"] = None

    def get_or_create_incident_id(self, run_id: str) -> str:
        if not run_id:
            return self._get_next_incident_id()

        if not self._ensure_connected():
            logger.error("Cannot resolve INCIDENT_ID: no HANA connection")
            return self._get_next_incident_id()

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"SELECT INCIDENT_ID FROM {self.map_table} WHERE RUN_ID = ?",
                (run_id,),
            )
            row = cursor.fetchone()
            if row:
                existing_id = row[0]
                logger.debug(
                    "RUN_ID %s already mapped to INCIDENT_ID %s", run_id, existing_id
                )
                return existing_id

            new_incident_id = self._get_next_incident_id()
            now = datetime.now().isoformat()
            cursor.execute(
                f"INSERT INTO {self.map_table} (RUN_ID, INCIDENT_ID, CREATED_AT) VALUES (?, ?, ?)",
                (run_id, new_incident_id, now),
            )
            self.conn.commit()
            logger.info(
                "Mapped new RUN_ID %s → INCIDENT_ID %s", run_id, new_incident_id
            )
            return new_incident_id

        except Exception as e:
            logger.error(
                "Failed to get/create INCIDENT_ID for RUN_ID %s: %s", run_id, e
            )
            if self.conn:
                self.conn.rollback()
            return self._get_next_incident_id()
        finally:
            cursor.close()

    # ------------------------------------------------------------------ #
    #  Observability Table Methods                                         #
    # ------------------------------------------------------------------ #

    def create_observability_table(self) -> bool:
        if not self._ensure_connected():
            logger.error("Cannot create observability table: no connection")
            return False

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT 1 FROM {self.full_table} LIMIT 1")
            exists = True
        except Exception:
            exists = False

        if exists:
            new_columns = [
                ("RESOURCE_GROUP",          "NVARCHAR(128)"),
                ("RUN_ID",                  "NVARCHAR(512)"),
                ("RESOURCE_ID",             "NVARCHAR(1000)"),
                ("EVENT_TIME",              "NVARCHAR(64)"),
                ("INGESTED_AT",             "NVARCHAR(64)"),
                ("ERROR_TYPE",              "NVARCHAR(128)"),
                ("AI_DIAGNOSIS",            "NCLOB"),
                ("AI_PROPOSED_FIX",         "NCLOB"),
                ("AI_CONFIDENCE",           "DOUBLE"),
                ("AI_FIX_PATCH",            "NCLOB"),
                ("FIELD_CHANGES",           "NCLOB"),
                ("HISTORY_ENTRIES",         "NCLOB"),
                ("PROPERTIES_JSON",         "NCLOB"),
                ("ARTIFACT_JSON",           "NCLOB"),
                ("ERROR_DETAILS_JSON",      "NCLOB"),
                ("MESSAGE_GUID",            "NVARCHAR(200)"),
                ("IFLOW_ID",                "NVARCHAR(500)"),
                ("SENDER",                  "NVARCHAR(200)"),
                ("RECEIVER",                "NVARCHAR(200)"),
                ("ROOT_CAUSE",              "NCLOB MEMORY THRESHOLD 1000"),
                ("PROPOSED_FIX",            "NCLOB MEMORY THRESHOLD 1000"),
                ("RCA_CONFIDENCE",          "DECIMAL(5,4)"),
                ("AFFECTED_COMPONENT",      "NVARCHAR(200)"),
                ("FIX_SUMMARY",             "NCLOB MEMORY THRESHOLD 1000"),
                ("COMMENT",                 "NCLOB MEMORY THRESHOLD 1000"),
                ("CORRELATION_ID",          "NVARCHAR(200)"),
                ("LOG_START",               "NVARCHAR(64)"),
                ("LOG_END",                 "NVARCHAR(64)"),
                ("RESOLVED_AT",             "NVARCHAR(64)"),
                ("TAGS",                    "NCLOB MEMORY THRESHOLD 1000"),
                ("INCIDENT_GROUP_KEY",      "NVARCHAR(64)"),
                ("OCCURRENCE_COUNT",        "INTEGER DEFAULT 1"),
                ("LAST_SEEN",               "NVARCHAR(64)"),
                ("VERIFICATION_STATUS",     "NVARCHAR(64)"),
                ("SOURCE_TYPE",             "NVARCHAR(64)"),
                ("FIX_STEPS",               "NCLOB MEMORY THRESHOLD 1000"),
                ("FIX_PLAN_GENERATED_AT",   "NVARCHAR(64)"),
                ("LAST_FAILED_STAGE",       "NVARCHAR(64)"),
                ("IFLOW_SNAPSHOT_BEFORE",   "NCLOB MEMORY THRESHOLD 1000"),
                ("PENDING_SINCE",           "NVARCHAR(64)"),
                ("TICKET_ID",               "NVARCHAR(512)"),
                ("CONSECUTIVE_FAILURES",    "INTEGER DEFAULT 0"),
                ("AUTO_ESCALATED",          "INTEGER DEFAULT 0"),
                ("INTEGRATION_FLOW_NAME",   "NVARCHAR(500)"),
                ("ARTIFACT_ID",             "NVARCHAR(500)"),
                ("DESIGNTIME_ARTIFACT_ID",  "NVARCHAR(500)"),
                ("PROPERTY_TO_CHANGE",      "NVARCHAR(500)"),
                ("CURRENT_VALUE",           "NCLOB MEMORY THRESHOLD 1000"),
                ("CORRECT_VALUE",           "NCLOB MEMORY THRESHOLD 1000"),
                ("RCA_FIXES_JSON",          "NCLOB MEMORY THRESHOLD 1000"),
            ]
            for col_name, col_type in new_columns:
                try:
                    cursor.execute(
                        f"ALTER TABLE {self.full_table} ADD ({col_name} {col_type})"
                    )
                    logger.info("Added column %s to %s", col_name, self.full_table)
                except Exception as e:
                    if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                        continue
                    logger.warning("Could not add column %s: %s", col_name, e)
            self.conn.commit()
            cursor.close()
            return True

        create_sql = f"""
            CREATE COLUMN TABLE {self.full_table} (
                INCIDENT_ID             NVARCHAR(64)   PRIMARY KEY,
                RUN_ID                  NVARCHAR(512),
                SUBSCRIPTION_ID         NVARCHAR(64),
                RESOURCE_GROUP          NVARCHAR(128),
                WORKFLOW_NAME           NVARCHAR(256),
                ERROR_CODE              NVARCHAR(128),
                ERROR_MESSAGE           NCLOB,
                ERROR_CATEGORY          NVARCHAR(64),
                STATUS                  NVARCHAR(32),
                RCA_ROOT_CAUSE          NCLOB,
                FIX_STRATEGY            NVARCHAR(256),
                CREATED_AT              TIMESTAMP,
                UPDATED_AT              TIMESTAMP,
                AUTO_FIX_ATTEMPTED      INTEGER DEFAULT 0,
                AUTO_FIX_SUCCESS        INTEGER DEFAULT 0,
                RETRY_COUNT             SMALLINT,
                RESOURCE_ID             NVARCHAR(1000),
                EVENT_TIME              NVARCHAR(64),
                INGESTED_AT             NVARCHAR(64),
                ERROR_TYPE              NVARCHAR(128),
                AI_DIAGNOSIS            NCLOB,
                AI_PROPOSED_FIX         NCLOB,
                AI_CONFIDENCE           DOUBLE,
                AI_FIX_PATCH            NCLOB,
                FIELD_CHANGES           NCLOB,
                HISTORY_ENTRIES         NCLOB,
                PROPERTIES_JSON         NCLOB,
                ARTIFACT_JSON           NCLOB,
                ERROR_DETAILS_JSON      NCLOB,
                MESSAGE_GUID            NVARCHAR(200),
                IFLOW_ID                NVARCHAR(500),
                SENDER                  NVARCHAR(200),
                RECEIVER                NVARCHAR(200),
                ROOT_CAUSE              NCLOB MEMORY THRESHOLD 1000,
                PROPOSED_FIX            NCLOB MEMORY THRESHOLD 1000,
                RCA_CONFIDENCE          DECIMAL(5,4),
                AFFECTED_COMPONENT      NVARCHAR(200),
                FIX_SUMMARY             NCLOB MEMORY THRESHOLD 1000,
                COMMENT                 NCLOB MEMORY THRESHOLD 1000,
                CORRELATION_ID          NVARCHAR(200),
                LOG_START               NVARCHAR(64),
                LOG_END                 NVARCHAR(64),
                RESOLVED_AT             NVARCHAR(64),
                TAGS                    NCLOB MEMORY THRESHOLD 1000,
                INCIDENT_GROUP_KEY      NVARCHAR(64),
                OCCURRENCE_COUNT        INTEGER DEFAULT 1,
                LAST_SEEN               NVARCHAR(64),
                VERIFICATION_STATUS     NVARCHAR(64),
                SOURCE_TYPE             NVARCHAR(64),
                FIX_STEPS               NCLOB MEMORY THRESHOLD 1000,
                FIX_PLAN_GENERATED_AT   NVARCHAR(64),
                LAST_FAILED_STAGE       NVARCHAR(64),
                IFLOW_SNAPSHOT_BEFORE   NCLOB MEMORY THRESHOLD 1000,
                PENDING_SINCE           NVARCHAR(64),
                TICKET_ID               NVARCHAR(512),
                CONSECUTIVE_FAILURES    INTEGER DEFAULT 0,
                AUTO_ESCALATED          INTEGER DEFAULT 0,
                INTEGRATION_FLOW_NAME   NVARCHAR(500),
                ARTIFACT_ID             NVARCHAR(500),
                DESIGNTIME_ARTIFACT_ID  NVARCHAR(500),
                PROPERTY_TO_CHANGE      NVARCHAR(500),
                CURRENT_VALUE           NCLOB MEMORY THRESHOLD 1000,
                CORRECT_VALUE           NCLOB MEMORY THRESHOLD 1000,
                RCA_FIXES_JSON          NCLOB MEMORY THRESHOLD 1000
            )
        """
        cursor.execute(create_sql)
        self.conn.commit()
        cursor.close()
        logger.info("Created observability table %s", self.full_table)
        return True

    def upsert_observability_record(self, record: Dict[str, Any]) -> bool:
        """
        Insert or update a single observability incident record.

        INCIDENT_ID resolution rules
        ----------------------------
        1. If the record supplies a ``run_id``, ``get_or_create_incident_id``
           is called to look up (or mint) the matching INCIDENT_ID.
        2. If no ``run_id`` is present but an ``incident_id`` is supplied,
           that value is used as-is (legacy path).
        3. If neither is present, a brand-new INCIDENT_ID is generated.

        Args:
            record (Dict[str, Any]): Incident data.

        Returns:
            bool: True if the operation succeeds, False otherwise.
        """
        if not self._ensure_connected():
            return False

        self._normalize_record(record)

        run_id = record.get("run_id") or record.get("RUN_ID")
        if run_id:
            incident_id = self.get_or_create_incident_id(run_id)
        elif record.get("incident_id"):
            incident_id = record["incident_id"]
        else:
            incident_id = self._get_next_incident_id()

        record["incident_id"] = incident_id

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()

        # ------------------------------------------------------------------
        # Build parameter tuple — _b() for INTEGER/BOOL, _to_str() for
        # NCLOB/NVARCHAR, _to_float() for DOUBLE/DECIMAL.
        # This prevents 'Invalid parameter [N]' when the caller passes
        # dicts, lists, or non-string types from JSON-parsed AI responses.
        # ------------------------------------------------------------------
        insert_params = (
            incident_id,                                              # 1  INCIDENT_ID
            _to_str(run_id),                                          # 2  RUN_ID
            _to_str(record.get("subscription_id")),                   # 3  SUBSCRIPTION_ID
            _to_str(record.get("resource_group")),                    # 4  RESOURCE_GROUP
            _to_str(record.get("workflow_name")),                     # 5  WORKFLOW_NAME
            _to_str(record.get("error_code", "unknown")),             # 6  ERROR_CODE
            (_to_str(record.get("error_message")) or "")[:2000],      # 7  ERROR_MESSAGE
            _to_str(record.get("error_category", "UNKNOWN_ERROR")),   # 8  ERROR_CATEGORY
            _to_str(record.get("status") or "DETECTED"),           # 9  STATUS
            (_to_str(record.get("rca_root_cause")) or "")[:4000],     # 10 RCA_ROOT_CAUSE
            (_to_str(record.get("fix_strategy")) or "")[:256],        # 11 FIX_STRATEGY
            now,                                                      # 12 CREATED_AT
            now,                                                      # 13 UPDATED_AT
            _b(record.get("auto_fix_attempted", False)),              # 14 AUTO_FIX_ATTEMPTED
            _b(record.get("auto_fix_success", False)),                # 15 AUTO_FIX_SUCCESS
            record.get("retry_count", 0) or 0,                        # 16 RETRY_COUNT
            _to_str(record.get("resource_id")),                       # 17 RESOURCE_ID
            _to_str(record.get("event_time")),                        # 18 EVENT_TIME
            _to_str(record.get("ingested_at")),                       # 19 INGESTED_AT
            _to_str(record.get("error_type")),                        # 20 ERROR_TYPE
            _to_str(record.get("ai_diagnosis")),                      # 21 AI_DIAGNOSIS
            _to_str(record.get("ai_proposed_fix")),                   # 22 AI_PROPOSED_FIX
            _to_float(record.get("ai_confidence")),                   # 23 AI_CONFIDENCE  ← DECIMAL
            _to_str(record.get("ai_fix_patch")),                      # 24 AI_FIX_PATCH   ← was crashing
            _to_str(record.get("field_changes")),                     # 25 FIELD_CHANGES
            _to_str(record.get("history_entries")),                   # 26 HISTORY_ENTRIES
            _to_str(record.get("properties_json")),                   # 27 PROPERTIES_JSON
            _to_str(record.get("artifact_json")),                     # 28 ARTIFACT_JSON
            _to_str(record.get("error_details_json")),                # 29 ERROR_DETAILS_JSON
        )

        try:
            insert_sql = f"""
                INSERT INTO {self.full_table} (
                    INCIDENT_ID, RUN_ID, SUBSCRIPTION_ID, RESOURCE_GROUP, WORKFLOW_NAME,
                    ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                    STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                    CREATED_AT, UPDATED_AT,
                    AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                    RESOURCE_ID, EVENT_TIME, INGESTED_AT, ERROR_TYPE,
                    AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                    AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                    PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?
                )
            """
            cursor.execute(insert_sql, insert_params)
            self.conn.commit()
            logger.info("Inserted new observability record %s", incident_id)
            return True

        except dbapi.IntegrityError:
            # Record already exists — perform an UPDATE instead.
            # Parameter order must exactly match the SET clause below.
            _status_val = _to_str(record.get("status"))
            update_params = (
                _to_str(run_id),                                           # 1  RUN_ID (COALESCE)
                _to_str(record.get("subscription_id")),                    # 2  SUBSCRIPTION_ID
                _to_str(record.get("resource_group")),                     # 3  RESOURCE_GROUP
                _to_str(record.get("workflow_name")),                      # 4  WORKFLOW_NAME
                _to_str(record.get("error_code", "unknown")),              # 5  ERROR_CODE
                (_to_str(record.get("error_message")) or "")[:2000],       # 6  ERROR_MESSAGE
                _to_str(record.get("error_category", "UNKNOWN_ERROR")),    # 7  ERROR_CATEGORY
                _status_val,                                               # 8  STATUS CASE WHEN ? IS NOT NULL
                _status_val,       # 8  STATUS
                (_to_str(record.get("rca_root_cause")) or "")[:4000],      # 9  RCA_ROOT_CAUSE
                (_to_str(record.get("fix_strategy")) or "")[:256],         # 10 FIX_STRATEGY
                now,                                                       # 11 UPDATED_AT
                _b(record.get("auto_fix_attempted", False)),               # 12 AUTO_FIX_ATTEMPTED
                _b(record.get("auto_fix_success", False)),                 # 13 AUTO_FIX_SUCCESS
                record.get("retry_count", 0) or 0,                         # 14 RETRY_COUNT
                _to_str(record.get("resource_id")),                        # 15 RESOURCE_ID (COALESCE)
                _to_str(record.get("event_time")),                         # 16 EVENT_TIME (COALESCE)
                _to_str(record.get("ingested_at")),                        # 17 INGESTED_AT (COALESCE)
                _to_str(record.get("error_type")),                         # 18 ERROR_TYPE (COALESCE)
                _to_str(record.get("ai_diagnosis")),                       # 19 AI_DIAGNOSIS (COALESCE)
                _to_str(record.get("ai_proposed_fix")),                    # 20 AI_PROPOSED_FIX (COALESCE)
                _to_float(record.get("ai_confidence")),                    # 21 AI_CONFIDENCE (COALESCE) ← DECIMAL
                _to_str(record.get("ai_fix_patch")),                       # 22 AI_FIX_PATCH (COALESCE)  ← was crashing
                _to_str(record.get("field_changes")),                      # 23 FIELD_CHANGES (COALESCE)
                _to_str(record.get("history_entries")),                    # 24 HISTORY_ENTRIES (COALESCE)
                _to_str(record.get("properties_json")),                    # 25 PROPERTIES_JSON (COALESCE)
                _to_str(record.get("artifact_json")),                      # 26 ARTIFACT_JSON (COALESCE)
                _to_str(record.get("error_details_json")),                 # 27 ERROR_DETAILS_JSON (COALESCE)
                incident_id,                                               # 28 WHERE INCIDENT_ID = ?
            )
            update_sql = f"""
                UPDATE {self.full_table}
                SET RUN_ID              = COALESCE(RUN_ID, ?),
                    SUBSCRIPTION_ID     = COALESCE(?, SUBSCRIPTION_ID),
                    RESOURCE_GROUP      = COALESCE(?, RESOURCE_GROUP),
                    WORKFLOW_NAME       = COALESCE(?, WORKFLOW_NAME),
                    ERROR_CODE          = ?,
                    ERROR_MESSAGE       = ?,
                    ERROR_CATEGORY      = ?,
                    STATUS              = CASE WHEN ? IS NOT NULL THEN ? ELSE STATUS END,
                    RCA_ROOT_CAUSE      = ?,
                    FIX_STRATEGY        = ?,
                    UPDATED_AT          = ?,
                    AUTO_FIX_ATTEMPTED  = ?,
                    AUTO_FIX_SUCCESS    = ?,
                    RETRY_COUNT         = ?,
                    RESOURCE_ID         = COALESCE(RESOURCE_ID, ?),
                    EVENT_TIME          = COALESCE(EVENT_TIME, ?),
                    INGESTED_AT         = COALESCE(INGESTED_AT, ?),
                    ERROR_TYPE          = COALESCE(ERROR_TYPE, ?),
                    AI_DIAGNOSIS        = COALESCE(?, AI_DIAGNOSIS),
                    AI_PROPOSED_FIX     = COALESCE(?, AI_PROPOSED_FIX),
                    AI_CONFIDENCE       = COALESCE(?, AI_CONFIDENCE),
                    AI_FIX_PATCH        = COALESCE(?, AI_FIX_PATCH),
                    FIELD_CHANGES       = COALESCE(?, FIELD_CHANGES),
                    HISTORY_ENTRIES     = COALESCE(?, HISTORY_ENTRIES),
                    PROPERTIES_JSON     = COALESCE(?, PROPERTIES_JSON),
                    ARTIFACT_JSON       = COALESCE(?, ARTIFACT_JSON),
                    ERROR_DETAILS_JSON  = COALESCE(?, ERROR_DETAILS_JSON)
                WHERE INCIDENT_ID = ?
            """
            try:
                cursor.execute(update_sql, update_params)
                self.conn.commit()
                logger.info("Updated existing observability record %s", incident_id)
                return True
            except Exception as ue:
                logger.error("Update failed for %s: %s", incident_id, ue)
                if self.conn:
                    self.conn.rollback()
                return False

        except Exception as e:
            logger.error("Upsert failed for %s: %s", incident_id, e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            cursor.close()

    def batch_upsert_observability(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert or update multiple observability records.

        Args:
            records (List[Dict[str, Any]]): List of observability incident records.

        Returns:
            Tuple[int, int]: (inserted_or_updated_count, failed_count)
        """
        if not records or not self._ensure_connected():
            return 0, len(records)

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        inserted = 0
        failed = 0

        for rec in records:
            self._normalize_record(rec)

            run_id = rec.get("run_id") or rec.get("RUN_ID")
            if run_id:
                incident_id = self.get_or_create_incident_id(run_id)
            elif rec.get("incident_id"):
                incident_id = rec["incident_id"]
            else:
                incident_id = self._get_next_incident_id()
            rec["incident_id"] = incident_id

            # ------------------------------------------------------------------
            # INSERT parameter tuple (36 values — matches 36 columns below)
            # _to_str() / _to_float() coerce all AI fields so dicts/lists
            # from JSON-parsed responses never reach HANA as wrong types.
            # ------------------------------------------------------------------
            insert_params = (
                incident_id,                                               # 1  INCIDENT_ID
                _to_str(run_id),                                           # 2  RUN_ID
                _to_str(rec.get("subscription_id")),                       # 3  SUBSCRIPTION_ID
                _to_str(rec.get("resource_group")),                        # 4  RESOURCE_GROUP
                _to_str(rec.get("workflow_name")),                         # 5  WORKFLOW_NAME
                _to_str(rec.get("error_code", "unknown")),                 # 6  ERROR_CODE
                (_to_str(rec.get("error_message")) or "")[:2000],          # 7  ERROR_MESSAGE
                _to_str(rec.get("error_category", "UNKNOWN_ERROR")),       # 8  ERROR_CATEGORY
                _to_str(rec.get("status") or "DETECTED"),              # 9  STATUS
                (_to_str(rec.get("rca_root_cause")) or "")[:4000],         # 10 RCA_ROOT_CAUSE
                (_to_str(rec.get("fix_strategy")) or "")[:256],            # 11 FIX_STRATEGY
                rec.get("created_at", now),                                # 12 CREATED_AT
                now,                                                       # 13 UPDATED_AT
                _b(rec.get("auto_fix_attempted", False)),                  # 14 AUTO_FIX_ATTEMPTED
                _b(rec.get("auto_fix_success", False)),                    # 15 AUTO_FIX_SUCCESS
                rec.get("retry_count", 0) or 0,                            # 16 RETRY_COUNT
                _to_str(rec.get("resource_id")),                           # 17 RESOURCE_ID
                _to_str(rec.get("event_time")),                            # 18 EVENT_TIME
                _to_str(rec.get("ingested_at")),                           # 19 INGESTED_AT
                _to_str(rec.get("error_type")),                            # 20 ERROR_TYPE
                _to_str(rec.get("ai_diagnosis")),                          # 21 AI_DIAGNOSIS
                _to_str(rec.get("ai_proposed_fix")),                       # 22 AI_PROPOSED_FIX
                _to_float(rec.get("ai_confidence")),                       # 23 AI_CONFIDENCE  ← DECIMAL
                _to_str(rec.get("ai_fix_patch")),                          # 24 AI_FIX_PATCH   ← was crashing
                _to_str(rec.get("field_changes")),                         # 25 FIELD_CHANGES
                _to_str(rec.get("history_entries")),                       # 26 HISTORY_ENTRIES
                _to_str(rec.get("properties_json")),                       # 27 PROPERTIES_JSON
                _to_str(rec.get("artifact_json")),                         # 28 ARTIFACT_JSON
                _to_str(rec.get("error_details_json")),                    # 29 ERROR_DETAILS_JSON
                _to_str(rec.get("log_start")),                             # 30 LOG_START
                _to_str(rec.get("last_seen")),                             # 31 LAST_SEEN
                rec.get("occurrence_count", 1) or 1,                       # 32 OCCURRENCE_COUNT
                _to_str(rec.get("affected_component")),                    # 33 AFFECTED_COMPONENT
                _to_str(rec.get("correlation_id")),                        # 34 CORRELATION_ID
                _to_str(rec.get("source_type", "AzureDiagnostics")),       # 35 SOURCE_TYPE
                _to_str(rec.get("integration_flow_name")),                 # 36 INTEGRATION_FLOW_NAME
            )

            insert_sql = f"""
                INSERT INTO {self.full_table} (
                    INCIDENT_ID, RUN_ID, SUBSCRIPTION_ID, RESOURCE_GROUP, WORKFLOW_NAME,
                    ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                    STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                    CREATED_AT, UPDATED_AT,
                    AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                    RESOURCE_ID, EVENT_TIME, INGESTED_AT, ERROR_TYPE,
                    AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                    AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                    PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON,
                    LOG_START, LAST_SEEN, OCCURRENCE_COUNT,
                    AFFECTED_COMPONENT, CORRELATION_ID, SOURCE_TYPE,
                    INTEGRATION_FLOW_NAME
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?
                )
            """

            try:
                cursor.execute(insert_sql, insert_params)
                inserted += 1

            except dbapi.IntegrityError:
                # INCIDENT_ID already exists — UPDATE and bump occurrence count.
                # Parameter order must exactly match the SET clause below.
                update_params = (
                    _to_str(run_id),                                           # 1  RUN_ID (COALESCE)
                    _to_str(rec.get("subscription_id")),                       # 2  SUBSCRIPTION_ID
                    _to_str(rec.get("resource_group")),                        # 3  RESOURCE_GROUP
                    _to_str(rec.get("workflow_name")),                         # 4  WORKFLOW_NAME
                    _to_str(rec.get("error_code", "unknown")),                 # 5  ERROR_CODE
                    (_to_str(rec.get("error_message")) or "")[:2000],          # 6  ERROR_MESSAGE
                    _to_str(rec.get("error_category", "UNKNOWN_ERROR")),       # 7  ERROR_CATEGORY
                    _to_str(rec.get("status")),    
                    _to_str(rec.get("status")), # 8  STATUS
                    (_to_str(rec.get("rca_root_cause")) or "")[:4000],         # 9  RCA_ROOT_CAUSE
                    (_to_str(rec.get("fix_strategy")) or "")[:256],            # 10 FIX_STRATEGY
                    now,                                                       # 11 UPDATED_AT
                    _b(rec.get("auto_fix_attempted", False)),                  # 12 AUTO_FIX_ATTEMPTED
                    _b(rec.get("auto_fix_success", False)),                    # 13 AUTO_FIX_SUCCESS
                    rec.get("retry_count", 0) or 0,                            # 14 RETRY_COUNT
                    _to_str(rec.get("resource_id")),                           # 15 RESOURCE_ID (COALESCE)
                    _to_str(rec.get("event_time")),                            # 16 EVENT_TIME (COALESCE)
                    _to_str(rec.get("ingested_at")),                           # 17 INGESTED_AT (COALESCE)
                    _to_str(rec.get("error_type")),                            # 18 ERROR_TYPE (COALESCE)
                    _to_str(rec.get("ai_diagnosis")),                          # 19 AI_DIAGNOSIS (COALESCE)
                    _to_str(rec.get("ai_proposed_fix")),                       # 20 AI_PROPOSED_FIX (COALESCE)
                    _to_float(rec.get("ai_confidence")),                       # 21 AI_CONFIDENCE (COALESCE) ← DECIMAL
                    _to_str(rec.get("ai_fix_patch")),                          # 22 AI_FIX_PATCH (COALESCE)  ← was crashing
                    _to_str(rec.get("field_changes")),                         # 23 FIELD_CHANGES (COALESCE)
                    _to_str(rec.get("history_entries")),                       # 24 HISTORY_ENTRIES (COALESCE)
                    _to_str(rec.get("properties_json")),                       # 25 PROPERTIES_JSON (COALESCE)
                    _to_str(rec.get("artifact_json")),                         # 26 ARTIFACT_JSON (COALESCE)
                    _to_str(rec.get("error_details_json")),                    # 27 ERROR_DETAILS_JSON (COALESCE)
                    _to_str(rec.get("log_start")),                             # 28 LOG_START (COALESCE)
                    now,                                                       # 29 LAST_SEEN (always update)
                    _to_str(rec.get("affected_component")),                    # 30 AFFECTED_COMPONENT (COALESCE)
                    _to_str(rec.get("correlation_id")),                        # 31 CORRELATION_ID (COALESCE)
                    _to_str(rec.get("source_type", "AzureDiagnostics")),       # 32 SOURCE_TYPE (COALESCE)
                    _to_str(rec.get("integration_flow_name")),                 # 33 INTEGRATION_FLOW_NAME (COALESCE)
                    incident_id,                                               # 34 WHERE INCIDENT_ID = ?
                )
                update_sql = f"""
                    UPDATE {self.full_table}
                    SET RUN_ID                = COALESCE(RUN_ID, ?),
                        SUBSCRIPTION_ID       = ?,
                        RESOURCE_GROUP        = ?,
                        WORKFLOW_NAME         = ?,
                        ERROR_CODE            = ?,
                        ERROR_MESSAGE         = ?,
                        ERROR_CATEGORY        = ?,
                        STATUS                = CASE WHEN ? IS NOT NULL THEN ? ELSE STATUS END, 
                        RCA_ROOT_CAUSE        = ?,
                        FIX_STRATEGY          = ?,
                        UPDATED_AT            = ?,
                        AUTO_FIX_ATTEMPTED    = ?,
                        AUTO_FIX_SUCCESS      = ?,
                        RETRY_COUNT           = ?,
                        RESOURCE_ID           = COALESCE(RESOURCE_ID, ?),
                        EVENT_TIME            = COALESCE(EVENT_TIME, ?),
                        INGESTED_AT           = COALESCE(INGESTED_AT, ?),
                        ERROR_TYPE            = COALESCE(ERROR_TYPE, ?),
                        AI_DIAGNOSIS          = COALESCE(AI_DIAGNOSIS, ?),
                        AI_PROPOSED_FIX       = COALESCE(AI_PROPOSED_FIX, ?),
                        AI_CONFIDENCE         = COALESCE(AI_CONFIDENCE, ?),
                        AI_FIX_PATCH          = COALESCE(AI_FIX_PATCH, ?),
                        FIELD_CHANGES         = COALESCE(FIELD_CHANGES, ?),
                        HISTORY_ENTRIES       = COALESCE(HISTORY_ENTRIES, ?),
                        PROPERTIES_JSON       = COALESCE(PROPERTIES_JSON, ?),
                        ARTIFACT_JSON         = COALESCE(ARTIFACT_JSON, ?),
                        ERROR_DETAILS_JSON    = COALESCE(ERROR_DETAILS_JSON, ?),
                        LOG_START             = COALESCE(LOG_START, ?),
                        LAST_SEEN             = ?,
                        OCCURRENCE_COUNT      = OCCURRENCE_COUNT + 1,
                        AFFECTED_COMPONENT    = COALESCE(AFFECTED_COMPONENT, ?),
                        CORRELATION_ID        = COALESCE(CORRELATION_ID, ?),
                        SOURCE_TYPE           = COALESCE(SOURCE_TYPE, ?),
                        INTEGRATION_FLOW_NAME = COALESCE(INTEGRATION_FLOW_NAME, ?)
                    WHERE INCIDENT_ID = ?
                """
                try:
                    cursor.execute(update_sql, update_params)
                    inserted += 1
                except Exception as ue:
                    logger.error("Batch update failed for %s: %s | params=%s", incident_id, ue, update_params)
                    failed += 1
                    if self.conn:
                        self.conn.rollback()
                    continue

            except Exception as e:
                logger.error("Batch insert failed for %s: %s | params=%s", incident_id, e, insert_params)
                failed += 1
                if self.conn:
                    self.conn.rollback()
                continue

        try:
            self.conn.commit()
        except Exception as ce:
            logger.error("Batch commit failed: %s", ce)

        cursor.close()
        logger.info("Batch upsert: %d inserted/updated, %d failed", inserted, failed)
        return inserted, failed

    def update_rca_record(
        self,
        incident_id: str,
        root_cause: str,
        proposed_fix: str,
        confidence: float,
        affected_component: str = None,
    ) -> bool:
        """
        Insert or update RCA data for an incident (upsert).

        Args:
            incident_id (str): The unique incident identifier.
            root_cause (str): Diagnosed root cause description.
            proposed_fix (str): Suggested remediation steps.
            confidence (float): AI confidence score (0.0 to 1.0).
            affected_component (str, optional): Component that failed.

        Returns:
            bool: True if the operation succeeded, False otherwise.
        """
        if not self._ensure_connected():
            logger.error("HANA not connected – cannot update RCA record")
            return False

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"SELECT 1 FROM {self.full_table} WHERE INCIDENT_ID = ?", (incident_id,)
            )
            exists = cursor.fetchone() is not None

            if exists:
                sql = f"""
                    UPDATE {self.full_table}
                    SET ROOT_CAUSE          = COALESCE(ROOT_CAUSE, ?),
                        PROPOSED_FIX        = COALESCE(PROPOSED_FIX, ?),
                        RCA_CONFIDENCE      = COALESCE(RCA_CONFIDENCE, ?),
                        AFFECTED_COMPONENT  = COALESCE(AFFECTED_COMPONENT, ?)
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(sql, (root_cause, proposed_fix, confidence, affected_component, incident_id))
                logger.info("Updated RCA record for %s", incident_id)
            else:
                sql = f"""
                    INSERT INTO {self.full_table} (
                        INCIDENT_ID, ROOT_CAUSE, PROPOSED_FIX, RCA_CONFIDENCE, AFFECTED_COMPONENT
                    ) VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (incident_id, root_cause, proposed_fix, confidence, affected_component))
                logger.info("Inserted new RCA record for %s", incident_id)

            self.conn.commit()
            return True
        except Exception as e:
            logger.error("Failed to upsert RCA record for %s: %s", incident_id, e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            cursor.close()

    def update_fix_result(
        self,
        incident_id: str,
        fix_summary: str,
        fix_steps: str,
        verification_status: str,
        resolved_at: str = None,
    ) -> bool:
        """Update fix-related columns after remediation."""
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            sql = f"""
                UPDATE {self.full_table}
                SET FIX_SUMMARY          = COALESCE(FIX_SUMMARY, ?),
                    FIX_STEPS            = COALESCE(FIX_STEPS, ?),
                    VERIFICATION_STATUS  = COALESCE(VERIFICATION_STATUS, ?),
                    RESOLVED_AT          = COALESCE(RESOLVED_AT, ?)
                WHERE INCIDENT_ID = ?
            """
            cursor.execute(sql, (fix_summary, fix_steps, verification_status, resolved_at, incident_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error("Failed to update fix result for %s: %s", incident_id, e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            cursor.close()

    def get_dashboard_stats(
        self,
        start_date: datetime,
        end_date: datetime,
        subscription_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve dashboard analytics for observability incidents.

        Args:
            start_date (datetime): Start of the reporting window.
            end_date (datetime): End of the reporting window.
            subscription_id (Optional[str]): Optional subscription filter.

        Returns:
            Dict[str, Any]: Dashboard statistics and metadata.
        """
        if not self._ensure_connected():
            return {"error": "HANA connection failed"}

        where = [
            f"UPDATED_AT >= '{start_date.isoformat()}'",
            f"UPDATED_AT <= '{end_date.isoformat()}'",
        ]
        if subscription_id:
            where.append(f"SUBSCRIPTION_ID = '{subscription_id}'")
        where_clause = " AND ".join(where)

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.full_table} WHERE {where_clause}")
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT
                    SUM(CASE WHEN AUTO_FIX_ATTEMPTED = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN AUTO_FIX_SUCCESS   = 1 THEN 1 ELSE 0 END)
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
                "end_date": end_date.isoformat(),
            }
        except Exception as e:
            logger.error("Dashboard stats error: %s", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------ #
    #  Knowledge Base Table Methods                                        #
    # ------------------------------------------------------------------ #

    def create_knowledge_table(self, drop_first: bool = False) -> None:
        """
        Create the vector-enabled knowledge base table.

        Args:
            drop_first (bool): If True, drops the table before creation (dev only).
        """
        if not self._ensure_connected():
            raise RuntimeError("Cannot create table: no HANA connection")

        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        if drop_first:
            if os.getenv("ENV", "production") == "development":
                try:
                    cur.execute(f"DROP TABLE {knowledge_table}")
                    self.conn.commit()
                except Exception:
                    self.conn.rollback()
            else:
                logger.warning("drop_first=True ignored in non-development environment")
        cur.execute(
            f"""
            CREATE COLUMN TABLE {knowledge_table} (
                ID         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                VEC_TEXT   NCLOB,
                VEC_META   NCLOB,
                VEC_VECTOR REAL_VECTOR({settings.VECTOR_DIMENSION})
            )
        """
        )
        self.conn.commit()
        cur.close()
        logger.info("Created knowledge table %s", knowledge_table)

    def get_existing_urls(self) -> set:
        """Retrieve all URLs already stored in the knowledge base."""
        if not self._ensure_connected():
            return set()
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        urls = set()
        try:
            cur.execute(f"SELECT VEC_META FROM {knowledge_table}")
            for (meta_str,) in cur.fetchall():
                try:
                    meta = json.loads(meta_str or "{}")
                    if meta.get("url"):
                        urls.add(meta["url"])
                except Exception:
                    pass
        except Exception as e:
            logger.warning("Failed to get existing URLs: %s", e)
        cur.close()
        return urls

    def insert_knowledge_chunks(self, chunks: List[Dict[str, Any]]) -> int:
        """
        Insert knowledge base text chunks into the database.

        Args:
            chunks (List[Dict[str, Any]]): List of chunk dicts with ``text`` and ``meta``.

        Returns:
            int: Number of successfully inserted chunks.
        """
        if not chunks or not self._ensure_connected():
            return 0
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        inserted = 0
        for chunk in chunks:
            try:
                cur.execute(
                    f"INSERT INTO {knowledge_table} (VEC_TEXT, VEC_META) VALUES (?, ?)",
                    (chunk["text"], json.dumps(chunk["meta"])),
                )
                inserted += 1
            except Exception as e:
                logger.warning("Failed to insert chunk: %s", e)
        self.conn.commit()
        cur.close()
        return inserted

    def get_unvectorized_chunks(self, limit: int = 20) -> List[tuple]:
        """
        Retrieve chunks that do not yet contain vector embeddings.

        Args:
            limit (int): Maximum number of rows to return.

        Returns:
            List[tuple]: List of (id, text, metadata) tuples.
        """
        if not self._ensure_connected():
            return []
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT TOP {limit} ID, VEC_TEXT, VEC_META
            FROM {knowledge_table}
            WHERE VEC_VECTOR IS NULL
        """
        )
        rows = cur.fetchall()
        cur.close()
        return rows

    def update_embeddings(self, updates: List[Tuple[int, List[float]]]) -> None:
        """
        Update vector embeddings for knowledge base chunks.

        Args:
            updates (List[Tuple[int, List[float]]]): List of (chunk_id, vector) pairs.
        """
        if not updates or not self._ensure_connected():
            return
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        for chunk_id, vector in updates:
            vec_str = "[" + ",".join(str(v) for v in vector) + "]"
            cur.execute(
                f"""
                UPDATE {knowledge_table}
                SET VEC_VECTOR = TO_REAL_VECTOR(?)
                WHERE ID = ?
            """,
                (vec_str, chunk_id),
            )
        self.conn.commit()
        cur.close()

    def search_similar(self, query_vector: List[float], top_k: int = 5) -> List[Dict]:
        """
        Search for semantically similar knowledge chunks using cosine similarity.

        Args:
            query_vector (List[float]): Query embedding vector.
            top_k (int): Maximum number of results to return.

        Returns:
            List[Dict]: Ranked similarity search results with metadata.
        """
        if not self._ensure_connected():
            return []
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        vec_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT VEC_TEXT, VEC_META,
                   COSINE_SIMILARITY(VEC_VECTOR, TO_REAL_VECTOR(?)) AS SIMILARITY
            FROM {knowledge_table}
            WHERE VEC_VECTOR IS NOT NULL
            ORDER BY SIMILARITY DESC
            LIMIT ?
        """,
            (vec_str, top_k),
        )
        results = []
        for row in cur.fetchall():
            results.append(
                {
                    "text": row[0],
                    "meta": json.loads(row[1]),
                    "similarity": row[2] * 100,
                }
            )
        cur.close()
        return results

    def get_knowledge_stats(self) -> Dict[str, int]:
        """
        Retrieve knowledge base storage statistics.

        Returns:
            Dict[str, int]: ``{total, vectorized, pending}``
        """
        if not self._ensure_connected():
            return {"total": 0, "vectorized": 0, "pending": 0}
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {knowledge_table}")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {knowledge_table} WHERE VEC_VECTOR IS NOT NULL")
        vectorized = cur.fetchone()[0]
        cur.close()
        return {"total": total, "vectorized": vectorized, "pending": total - vectorized}

    def close(self) -> None:
        """Close the active database connection. Safe to call multiple times."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        self._ensure_connected()
        return self

    def __exit__(self, *args):
        self.close()


# ------------------------------------------------------------------ #
#  Singleton Support                                                   #
# ------------------------------------------------------------------ #

def get_global_client() -> Optional[HanaClient]:
    """
    Retrieve the singleton HANA client instance.

    Returns:
        Optional[HanaClient]: Shared HANA client instance, or None on failure.
    """
    global _global_client
    if _global_client is not None:
        if _global_client._ensure_connected():
            return _global_client
        else:
            _global_client = None

    try:
        settings = get_settings()
        if not all(
            [settings.HANA_HOST, settings.HANA_USER, settings.HANA_PASSWORD, settings.HANA_SCHEMA]
        ):
            logger.error("HANA credentials missing – cannot create client")
            return None
        _global_client = HanaClient()
        _global_client.create_observability_table()
        _global_client.create_run_incident_map_table()
        logger.info(
            "Singleton HANA client created; observability and RUN_INCIDENT_MAP tables ready"
        )
        return _global_client
    except Exception as e:
        logger.error("Failed to create global HANA client: %s", e)
        return None


def get_hana_client(settings=None):
    """
    Deprecated compatibility wrapper for legacy integrations.

    Args:
        settings: Unused legacy parameter retained for backward compatibility.

    Returns:
        Optional[HanaClient]: Shared singleton HANA client instance.
    """
    logger.warning("get_hana_client() is deprecated – use get_global_client() instead")
    return get_global_client()