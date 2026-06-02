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
import json, os, time, logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from hdbcli import dbapi
from config import get_settings

logger = logging.getLogger(__name__)

# Global singleton instance
_global_client = None


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

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ):
        """
        Initialize the HANA client and establish a database connection.

        Args:
            host (Optional[str]): HANA server hostname or IP address.
            port (Optional[int]): HANA server port.
            user (Optional[str]): Database username.
            password (Optional[str]): Database password.
            schema (Optional[str]): Target HANA schema.
            table (Optional[str]): Observability table name.

        Raises:
            RuntimeError: If database connection initialization fails.
        """
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
        """
        Establish a connection to the SAP HANA database.

        Performs up to 3 retry attempts before failing.

        Returns:
            bool: True if connection succeeds, False otherwise.
        """
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
        """
        Ensure that the database connection is active.

        Validates the current connection using a lightweight query.
        Reconnects automatically if the connection is invalid.

        Returns:
            bool: True if the connection is active or successfully restored,
                  False otherwise.
        """
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
        """
        Create the RUN_INCIDENT_MAP table if it does not already exist.

        This table persists the one-to-one mapping between a RUN_ID
        (the raw identifier coming from Azure / CPI) and the human-friendly
        INCIDENT_ID (ORBLOGICAPPS-YYYYMMDD-XXXXXX) generated by this system.

        Schema
        ------
        RUN_ID        – the original run / correlation identifier (PK)
        INCIDENT_ID   – the generated incident identifier
        CREATED_AT    – when the mapping was first created

        Returns:
            bool: True on success, False otherwise.
        """
        if not self._ensure_connected():
            logger.error("Cannot create RUN_INCIDENT_MAP table: no connection")
            return False

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT 1 FROM {self.map_table} LIMIT 1")
            logger.debug("RUN_INCIDENT_MAP table already exists")
        except Exception:
            # Table does not exist – create it
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
        """
        Generate a new incident ID in the format ORBLOGICAPPS-YYYYMMDD-XXXXXX.

        The numeric suffix is based on the highest existing suffix for today
        across BOTH the observability table and the RUN_INCIDENT_MAP table,
        so there are no gaps or collisions even if one table is queried first.

        Returns:
            str: New unique incident ID.
        """
        today = datetime.now().strftime("%Y%m%d")
        prefix = f"ORBLOGICAPPS-{today}-"
        suffix_start = len(prefix) + 1   # SUBSTRING is 1-based in HANA

        cursor = self.conn.cursor()
        max_num = 0

        # Check the observability table
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
            pass  # Table may not exist yet; ignore

        # Check the map table
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
            pass  # Map table may not exist yet; ignore

        cursor.close()
        return f"{prefix}{(max_num + 1):06d}"

    # Raw Azure / CPI run IDs are long purely alphanumeric strings that end
    # in "CU<digits>" – e.g. "08584217294698359236451708384CU12".
    # They are clearly NOT an ORBLOGICAPPS-style incident ID.
    import re as _re
    _RAW_RUN_ID_PATTERN = _re.compile(r'^[0-9A-Za-z]{20,}CU\d+$')

    def _normalize_record(self, record: Dict[str, Any]) -> None:
        """
        Detect and fix records where the upstream code placed the raw run ID
        (e.g. ``08584217294698359236451708384CU12``) into ``incident_id``
        instead of ``run_id``, leaving ``run_id`` empty.

        When this pattern is detected the value is moved to ``run_id`` and
        ``incident_id`` is cleared so that ``get_or_create_incident_id``
        generates the correct ORBLOGICAPPS-YYYYMMDD-XXXXXX identifier.

        This is a safety net – the real fix should be in the upstream ingestion
        code that builds the record dict.
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
        """
        Return the INCIDENT_ID that belongs to *run_id*.

        If this run_id has been seen before, the existing INCIDENT_ID is
        returned.  If it is new, a fresh ORBLOGICAPPS-YYYYMMDD-XXXXXX ID is
        generated, persisted in RUN_INCIDENT_MAP, and returned.

        Args:
            run_id (str): The raw run / correlation identifier from the source
                          system (Azure Logic App run ID, CPI message GUID, …).

        Returns:
            str: The stable INCIDENT_ID for this run.
        """
        if not run_id:
            # No run_id available – generate a brand-new incident ID
            return self._get_next_incident_id()

        if not self._ensure_connected():
            logger.error("Cannot resolve INCIDENT_ID: no HANA connection")
            return self._get_next_incident_id()

        cursor = self.conn.cursor()
        try:
            # 1. Look up existing mapping
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

            # 2. First time we see this run_id – create a new INCIDENT_ID
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
            # Fall back to a plain new ID so the caller is never left without one
            return self._get_next_incident_id()
        finally:
            cursor.close()

    # ------------------------------------------------------------------ #
    #  Observability Table Methods                                         #
    # ------------------------------------------------------------------ #

    def create_observability_table(self) -> bool:
        """
        Create the observability table if it does not already exist.

        If the table exists, missing columns required for newer schema
        versions are added automatically.

        Returns:
            bool: True if the table exists or was successfully created,
                  False otherwise.
        """
        if not self._ensure_connected():
            logger.error("Cannot create observability table: no connection")
            return False

        cursor = self.conn.cursor()
        # Check existence
        try:
            cursor.execute(f"SELECT 1 FROM {self.full_table} LIMIT 1")
            exists = True
        except Exception:
            exists = False

        if exists:
            # Migrate missing columns – add all new columns if not present
            new_columns = [
                ("RESOURCE_GROUP", "NVARCHAR(128)"),
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
                ("RECEIVER",               "NVARCHAR(200)"),
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

        # Create table (full schema)
        create_sql = f"""
            CREATE COLUMN TABLE {self.full_table} (
                INCIDENT_ID             NVARCHAR(64)   PRIMARY KEY,
                RUN_ID                  NVARCHAR(512),
                SUBSCRIPTION_ID         NVARCHAR(64),
                RESOURCE_GROUP       NVARCHAR(128),
                WORKFLOW_NAME           NVARCHAR(256),
                ERROR_CODE              NVARCHAR(128),
                ERROR_MESSAGE           NCLOB,
                ERROR_CATEGORY          NVARCHAR(64),
                STATUS                  NVARCHAR(32),
                RCA_ROOT_CAUSE          NCLOB,
                FIX_STRATEGY            NVARCHAR(256),
                CREATED_AT              TIMESTAMP,
                UPDATED_AT              TIMESTAMP,
                AUTO_FIX_ATTEMPTED      BOOLEAN,
                AUTO_FIX_SUCCESS        BOOLEAN,
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

        The ``run_id`` value is always stored in the RUN_ID column so it can
        be referenced later.

        Args:
            record (Dict[str, Any]):
                Incident data containing observability and AI analysis fields.
                May include ``run_id`` (preferred) or ``incident_id`` (legacy).

        Returns:
            bool: True if the operation succeeds, False otherwise.
        """
        if not self._ensure_connected():
            return False

        # ── Resolve INCIDENT_ID ──────────────────────────────────────────
        # Safety net: move raw run IDs out of incident_id into run_id
        self._normalize_record(record)

        run_id = record.get("run_id") or record.get("RUN_ID")
        if run_id:
            incident_id = self.get_or_create_incident_id(run_id)
        elif record.get("incident_id"):
            incident_id = record["incident_id"]
        else:
            incident_id = self._get_next_incident_id()

        record["incident_id"] = incident_id
        # ────────────────────────────────────────────────────────────────

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        try:
            sql = f"""
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
            cursor.execute(sql, (
                incident_id,
                run_id,
                record.get("subscription_id"),
                record.get("resource_group"),
                record.get("workflow_name"),
                record.get("error_code", "unknown"),
                (record.get("error_message") or "")[:2000],
                record.get("error_category", "UNKNOWN_ERROR"),
                record.get("status", "Ticket Created"),
                (record.get("rca_root_cause") or "")[:4000],
                (record.get("fix_strategy") or "")[:256],
                now, now,
                record.get("auto_fix_attempted", False),
                record.get("auto_fix_success", False),
                record.get("retry_count", 0),
                record.get("resource_id"),
                record.get("event_time"),
                record.get("ingested_at"),
                record.get("error_type"),
                record.get("ai_diagnosis"),
                record.get("ai_proposed_fix"),
                record.get("ai_confidence"),
                record.get("ai_fix_patch"),
                record.get("field_changes"),
                record.get("history_entries"),
                record.get("properties_json"),
                record.get("artifact_json"),
                record.get("error_details_json"),
            ))
            self.conn.commit()
            return True

        except dbapi.IntegrityError:
            # Record already exists for this INCIDENT_ID – update it
            update_sql = f"""
                UPDATE {self.full_table}
                SET RUN_ID              = COALESCE(RUN_ID, ?),
                    SUBSCRIPTION_ID     = ?,
                    RESOURCE_GROUP = ?,
                    WORKFLOW_NAME       = ?,
                    ERROR_CODE          = ?,
                    ERROR_MESSAGE       = ?,
                    ERROR_CATEGORY      = ?,
                    STATUS              = ?,
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
            cursor.execute(update_sql, (
                run_id,
                record.get("subscription_id"),
                record.get("resource_group"),
                record.get("workflow_name"),
                record.get("error_code", "unknown"),
                (record.get("error_message") or "")[:2000],
                record.get("error_category", "UNKNOWN_ERROR"),
                record.get("status", "Ticket Created"),
                (record.get("rca_root_cause") or "")[:4000],
                (record.get("fix_strategy") or "")[:256],
                now,
                record.get("auto_fix_attempted", False),
                record.get("auto_fix_success", False),
                record.get("retry_count", 0),
                record.get("resource_id"),
                record.get("event_time"),
                record.get("ingested_at"),
                record.get("error_type"),
                record.get("ai_diagnosis"),
                record.get("ai_proposed_fix"),
                record.get("ai_confidence"),
                record.get("ai_fix_patch"),
                record.get("field_changes"),
                record.get("history_entries"),
                record.get("properties_json"),
                record.get("artifact_json"),
                record.get("error_details_json"),
                incident_id,
            ))
            self.conn.commit()
            return True

        except Exception as e:
            logger.error("Upsert failed for %s: %s", incident_id, e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            cursor.close()

    def batch_upsert_observability(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert or update multiple observability records in a single transaction.

        For each record, the INCIDENT_ID is resolved via
        ``get_or_create_incident_id`` when a ``run_id`` is present, otherwise
        a new ID is generated.  The ``run_id`` is stored in the RUN_ID column.

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
            # ── Resolve INCIDENT_ID ──────────────────────────────────────
            # Safety net: move raw run IDs out of incident_id into run_id
            self._normalize_record(rec)

            run_id = rec.get("run_id") or rec.get("RUN_ID")
            if run_id:
                incident_id = self.get_or_create_incident_id(run_id)
            elif rec.get("incident_id"):
                incident_id = rec["incident_id"]
            else:
                incident_id = self._get_next_incident_id()
            rec["incident_id"] = incident_id
            # ─────────────────────────────────────────────────────────────

            try:
                sql = f"""
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
                cursor.execute(sql, (
                    incident_id,
                    run_id,
                    rec.get("subscription_id"),
                    rec.get("resource_group"),
                    rec.get("workflow_name"),
                    rec.get("error_code", "unknown"),
                    (rec.get("error_message") or "")[:2000],
                    rec.get("error_category", "UNKNOWN_ERROR"),
                    rec.get("status", "Ticket Created"),
                    (rec.get("rca_root_cause") or "")[:4000],
                    (rec.get("fix_strategy") or "")[:256],
                    rec.get("created_at"), now,
                    rec.get("auto_fix_attempted", False),
                    rec.get("auto_fix_success", False),
                    rec.get("retry_count", 0),
                    rec.get("resource_id"),
                    rec.get("event_time"),
                    rec.get("ingested_at"),
                    rec.get("error_type"),
                    rec.get("ai_diagnosis"),
                    rec.get("ai_proposed_fix"),
                    rec.get("ai_confidence"),
                    rec.get("ai_fix_patch"),
                    rec.get("field_changes"),
                    rec.get("history_entries"),
                    rec.get("properties_json"),
                    rec.get("artifact_json"),
                    rec.get("error_details_json"),
                    rec.get("log_start"),
                    rec.get("last_seen"),
                    rec.get("occurrence_count", 1),
                    rec.get("affected_component"),
                    rec.get("correlation_id"),
                    rec.get("source_type", "AzureDiagnostics"),
                    rec.get("integration_flow_name"),
                ))
                inserted += 1

            except dbapi.IntegrityError:
                # INCIDENT_ID already exists – update and bump occurrence count
                update_sql = f"""
                    UPDATE {self.full_table}
                    SET RUN_ID              = COALESCE(RUN_ID, ?),
                        SUBSCRIPTION_ID     = ?,
                        RESOURCE_GROUP = ?, WORKFLOW_NAME       = ?,
                        ERROR_CODE          = ?,
                        ERROR_MESSAGE       = ?,
                        ERROR_CATEGORY      = ?,
                        STATUS              = ?,
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
                        AI_DIAGNOSIS        = COALESCE(AI_DIAGNOSIS, ?),
                        AI_PROPOSED_FIX     = COALESCE(AI_PROPOSED_FIX, ?),
                        AI_CONFIDENCE       = COALESCE(AI_CONFIDENCE, ?),
                        AI_FIX_PATCH        = COALESCE(AI_FIX_PATCH, ?),
                        FIELD_CHANGES       = COALESCE(FIELD_CHANGES, ?),
                        HISTORY_ENTRIES     = COALESCE(HISTORY_ENTRIES, ?),
                        PROPERTIES_JSON     = COALESCE(PROPERTIES_JSON, ?),
                        ARTIFACT_JSON       = COALESCE(ARTIFACT_JSON, ?),
                        ERROR_DETAILS_JSON  = COALESCE(ERROR_DETAILS_JSON, ?),
                        LOG_START           = COALESCE(LOG_START, ?),
                        LAST_SEEN           = ?,
                        OCCURRENCE_COUNT    = OCCURRENCE_COUNT + 1,
                        AFFECTED_COMPONENT  = COALESCE(AFFECTED_COMPONENT, ?),
                        CORRELATION_ID      = COALESCE(CORRELATION_ID, ?),
                        SOURCE_TYPE         = COALESCE(SOURCE_TYPE, ?),
                        INTEGRATION_FLOW_NAME = COALESCE(INTEGRATION_FLOW_NAME, ?)
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(update_sql, (
                    run_id,
                    rec.get("subscription_id"),
                    rec.get("resource_group"),
                    rec.get("workflow_name"),
                    rec.get("error_code", "unknown"),
                    (rec.get("error_message") or "")[:2000],
                    rec.get("error_category", "UNKNOWN_ERROR"),
                    rec.get("status", "Ticket Created"),
                    (rec.get("rca_root_cause") or "")[:4000],
                    (rec.get("fix_strategy") or "")[:256],
                    now,
                    rec.get("auto_fix_attempted", False),
                    rec.get("auto_fix_success", False),
                    rec.get("retry_count", 0),
                    rec.get("resource_id"),
                    rec.get("event_time"),
                    rec.get("ingested_at"),
                    rec.get("error_type"),
                    rec.get("ai_diagnosis"),
                    rec.get("ai_proposed_fix"),
                    rec.get("ai_confidence"),
                    rec.get("ai_fix_patch"),
                    rec.get("field_changes"),
                    rec.get("history_entries"),
                    rec.get("properties_json"),
                    rec.get("artifact_json"),
                    rec.get("error_details_json"),
                    rec.get("log_start"),
                    now,
                    rec.get("affected_component"),
                    rec.get("correlation_id"),
                    rec.get("source_type", "AzureDiagnostics"),
                    rec.get("integration_flow_name"),
                    incident_id,
                ))
                inserted += 1

            except Exception as e:
                logger.error("Failed to process %s: %s", incident_id, e)
                failed += 1
                if self.conn:
                    self.conn.rollback()

        self.conn.commit()
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

        If the incident record does not exist, a minimal row is inserted
        with only the RCA fields and the given incident_id.

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

        Metrics include:
            - Total incidents
            - Auto-fix attempt counts
            - Auto-fix success rate
            - Error category distribution
            - Status distribution
            - Top failing workflows

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
                    SUM(CASE WHEN AUTO_FIX_ATTEMPTED = TRUE THEN 1 ELSE 0 END),
                    SUM(CASE WHEN AUTO_FIX_SUCCESS   = TRUE THEN 1 ELSE 0 END)
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

        Optionally drops the existing table in development environments.

        Args:
            drop_first (bool): If True, drops the table before creation.
                               Ignored outside development environments.

        Raises:
            RuntimeError: If the database connection is unavailable.
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
        """
        Retrieve all URLs already stored in the knowledge base.

        Returns:
            set: Unique URLs extracted from metadata.
        """
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

    Reuses an existing active connection when available.
    Automatically initialises the observability table and the
    RUN_INCIDENT_MAP table.

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
        _global_client.create_run_incident_map_table()   # ← new
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