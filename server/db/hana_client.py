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

Classes:
    HanaClient: Main database client for SAP HANA operations.

Functions:
    get_global_client: Returns a singleton HanaClient instance.
    get_hana_client: Deprecated compatibility wrapper.
"""
import json,os,time,logging
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
        self.conn: Optional[dbapi.Connection] = None
        self._connect()

    # ---------- Connection Management ----------
    def _connect(self) -> bool:
        """
        Establish a connection to the SAP HANA database.

        Performs up to 3 retry attempts before failing.

        Returns:
            bool:
                True if connection succeeds,
                False otherwise.
        """
        if not self.host:
            logger.error("Cannot connect: HANA_HOST missing")
            return False

        for attempt in range(3):
            try:
                logger.info("Connecting to HANA %s:%s as %s (attempt %d/3)", self.host, self.port, self.user, attempt + 1)
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
            bool:
                True if the connection is active or successfully restored,
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

    # ---------- Observability Table Methods ----------
    def create_observability_table(self) -> bool:
        """
        Create the observability table if it does not already exist.

        If the table exists, missing columns required for newer schema
        versions are added automatically.

        Returns:
            bool:
                True if the table exists or was successfully created,
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
            # Migrate missing columns
            new_columns = [
                ("RESOURCE_GROUP", "NVARCHAR(128)"),
                ("AI_DIAGNOSIS", "NCLOB"),
                ("AI_PROPOSED_FIX", "NCLOB"),
                ("AI_CONFIDENCE", "DOUBLE"),
                ("AI_FIX_PATCH", "NCLOB"),
                ("FIELD_CHANGES", "NCLOB"),
                ("HISTORY_ENTRIES", "NCLOB"),
                ("PROPERTIES_JSON", "NCLOB"),
                ("ARTIFACT_JSON", "NCLOB"),
                ("ERROR_DETAILS_JSON", "NCLOB"),
                ("ITSM_TICKET_ID", "NVARCHAR(64)"),
                ("ITSM_TICKET_NUMBER", "NVARCHAR(64)"),
                ("ITSM_TICKET_STATE", "NVARCHAR(64)"),
                ("ITSM_TICKET_URL", "NVARCHAR(512)"),
            ]
            for col_name, col_type in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE {self.full_table} ADD ({col_name} {col_type})")
                    logger.info("Added column %s to %s", col_name, self.full_table)
                except Exception as e:
                    if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                        continue
                    logger.warning("Could not add column %s: %s", col_name, e)
            self.conn.commit()
            cursor.close()
            return True

        # Create table
        create_sql = f"""
            CREATE COLUMN TABLE {self.full_table} (
                INCIDENT_ID          NVARCHAR(64) PRIMARY KEY,
                SUBSCRIPTION_ID      NVARCHAR(64),
                RESOURCE_GROUP       NVARCHAR(128),
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
                ERROR_DETAILS_JSON   NCLOB,
                ITSM_TICKET_ID       NVARCHAR(64),
                ITSM_TICKET_NUMBER   NVARCHAR(64),
                ITSM_TICKET_STATE    NVARCHAR(64),
                ITSM_TICKET_URL      NVARCHAR(512)
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

        If the incident already exists, the existing record is updated.

        Args:
            record (Dict[str, Any]):
                Incident data containing observability and AI analysis fields.

        Returns:
            bool:
                True if the operation succeeds,
                False otherwise.
        """
        if not self._ensure_connected():
            return False

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        try:
            sql = f"""
                INSERT INTO {self.full_table} (
                    INCIDENT_ID, SUBSCRIPTION_ID, RESOURCE_GROUP, WORKFLOW_NAME,
                    ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                    STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                    CREATED_AT, UPDATED_AT,
                    AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                    AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                    AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                    PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON,
                    ITSM_TICKET_ID, ITSM_TICKET_NUMBER, ITSM_TICKET_STATE, ITSM_TICKET_URL
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(sql, (
                record.get("incident_id"),
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
                record.get("ai_diagnosis"),
                record.get("ai_proposed_fix"),
                record.get("ai_confidence"),
                record.get("ai_fix_patch"),
                record.get("field_changes"),
                record.get("history_entries"),
                record.get("properties_json"),
                record.get("artifact_json"),
                record.get("error_details_json"),
                record.get("itsm_ticket_id"),
                record.get("itsm_ticket_number"),
                record.get("itsm_ticket_state"),
                record.get("itsm_ticket_url")
            ))
            self.conn.commit()
            return True
        except dbapi.IntegrityError:
            update_sql = f"""
                UPDATE {self.full_table}
                SET SUBSCRIPTION_ID = ?,
                    RESOURCE_GROUP = ?,
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
                    AI_DIAGNOSIS = COALESCE(?, AI_DIAGNOSIS),
                    AI_PROPOSED_FIX = COALESCE(?, AI_PROPOSED_FIX),
                    AI_CONFIDENCE = COALESCE(?, AI_CONFIDENCE),
                    AI_FIX_PATCH = COALESCE(?, AI_FIX_PATCH),
                    FIELD_CHANGES = COALESCE(?, FIELD_CHANGES),
                    HISTORY_ENTRIES = COALESCE(?, HISTORY_ENTRIES),
                    PROPERTIES_JSON = COALESCE(?, PROPERTIES_JSON),
                    ARTIFACT_JSON = COALESCE(?, ARTIFACT_JSON),
                    ERROR_DETAILS_JSON = COALESCE(?, ERROR_DETAILS_JSON),
                    ITSM_TICKET_ID = COALESCE(?, ITSM_TICKET_ID),
                    ITSM_TICKET_NUMBER = COALESCE(?, ITSM_TICKET_NUMBER),
                    ITSM_TICKET_STATE = COALESCE(?, ITSM_TICKET_STATE),
                    ITSM_TICKET_URL = COALESCE(?, ITSM_TICKET_URL)
                WHERE INCIDENT_ID = ?
            """
            cursor.execute(update_sql, (
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
                record.get("ai_diagnosis"),
                record.get("ai_proposed_fix"),
                record.get("ai_confidence"),
                record.get("ai_fix_patch"),
                record.get("field_changes"),
                record.get("history_entries"),
                record.get("properties_json"),
                record.get("artifact_json"),
                record.get("error_details_json"),
                record.get("itsm_ticket_id"),
                record.get("itsm_ticket_number"),
                record.get("itsm_ticket_state"),
                record.get("itsm_ticket_url"),
                record.get("incident_id")
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error("Upsert failed for %s: %s", record.get("incident_id"), e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            cursor.close()

    def batch_upsert_observability(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert or update multiple observability records in a single transaction.

        Existing records are updated automatically when primary key conflicts occur.

        Args:
            records (List[Dict[str, Any]]):
                List of observability incident records.

        Returns:
            Tuple[int, int]:
                A tuple containing:
                    - inserted_or_updated_count (int)
                    - failed_count (int)
        """
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
                        INCIDENT_ID, SUBSCRIPTION_ID, RESOURCE_GROUP, WORKFLOW_NAME,
                        ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                        STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                        CREATED_AT, UPDATED_AT,
                        AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                        AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                        AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                        PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON,
                        ITSM_TICKET_ID, ITSM_TICKET_NUMBER, ITSM_TICKET_STATE, ITSM_TICKET_URL
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    rec.get("incident_id"),
                    rec.get("subscription_id"),
                    rec.get("resource_group"),
                    rec.get("workflow_name"),
                    rec.get("error_code", "unknown"),
                    (rec.get("error_message") or "")[:2000],
                    rec.get("error_category", "UNKNOWN_ERROR"),
                    rec.get("status", "Ticket Created"),
                    (rec.get("rca_root_cause") or "")[:4000],
                    (rec.get("fix_strategy") or "")[:256],
                    now, now,
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
                    rec.get("itsm_ticket_id"),
                    rec.get("itsm_ticket_number"),
                    rec.get("itsm_ticket_state"),
                    rec.get("itsm_ticket_url")
                ))
                inserted += 1
            except dbapi.IntegrityError:
                update_sql = f"""
                    UPDATE {self.full_table}
                    SET SUBSCRIPTION_ID = ?, RESOURCE_GROUP = ?, WORKFLOW_NAME = ?, ERROR_CODE = ?, ERROR_MESSAGE = ?,
                        ERROR_CATEGORY = ?, STATUS = ?, RCA_ROOT_CAUSE = ?, FIX_STRATEGY = ?,
                        UPDATED_AT = ?, AUTO_FIX_ATTEMPTED = ?, AUTO_FIX_SUCCESS = ?, RETRY_COUNT = ?,
                        AI_DIAGNOSIS = COALESCE(AI_DIAGNOSIS, ?),
                        AI_PROPOSED_FIX = COALESCE(AI_PROPOSED_FIX, ?),
                        AI_CONFIDENCE = COALESCE(AI_CONFIDENCE, ?),
                        AI_FIX_PATCH = COALESCE(AI_FIX_PATCH, ?),
                        FIELD_CHANGES = COALESCE(FIELD_CHANGES, ?),
                        HISTORY_ENTRIES = COALESCE(HISTORY_ENTRIES, ?),
                        PROPERTIES_JSON = COALESCE(PROPERTIES_JSON, ?),
                        ARTIFACT_JSON = COALESCE(ARTIFACT_JSON, ?),
                        ERROR_DETAILS_JSON = COALESCE(ERROR_DETAILS_JSON, ?),
                        ITSM_TICKET_ID = COALESCE(?, ITSM_TICKET_ID),
                        ITSM_TICKET_NUMBER = COALESCE(?, ITSM_TICKET_NUMBER),
                        ITSM_TICKET_STATE = COALESCE(?, ITSM_TICKET_STATE),
                        ITSM_TICKET_URL = COALESCE(?, ITSM_TICKET_URL)
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(update_sql, (
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
                    rec.get("ai_diagnosis"),
                    rec.get("ai_proposed_fix"),
                    rec.get("ai_confidence"),
                    rec.get("ai_fix_patch"),
                    rec.get("field_changes"),
                    rec.get("history_entries"),
                    rec.get("properties_json"),
                    rec.get("artifact_json"),
                    rec.get("error_details_json"),
                    rec.get("itsm_ticket_id"),
                    rec.get("itsm_ticket_number"),
                    rec.get("itsm_ticket_state"),
                    rec.get("itsm_ticket_url"),
                    rec.get("incident_id")
                ))
                inserted += 1
            except Exception as e:
                logger.error("Failed to process %s: %s", rec.get("incident_id"), e)
                failed += 1
                if self.conn:
                    self.conn.rollback()

        self.conn.commit()
        cursor.close()
        logger.info("Batch upsert: %d inserted/updated, %d failed", inserted, failed)
        return inserted, failed

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
            start_date (datetime):
                Start of the reporting window.
            end_date (datetime):
                End of the reporting window.
            subscription_id (Optional[str]):
                Optional subscription filter.

        Returns:
            Dict[str, Any]:
                Dictionary containing dashboard statistics and metadata.
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
                "end_date": end_date.isoformat(),
            }
        except Exception as e:
            logger.error("Dashboard stats error: %s", e)
            return {"error": str(e)}

    # ---------- Knowledge Base Table Methods ----------
    def create_knowledge_table(self, drop_first: bool = False) -> None:
        """
        Create the vector-enabled knowledge base table.

        Optionally drops the existing table in development environments.

        Args:
            drop_first (bool):
                If True, drops the table before creation.
                Ignored outside development environments.

        Raises:
            RuntimeError:
                If the database connection is unavailable.
        """
        if not self._ensure_connected():
            raise RuntimeError("Cannot create table: no HANA connection")

        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        if drop_first:
            # Only allow destructive operation if explicitly flagged (e.g., not in production)
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
                ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                VEC_TEXT NCLOB,
                VEC_META NCLOB,
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
            set:
                Set of unique URLs extracted from metadata.
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
            chunks (List[Dict[str, Any]]):
                List of chunk dictionaries containing:
                    - text
                    - meta

        Returns:
            int:
                Number of successfully inserted chunks.
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
            limit (int):
                Maximum number of rows to return.

        Returns:
            List[tuple]:
                List of tuples containing:
                    (id, text, metadata)
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
            updates (List[Tuple[int, List[float]]]):
                List containing:
                    - chunk ID
                    - embedding vector
        """
        if not updates or not self._ensure_connected():
            return
        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        for chunk_id, vector in updates:
            vec_str = "[" + ",".join(str(v) for v in vector) + "]"
            # Use parameterized query to prevent injection
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
        Search for semantically similar knowledge chunks.

        Uses cosine similarity against stored vector embeddings.

        Args:
            query_vector (List[float]):
                Query embedding vector.
            top_k (int):
                Maximum number of results to return.

        Returns:
            List[Dict]:
                Ranked similarity search results with metadata.
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
                   COSINE_SIMILARITY(VEC_VECTOR, TO_REAL_VECTOR(?)) as SIMILARITY
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
            Dict[str, int]:
                Dictionary containing:
                    - total
                    - vectorized
                    - pending
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
        """
        Close the active database connection.

        Safe to call multiple times.
        """
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """
        Enter context manager scope.

        Returns:
            HanaClient:
                Active HANA client instance.
        """
        self._ensure_connected()
        return self

    def __exit__(self, *args):
        """
        Exit context manager scope and close the database connection.

        Args:
            *args:
                Context manager exception arguments.
        """
        self.close()


# ---------- Singleton Support ----------
def get_global_client() -> Optional[HanaClient]:
    """
    Retrieve the singleton HANA client instance.

    Reuses an existing active connection when available.
    Automatically initializes the observability table.

    Returns:
        Optional[HanaClient]:
            Shared HANA client instance if successful,
            otherwise None.
    """
    global _global_client
    if _global_client is not None:
        if _global_client._ensure_connected():
            return _global_client
        else:
            _global_client = None

    try:
        settings = get_settings()
        if not all([settings.HANA_HOST, settings.HANA_USER, settings.HANA_PASSWORD, settings.HANA_SCHEMA]):
            logger.error("HANA credentials missing – cannot create client")
            return None
        _global_client = HanaClient()
        _global_client.create_observability_table()
        logger.info("Singleton HANA client created and observability table ready")
        return _global_client
    except Exception as e:
        logger.error("Failed to create global HANA client: %s", e)
        return None


# Compatibility wrapper for old code
def get_hana_client(settings=None):
    """
    Deprecated compatibility wrapper for legacy integrations.

    Args:
        settings:
            Unused legacy parameter retained for backward compatibility.

    Returns:
        Optional[HanaClient]:
            Shared singleton HANA client instance.
    """
    
    logger.warning("get_hana_client() is deprecated – use get_global_client() instead")
    return get_global_client()