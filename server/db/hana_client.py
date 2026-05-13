# server/db/hana_client.py
"""
Unified HANA database client for observability and knowledge base.
Singleton pattern, batch operations, and dashboard analytics.
"""
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from hdbcli import dbapi
from config import get_settings

logger = logging.getLogger(__name__)

# Global singleton instance
_global_client = None


class HanaClient:
    """
    HANA client supporting both observability table and knowledge base table.
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
        """Establish connection with retries and detailed logging."""
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
        """Check connection and reconnect if needed."""
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
        """Create observability table if not exists and migrate columns."""
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
                ("AI_DIAGNOSIS", "NCLOB"),
                ("AI_PROPOSED_FIX", "NCLOB"),
                ("AI_CONFIDENCE", "DOUBLE"),
                ("AI_FIX_PATCH", "NCLOB"),
                ("FIELD_CHANGES", "NCLOB"),
                ("HISTORY_ENTRIES", "NCLOB"),
                ("PROPERTIES_JSON", "NCLOB"),
                ("ARTIFACT_JSON", "NCLOB"),
                ("ERROR_DETAILS_JSON", "NCLOB"),
                ("MESSAGE_GUID", "NVARCHAR(200)"),
                ("IFLOW_ID", "NVARCHAR(500)"),
                ("SENDER", "NVARCHAR(200)"),
                ("RECEIVER", "NVARCHAR(200)"),
                ("ROOT_CAUSE", "NCLOB MEMORY THRESHOLD 1000"),
                ("PROPOSED_FIX", "NCLOB MEMORY THRESHOLD 1000"),
                ("RCA_CONFIDENCE", "DECIMAL(5,4)"),
                ("AFFECTED_COMPONENT", "NVARCHAR(200)"),
                ("FIX_SUMMARY", "NCLOB MEMORY THRESHOLD 1000"),
                ("COMMENT", "NCLOB MEMORY THRESHOLD 1000"),
                ("CORRELATION_ID", "NVARCHAR(200)"),
                ("LOG_START", "NVARCHAR(64)"),
                ("LOG_END", "NVARCHAR(64)"),
                ("RESOLVED_AT", "NVARCHAR(64)"),
                ("TAGS", "NCLOB MEMORY THRESHOLD 1000"),
                ("INCIDENT_GROUP_KEY", "NVARCHAR(64)"),
                ("OCCURRENCE_COUNT", "INTEGER DEFAULT 1"),
                ("LAST_SEEN", "NVARCHAR(64)"),
                ("VERIFICATION_STATUS", "NVARCHAR(64)"),
                ("SOURCE_TYPE", "NVARCHAR(64)"),
                ("FIX_STEPS", "NCLOB MEMORY THRESHOLD 1000"),
                ("FIX_PLAN_GENERATED_AT", "NVARCHAR(64)"),
                ("LAST_FAILED_STAGE", "NVARCHAR(64)"),
                ("IFLOW_SNAPSHOT_BEFORE", "NCLOB MEMORY THRESHOLD 1000"),
                ("PENDING_SINCE", "NVARCHAR(64)"),
                ("TICKET_ID", "NVARCHAR(512)"),
                ("CONSECUTIVE_FAILURES", "INTEGER DEFAULT 0"),
                ("AUTO_ESCALATED", "INTEGER DEFAULT 0"),
                ("INTEGRATION_FLOW_NAME", "NVARCHAR(500)"),
                ("ARTIFACT_ID", "NVARCHAR(500)"),
                ("designtime_artifact_id", "NVARCHAR(500)"),
                ("property_to_change", "NVARCHAR(500)"),
                ("current_value", "NCLOB MEMORY THRESHOLD 1000"),
                ("correct_value", "NCLOB MEMORY THRESHOLD 1000"),
                ("rca_fixes_json", "NCLOB MEMORY THRESHOLD 1000"),
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

        # Create table (full schema)
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
                ERROR_DETAILS_JSON   NCLOB,
                MESSAGE_GUID         NVARCHAR(200),
                IFLOW_ID             NVARCHAR(500),
                SENDER               NVARCHAR(200),
                RECEIVER             NVARCHAR(200),
                ROOT_CAUSE           NCLOB MEMORY THRESHOLD 1000,
                PROPOSED_FIX         NCLOB MEMORY THRESHOLD 1000,
                RCA_CONFIDENCE       DECIMAL(5,4),
                AFFECTED_COMPONENT   NVARCHAR(200),
                FIX_SUMMARY          NCLOB MEMORY THRESHOLD 1000,
                COMMENT              NCLOB MEMORY THRESHOLD 1000,
                CORRELATION_ID       NVARCHAR(200),
                LOG_START            NVARCHAR(64),
                LOG_END              NVARCHAR(64),
                RESOLVED_AT          NVARCHAR(64),
                TAGS                 NCLOB MEMORY THRESHOLD 1000,
                INCIDENT_GROUP_KEY   NVARCHAR(64),
                OCCURRENCE_COUNT     INTEGER DEFAULT 1,
                LAST_SEEN            NVARCHAR(64),
                VERIFICATION_STATUS  NVARCHAR(64),
                SOURCE_TYPE          NVARCHAR(64),
                FIX_STEPS            NCLOB MEMORY THRESHOLD 1000,
                FIX_PLAN_GENERATED_AT NVARCHAR(64),
                LAST_FAILED_STAGE    NVARCHAR(64),
                IFLOW_SNAPSHOT_BEFORE NCLOB MEMORY THRESHOLD 1000,
                PENDING_SINCE        NVARCHAR(64),
                TICKET_ID            NVARCHAR(512),
                CONSECUTIVE_FAILURES INTEGER DEFAULT 0,
                AUTO_ESCALATED       INTEGER DEFAULT 0,
                INTEGRATION_FLOW_NAME NVARCHAR(500),
                ARTIFACT_ID          NVARCHAR(500),
                designtime_artifact_id NVARCHAR(500),
                property_to_change   NVARCHAR(500),
                current_value        NCLOB MEMORY THRESHOLD 1000,
                correct_value        NCLOB MEMORY THRESHOLD 1000,
                rca_fixes_json       NCLOB MEMORY THRESHOLD 1000
            )
        """
        cursor.execute(create_sql)
        self.conn.commit()
        cursor.close()
        logger.info("Created observability table %s", self.full_table)
        return True

    def upsert_observability_record(self, record: Dict[str, Any]) -> bool:
        """Insert or update a single observability record."""
        return self.batch_upsert_observability([record])[0] > 0  # reuse batch logic

    def batch_upsert_observability(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert or update multiple observability records in a single transaction.
        Now includes new columns: LOG_START, LAST_SEEN, OCCURRENCE_COUNT,
        AFFECTED_COMPONENT, CORRELATION_ID, SOURCE_TYPE, etc.
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
                        INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME,
                        ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                        STATUS, RCA_ROOT_CAUSE, FIX_STRATEGY,
                        CREATED_AT, UPDATED_AT,
                        AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT,
                        AI_DIAGNOSIS, AI_PROPOSED_FIX, AI_CONFIDENCE,
                        AI_FIX_PATCH, FIELD_CHANGES, HISTORY_ENTRIES,
                        PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON,
                        LOG_START, LAST_SEEN, OCCURRENCE_COUNT,
                        AFFECTED_COMPONENT, CORRELATION_ID, SOURCE_TYPE,
                        INTEGRATION_FLOW_NAME
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    rec.get("incident_id"),
                    rec.get("subscription_id"),
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
                # Update on conflict – increment occurrence count and update last_seen
                update_sql = f"""
                    UPDATE {self.full_table}
                    SET SUBSCRIPTION_ID = ?, WORKFLOW_NAME = ?, ERROR_CODE = ?, ERROR_MESSAGE = ?,
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
                        LOG_START = COALESCE(LOG_START, ?),
                        LAST_SEEN = ?,
                        OCCURRENCE_COUNT = OCCURRENCE_COUNT + 1,
                        AFFECTED_COMPONENT = COALESCE(AFFECTED_COMPONENT, ?),
                        CORRELATION_ID = COALESCE(CORRELATION_ID, ?),
                        SOURCE_TYPE = COALESCE(SOURCE_TYPE, ?),
                        INTEGRATION_FLOW_NAME = COALESCE(INTEGRATION_FLOW_NAME, ?)
                    WHERE INCIDENT_ID = ?
                """
                cursor.execute(update_sql, (
                    rec.get("subscription_id"),
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
                    rec.get("log_start"),
                    now,
                    rec.get("affected_component"),
                    rec.get("correlation_id"),
                    rec.get("source_type", "AzureDiagnostics"),
                    rec.get("integration_flow_name"),
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

    def update_rca_record(self, incident_id: str, root_cause: str, proposed_fix: str,
                          confidence: float, affected_component: str = None) -> bool:
        """Update RCA-related columns for a given incident."""
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            sql = f"""
                UPDATE {self.full_table}
                SET ROOT_CAUSE = COALESCE(ROOT_CAUSE, ?),
                    PROPOSED_FIX = COALESCE(PROPOSED_FIX, ?),
                    RCA_CONFIDENCE = COALESCE(RCA_CONFIDENCE, ?),
                    AFFECTED_COMPONENT = COALESCE(AFFECTED_COMPONENT, ?)
                WHERE INCIDENT_ID = ?
            """
            cursor.execute(sql, (root_cause, proposed_fix, confidence, affected_component, incident_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error("Failed to update RCA record for %s: %s", incident_id, e)
            return False
        finally:
            cursor.close()

    def update_fix_result(self, incident_id: str, fix_summary: str, fix_steps: str,
                          verification_status: str, resolved_at: str = None) -> bool:
        """Update fix-related columns after remediation."""
        if not self._ensure_connected():
            return False
        cursor = self.conn.cursor()
        try:
            sql = f"""
                UPDATE {self.full_table}
                SET FIX_SUMMARY = COALESCE(FIX_SUMMARY, ?),
                    FIX_STEPS = COALESCE(FIX_STEPS, ?),
                    VERIFICATION_STATUS = COALESCE(VERIFICATION_STATUS, ?),
                    RESOLVED_AT = COALESCE(RESOLVED_AT, ?)
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
        """Gather dashboard metrics (unchanged, kept for compatibility)."""
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
        """Create knowledge base table with vector column."""
        if not self._ensure_connected():
            raise RuntimeError("Cannot create table: no HANA connection")

        settings = get_settings()
        knowledge_table = f'"{self.schema}"."{settings.HANA_TABLE}"'
        cur = self.conn.cursor()
        if drop_first:
            import os
            # Only allow destructive operation if explicitly flagged
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
        """Return set of URLs already stored in knowledge base."""
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
        """Insert text chunks with metadata into knowledge table."""
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
        """Return (id, text, meta) for chunks without embeddings."""
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
        """Update vector embeddings for given chunk IDs using parameterized queries."""
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
        """Search for similar chunks using cosine similarity."""
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
        """Return total, vectorized, pending counts for knowledge table."""
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
        if self.conn:
            self.conn.close()

    def __enter__(self):
        self._ensure_connected()
        return self

    def __exit__(self, *args):
        self.close()


# ---------- Singleton Support ----------
def get_global_client() -> Optional[HanaClient]:
    """Return a singleton HANA client instance (reused across the application)."""
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


def get_hana_client(settings=None):
    """Deprecated – kept for backward compatibility."""
    logger.warning("get_hana_client() is deprecated – use get_global_client() instead")
    return get_global_client()
# server/db/hana_client.py (inside HanaClient class)

def update_rca_record(self, incident_id: str, root_cause: str, proposed_fix: str,
                      confidence: float, affected_component: str = None) -> bool:
    """
    Insert or update RCA data for an incident (upsert).
    """
    if not self._ensure_connected():
        logger.error("HANA not connected – cannot update RCA record")
        return False

    cursor = self.conn.cursor()
    try:
        # Check if record exists
        cursor.execute(f"SELECT 1 FROM {self.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
        exists = cursor.fetchone() is not None

        if exists:
            sql = f"""
                UPDATE {self.full_table}
                SET ROOT_CAUSE = COALESCE(ROOT_CAUSE, ?),
                    PROPOSED_FIX = COALESCE(PROPOSED_FIX, ?),
                    RCA_CONFIDENCE = COALESCE(RCA_CONFIDENCE, ?),
                    AFFECTED_COMPONENT = COALESCE(AFFECTED_COMPONENT, ?)
                WHERE INCIDENT_ID = ?
            """
            cursor.execute(sql, (root_cause, proposed_fix, confidence, affected_component, incident_id))
            logger.info(f"Updated RCA record for {incident_id}")
        else:
            # Insert a new record with minimal data
            sql = f"""
                INSERT INTO {self.full_table} (
                    INCIDENT_ID, ROOT_CAUSE, PROPOSED_FIX, RCA_CONFIDENCE, AFFECTED_COMPONENT
                ) VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(sql, (incident_id, root_cause, proposed_fix, confidence, affected_component))
            logger.info(f"Inserted new RCA record for {incident_id}")

        self.conn.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to upsert RCA record for {incident_id}: {e}")
        return False
    finally:
        cursor.close()