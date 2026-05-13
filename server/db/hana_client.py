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
            # Migrate missing columns
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
        cursor.close()
        logger.info("Created observability table %s", self.full_table)
        return True

    def upsert_observability_record(self, record: Dict[str, Any]) -> bool:
        """Insert or update a single observability record."""
        if not self._ensure_connected():
            return False

        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
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
            cursor.execute(sql, (
                record.get("incident_id"),
                record.get("subscription_id"),
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
                record.get("error_details_json")
            ))
            self.conn.commit()
            return True
        except dbapi.IntegrityError:
            # Update on conflict
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
                    ERROR_DETAILS_JSON = COALESCE(ERROR_DETAILS_JSON, ?)
                WHERE INCIDENT_ID = ?
            """
            cursor.execute(update_sql, (
                record.get("subscription_id"),
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
        Returns (inserted_count, failed_count).
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
                        PROPERTIES_JSON, ARTIFACT_JSON, ERROR_DETAILS_JSON
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    rec.get("error_details_json")
                ))
                inserted += 1
            except dbapi.IntegrityError:
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
                        ERROR_DETAILS_JSON = COALESCE(ERROR_DETAILS_JSON, ?)
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
        Gather dashboard metrics for the observability table.

        Args:
            start_date: Start of time range.
            end_date: End of time range.
            subscription_id: Optional filter by subscription.

        Returns:
            Dict with total_incidents, auto_fix_attempted, auto_fix_succeeded,
            auto_fix_rate_percent, error_distribution, status_distribution,
            top_failing_workflows.
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
        """Create knowledge base table with vector column."""
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


# Compatibility wrapper for old code
def get_hana_client(settings=None):
    """Deprecated – kept for backward compatibility."""
    logger.warning("get_hana_client() is deprecated – use get_global_client() instead")
    return get_global_client()