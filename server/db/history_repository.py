# """
# History repository for managing incident timeline entries in SAP HANA.

# This module provides utility functions to:
# - Append timeline/history entries to incidents
# - Retrieve incident history entries
# - Maintain chronological audit tracking for observability workflows

# History entries are stored as JSON arrays inside the
# HISTORY_ENTRIES column of the observability table.
# """
# import json,logging
# from datetime import datetime, timezone
# from typing import List, Dict, Any

# from db.hana_client import get_global_client

# logger = logging.getLogger(__name__)


# def append_history_entry(
#     incident_id: str,
#     step: str,
#     description: str,
#     status: str,
#     timestamp: str = None,
# ) -> bool:
#     """
#     Append a timeline entry to the incident's history in HANA.

#     Args:
#         incident_id (str): The run ID (incident ID).
#         step (str): Short title of the event (e.g., "RCA Analysis").
#         description (str): Detailed description.
#         status (str): One of "completed", "failed", "pending", "in_progress", "info".
#         timestamp (str, optional): ISO timestamp. If None, uses current UTC time.

#     Returns:
#         bool: True if successful, False otherwise.
#     """
#     client = get_global_client()
#     if not client or not client._ensure_connected():
#         logger.warning("Cannot append history: HANA client not available")
#         return False

#     if timestamp is None:
#         timestamp = datetime.now(timezone.utc).isoformat()

#     new_entry = {
#         "step": step,
#         "description": description,
#         "status": status,
#         "timestamp": timestamp,
#     }

#     cur = client.conn.cursor()
#     try:
#         # Fetch existing history
#         cur.execute(f"SELECT HISTORY_ENTRIES FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
#         row = cur.fetchone()
#         entries = json.loads(row[0]) if row and row[0] else []
#         entries.append(new_entry)

#         # Update the row
#         cur.execute(
#             f"UPDATE {client.full_table} SET HISTORY_ENTRIES = ? WHERE INCIDENT_ID = ?",
#             (json.dumps(entries), incident_id)
#         )
#         client.conn.commit()
#         logger.info("History entry added for %s: %s", incident_id, step)
#         return True
#     except Exception as e:
#         logger.error("Failed to append history for %s: %s", incident_id, e)
#         if client.conn:
#             client.conn.rollback()
#         return False
#     finally:
#         cur.close()


# def get_history_entries(incident_id: str) -> List[Dict[str, Any]]:
#     """
#     Retrieve all history entries for an incident.

#     Args:
#         incident_id (str): The incident ID.

#     Returns:
#         List[Dict]: List of history entries (each with step, description, status, timestamp).
#     """
#     client = get_global_client()
#     if not client or not client._ensure_connected():
#         return []

#     cur = client.conn.cursor()
#     try:
#         cur.execute(f"SELECT HISTORY_ENTRIES FROM {client.full_table} WHERE INCIDENT_ID = ?", (incident_id,))
#         row = cur.fetchone()
#         if row and row[0]:
#             return json.loads(row[0])
#         return []
#     except Exception as e:
#         logger.error("Failed to get history for %s: %s", incident_id, e)
#         return []
#     finally:
#         cur.close()