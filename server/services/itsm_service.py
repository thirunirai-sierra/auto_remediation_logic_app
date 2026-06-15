"""
ITSM incident creation for remediation failures that require human follow-up.
"""
import logging
from typing import Any, Dict, Optional, Tuple

import requests

from config import Settings, get_settings

logger = logging.getLogger(__name__)

TICKET_TRIGGER_STATUSES = {"FAILED", "FIX_FAILED", "NEEDS_MANUAL_REVIEW"}
VALID_ITSM_CATEGORIES = {
    "EXPRESSION_ERROR",
    "MAPPING_ERROR",
    "TRIGGER_ERROR",
    "CONNECTOR_ERROR",
    "WORKFLOW_DEFINITION_ERROR",
    "AUTH_CONFIG_ERROR",
    "HTTP_ERROR",
    "ODATA_ERROR",
    "BACKEND_ERROR",
    "THROTTLING_ERROR",
    "RESOURCE_LIMIT_ERROR",
    "SSL_ERROR",
    "DEPENDENCY_ERROR",
    "AUTH_ERROR",
    "UNKNOWN_ERROR",
    "CONNECTIVITY_ERROR",
    "TIMEOUT_ERROR",
}

# Non-canonical ERROR_CATEGORY values from ingest/classifier → canonical type
_ERROR_CATEGORY_ALIASES = {
    "BAD_REQUEST": "HTTP_ERROR",
    "ACTION_FAILED": "HTTP_ERROR",
    "NULL_REFERENCE_ERROR": "EXPRESSION_ERROR",
    "SCHEMA_ERROR": "MAPPING_ERROR",
    "DATA_VALIDATION": "MAPPING_ERROR",
    "404": "MAPPING_ERROR",
    "401": "AUTH_ERROR",
    "TIMEOUT": "TIMEOUT_ERROR",
}

_TICKET_CREATED_CATEGORIES = {
    "BACKEND_ERROR",
    "THROTTLING_ERROR",
    "RESOURCE_LIMIT_ERROR",
    "SSL_ERROR",
    "DEPENDENCY_ERROR",
}

_APPROVAL_CATEGORIES = {
    "AUTH_ERROR",
    "UNKNOWN_ERROR",
}

_RETRY_CATEGORIES = {
    "CONNECTIVITY_ERROR",
    "TIMEOUT_ERROR",
}


def _normalize_error_category(error_category: Optional[str]) -> str:
    """Map ingest/classifier values to a canonical ERROR_CATEGORY."""
    cat = (error_category or "UNKNOWN_ERROR").strip().upper()
    return _ERROR_CATEGORY_ALIASES.get(cat, cat)


def priority_from_error_category(error_category: Optional[str]) -> str:
    """Return ticket priority (CRITICAL/HIGH/MEDIUM/LOW) from ERROR_CATEGORY."""
    cat = _normalize_error_category(error_category)

    if cat in _TICKET_CREATED_CATEGORIES:
        return "HIGH"
    if cat in _APPROVAL_CATEGORIES:
        return "MEDIUM"
    if cat in _RETRY_CATEGORIES:
        return "LOW"
    return "MEDIUM"  # AUTO_FIX types and anything unknown

def _itsm_assigned_to(ticket: Dict[str, Any]) -> Optional[str]:
    """Extract display name for assignee from ITSM GET/transition payload."""
    assignee = ticket.get("assignee")
    if isinstance(assignee, dict):
        return assignee.get("name") or assignee.get("email")
    return None

def is_enabled(settings: Optional[Settings] = None) -> bool:
    """Return True when ITSM integration is configured and enabled."""
    settings = settings or get_settings()
    return bool(settings.ITSM_ENABLED and settings.ITSM_BASE_URL and settings.ITSM_FAKE_USER)


def normalize_status(status: Optional[str]) -> str:
    return (status or "").strip().upper()


def should_create_ticket(status: Optional[str]) -> bool:
    """Return True when the incident status should create a human ticket."""
    return normalize_status(status) in TICKET_TRIGGER_STATUSES


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _ticket_url(base_url: str, ticket_id: str) -> str:
    return f"{_normalize_base_url(base_url)}/api/incidents/{ticket_id}"


def _build_ticket_payload(record: Dict[str, Any], status: str, settings: Settings) -> Dict[str, Any]:
    incident_id = record.get("INCIDENT_ID") or record.get("incident_id") or ""
    workflow_name = record.get("WORKFLOW_NAME") or record.get("workflow_name") or "unknown"
    resource_group = record.get("RESOURCE_GROUP") or record.get("resource_group") or ""
    subscription_id = record.get("SUBSCRIPTION_ID") or record.get("subscription_id") or ""
    error_category = record.get("ERROR_CATEGORY") or record.get("error_category") or "UNKNOWN_ERROR"
    if error_category not in VALID_ITSM_CATEGORIES:
        error_category = "UNKNOWN_ERROR"
    error_code = record.get("ERROR_CODE") or record.get("error_code") or "unknown"
    error_message = record.get("ERROR_MESSAGE") or record.get("error_message") or ""
    root_cause = record.get("RCA_ROOT_CAUSE") or record.get("rca_root_cause") or "unknown"
    suggested_fix = (
        record.get("AI_PROPOSED_FIX")
        or record.get("ai_proposed_fix")
        or record.get("FIX_STRATEGY")
        or record.get("fix_strategy")
        or ""
    )
    fix_failure_reason = record.get("FIX_STRATEGY") or record.get("fix_strategy") or ""

    description_lines = [
        f"Incident Status: {status}",
        f"Workflow Name: {workflow_name}",
        f"Incident ID: {incident_id}",
        f"Resource Group: {resource_group}",
        f"Subscription ID: {subscription_id}",
        f"Error Category: {error_category}",
        f"Error Code: {error_code}",
        f"Error Message: {error_message}",
        f"RCA Root Cause: {root_cause}",
        f"Suggested Fix: {suggested_fix}",
        f"Fix Failure Reason: {fix_failure_reason}",
    ]

    return {
        "title": f"{status}: {workflow_name}",
        "description": "\n".join(description_lines),
        "priority": 1,
        "category": error_category,
        "source": settings.ITSM_SOURCE,
    }


def create_ticket(record: Dict[str, Any], status: str, settings: Optional[Settings] = None) -> Tuple[bool, Dict[str, Any]]:
    """Create a ticket for an incident record."""
    settings = settings or get_settings()
    if not is_enabled(settings):
        return False, {"error": "ITSM integration is disabled or incomplete."}

    payload = _build_ticket_payload(record, status, settings)
    url = f"{_normalize_base_url(settings.ITSM_BASE_URL)}/api/incidents"
    headers = {
        "Content-Type": "application/json",
        "X-Fake-User": settings.ITSM_FAKE_USER,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.error("ITSM ticket creation failed for incident %s: %s", record.get("INCIDENT_ID"), exc)
        return False, {"error": str(exc)}
    except ValueError as exc:
        logger.error("ITSM ticket response was not valid JSON for incident %s: %s", record.get("INCIDENT_ID"), exc)
        return False, {"error": f"Invalid JSON response: {exc}"}

    ticket_id = data.get("id")
    if ticket_id and "url" not in data:
        data["url"] = _ticket_url(settings.ITSM_BASE_URL, ticket_id)
    return True, data

def fetch_ticket_from_itsm(
    itsm_ticket_id: str,
    settings: Optional[Settings] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Fetch current ticket state from ITSM."""
    settings = settings or get_settings()
    if not is_enabled(settings):
        return False, {"error": "ITSM integration is disabled or incomplete."}
    if not itsm_ticket_id:
        return False, {"error": "missing_ticket_id"}

    url = _ticket_url(settings.ITSM_BASE_URL, itsm_ticket_id)
    headers = {
        "Content-Type": "application/json",
        "X-Fake-User": settings.ITSM_FAKE_USER,
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.error("ITSM fetch failed for ticket %s: %s", itsm_ticket_id, exc)
        return False, {"error": str(exc)}
    except ValueError as exc:
        logger.error("ITSM fetch response was not valid JSON for ticket %s: %s", itsm_ticket_id, exc)
        return False, {"error": f"Invalid JSON response: {exc}"}

    if data.get("id") and "url" not in data:
        data["url"] = _ticket_url(settings.ITSM_BASE_URL, data["id"])
    return True, data

def transition_ticket_in_itsm(
    itsm_ticket_id: str,
    to_state: str,
    settings: Optional[Settings] = None,
    resolution_code: Optional[str] = None,
    resolution_notes: Optional[str] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Transition incident state in ITSM."""
    settings = settings or get_settings()
    if not is_enabled(settings):
        return False, {"error": "ITSM integration is disabled or incomplete."}
    if not itsm_ticket_id:
        return False, {"error": "missing_ticket_id"}
    if not to_state:
        return False, {"error": "missing_to_state"}

    url = f"{_normalize_base_url(settings.ITSM_BASE_URL)}/api/incidents/{itsm_ticket_id}/transition"
    headers = {
        "Content-Type": "application/json",
        "X-Fake-User": settings.ITSM_FAKE_USER,
    }
    payload: Dict[str, Any] = {"to_state": to_state}
    if resolution_code:
        payload["resolution_code"] = resolution_code
    if resolution_notes:
        payload["resolution_notes"] = resolution_notes

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.error("ITSM transition failed for ticket %s -> %s: %s", itsm_ticket_id, to_state, exc)
        return False, {"error": str(exc)}
    except ValueError as exc:
        logger.error("ITSM transition response was not valid JSON for ticket %s: %s", itsm_ticket_id, exc)
        return False, {"error": f"Invalid JSON response: {exc}"}

    if data.get("id") and "url" not in data:
        data["url"] = _ticket_url(settings.ITSM_BASE_URL, data["id"])
    return True, data

RESOLVED_ITSM_STATES = {"resolved", "closed"}


def resolve_ticket_in_itsm(
    client: Any,
    incident_id: str,
    itsm_ticket_id: str,
    settings: Optional[Settings] = None,
    resolution_code: str = "Fixed",
    resolution_notes: Optional[str] = None,
) -> Dict[str, Any]:
    """Transition ticket to resolved in ITSM and persist to HANA."""
    settings = settings or get_settings()
    if not is_enabled(settings):
        return {"resolved": False, "skipped": True, "reason": "itsm_disabled"}

    success, ticket = fetch_ticket_from_itsm(itsm_ticket_id, settings)
    if not success:
        return {"resolved": False, "error": ticket.get("error")}

    notes = resolution_notes or "Resolved from Logic Apps app"
    state = (ticket.get("state") or "").strip().lower()
    if state in RESOLVED_ITSM_STATES:
        if notes and not ticket.get("resolution_notes"):
            ticket["resolution_notes"] = notes
        _persist_ticket_sync(client, incident_id, ticket)
        return {"resolved": True, "already": True, "state": state}

    max_steps = 5

    for _ in range(max_steps):
        if state in RESOLVED_ITSM_STATES:
            break

        if state == "new":
            ok, ticket = transition_ticket_in_itsm(itsm_ticket_id, "assigned", settings)
        elif state == "assigned":
            ok, ticket = transition_ticket_in_itsm(itsm_ticket_id, "in_progress", settings)
        elif state == "in_progress":
            ok, ticket = transition_ticket_in_itsm(
                itsm_ticket_id,
                "resolved",
                settings,
                resolution_code=resolution_code,
                resolution_notes=notes,
            )
        else:
            return {"resolved": False, "error": f"unsupported_state:{state}"}

        if not ok:
            return {"resolved": False, "error": ticket.get("error")}

        state = (ticket.get("state") or "").strip().lower()

    if state not in RESOLVED_ITSM_STATES:
        return {"resolved": False, "error": f"could_not_reach_resolved:{state}"}

    if notes and not ticket.get("resolution_notes"):
        ticket["resolution_notes"] = notes

    _persist_ticket_sync(client, incident_id, ticket)
    return {
        "resolved": True,
        "state": state,
        "number": ticket.get("number"),
    }

def sync_ticket_from_itsm(
    client: Any,
    incident_id: str,
    itsm_ticket_id: str,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    """Fetch ticket from ITSM and persist latest state to HANA."""
    settings = settings or get_settings()
    if not is_enabled(settings):
        return {"synced": False, "skipped": True, "reason": "itsm_disabled"}

    success, payload = fetch_ticket_from_itsm(itsm_ticket_id, settings)
    if not success:
        return {"synced": False, "error": payload.get("error")}

    _persist_ticket_sync(client, incident_id, payload)
    return {
        "synced": True,
        "state": payload.get("state"),
        "number": payload.get("number"),
    }

def _fetch_incident_record(client: Any, incident_id: str) -> Optional[Dict[str, Any]]:
    cursor = client.conn.cursor()
    try:
        cursor.execute(
            f"""SELECT INCIDENT_ID, SUBSCRIPTION_ID, RESOURCE_GROUP, WORKFLOW_NAME,
                       ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY, STATUS, RCA_ROOT_CAUSE,
                       FIX_STRATEGY, AI_PROPOSED_FIX, ITSM_TICKET_ID, ITSM_TICKET_NUMBER,
                       ITSM_TICKET_STATE, ITSM_TICKET_URL
                FROM {client.full_table} WHERE INCIDENT_ID = ?""",
            (incident_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))
    finally:
        cursor.close()


def _persist_ticket_state(
    client: Any,
    incident_id: str,
    status: str,
    ticket: Dict[str, Any],
    error_category: Optional[str] = None,
) -> None:
    cursor = client.conn.cursor()
    try:
        priority = priority_from_error_category(error_category)
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET STATUS = ?, UPDATED_AT = CURRENT_TIMESTAMP,
                    ITSM_TICKET_ID = ?, ITSM_TICKET_NUMBER = ?, ITSM_TICKET_STATE = ?, ITSM_TICKET_URL = ?, TICKET_PRIORITY = ?
                WHERE INCIDENT_ID = ?""",
            (
                status,
                ticket.get("id"),
                ticket.get("number"),
                ticket.get("state"),
                ticket.get("url"),
                priority,
                incident_id,
            ),
        )
        client.conn.commit()
    finally:
        cursor.close()

def _persist_ticket_sync(client: Any, incident_id: str, ticket: Dict[str, Any]) -> None:
    """Update ITSM fields from ITSM GET without overwriting incident STATUS."""
    cursor = client.conn.cursor()
    try:
        assigned_to = _itsm_assigned_to(ticket)
        resolution_notes = ticket.get("resolution_notes")
        cursor.execute(
            f"""UPDATE {client.full_table}
                SET UPDATED_AT = CURRENT_TIMESTAMP,
                    ITSM_TICKET_ID = ?,
                    ITSM_TICKET_NUMBER = ?,
                    ITSM_TICKET_STATE = ?,
                    ITSM_TICKET_URL = ?,
                    ITSM_ASSIGNED_TO = ?,
                    ITSM_RESOLUTION_NOTES = ?
                WHERE INCIDENT_ID = ?""",
            (
                ticket.get("id"),
                ticket.get("number"),
                ticket.get("state"),
                ticket.get("url"),
                assigned_to,
                resolution_notes,
                incident_id,
            ),
        )
        client.conn.commit()
    finally:
        cursor.close()

def _persist_ticket_failure(client: Any, incident_id: str) -> None:
    cursor = client.conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {client.full_table} SET STATUS = ?, UPDATED_AT = CURRENT_TIMESTAMP WHERE INCIDENT_ID = ?",
            ("TICKET_CREATE_FAILED", incident_id),
        )
        client.conn.commit()
    finally:
        cursor.close()


def ensure_ticket_for_incident(client: Any, incident_id: str, trigger_status: str, settings: Optional[Settings] = None) -> Dict[str, Any]:
    """Create and persist a ticket for an incident when required."""
    settings = settings or get_settings()
    normalized_status = normalize_status(trigger_status)
    if not should_create_ticket(normalized_status):
        return {"created": False, "skipped": True, "reason": "status_not_eligible"}
    if not is_enabled(settings):
        return {"created": False, "skipped": True, "reason": "itsm_disabled"}

    record = _fetch_incident_record(client, incident_id)
    if not record:
        return {"created": False, "error": "incident_not_found"}

    if record.get("ITSM_TICKET_ID"):
        _persist_ticket_state(client, incident_id, "TICKET_CREATED", {
            "id": record.get("ITSM_TICKET_ID"),
            "number": record.get("ITSM_TICKET_NUMBER"),
            "state": record.get("ITSM_TICKET_STATE"),
            "url": record.get("ITSM_TICKET_URL"),
        }, record.get("ERROR_CATEGORY"))
        return {"created": False, "existing": True, "ticket_id": record.get("ITSM_TICKET_ID")}

    success, payload = create_ticket(record, normalized_status, settings)
    if success:
        _persist_ticket_state(client, incident_id, "TICKET_CREATED", payload, record.get("ERROR_CATEGORY"))
        return {"created": True, "ticket_id": payload.get("id"), "ticket_number": payload.get("number")}

    _persist_ticket_failure(client, incident_id)
    return {"created": False, "error": payload.get("error", "ticket_create_failed")}
