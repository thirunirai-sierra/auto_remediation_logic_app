"""Rule-based auto-fix rules for Logic Apps actions."""

from __future__ import annotations

import logging
from typing import Any, Dict, Tuple

from agent.fixer.utils import recursively_patch_contains

logger = logging.getLogger(__name__)

JSONDict = Dict[str, Any]


def _mark_action(action: JSONDict, note: str) -> None:
    meta = action.setdefault("_auto_fix_metadata", {})
    if isinstance(meta, dict):
        meta["note"] = note


def _fix_payload_or_schema_error(action: JSONDict) -> bool:
    changed = recursively_patch_contains(action)
    if changed:
        _mark_action(action, "Applied null guard with coalesce() for contains() expressions.")
    return changed


def _fix_timeout(action: JSONDict) -> bool:
    policy = {
        "type": "fixed",
        "count": 4,
        "interval": "PT20S",
    }
    if action.get("retryPolicy") == policy:
        return False
    action["retryPolicy"] = policy
    _mark_action(action, "Applied fixed retry policy for timeout root cause.")
    return True


def _fix_throttling(action: JSONDict) -> bool:
    policy = {
        "type": "exponential",
        "count": 6,
        "interval": "PT10S",
        "minimumInterval": "PT5S",
        "maximumInterval": "PT1M",
    }
    if action.get("retryPolicy") == policy:
        return False
    action["retryPolicy"] = policy
    _mark_action(action, "Applied exponential retry policy for throttling root cause.")
    return True


def _fix_auth(action: JSONDict) -> bool:
    # Non-invasive hint marker; keeps workflow structure intact.
    _mark_action(
        action,
        "Authorization issue detected. Verify api connection reference, identity, and secret/token bindings.",
    )
    logger.info("Auth root cause detected; applied connection reference diagnostic marker")
    return True


def _fix_not_found(action: JSONDict) -> bool:
    _mark_action(
        action,
        "NotFound detected. Verify endpoint path/resource identifier and API version for this action.",
    )
    logger.info("NotFound root cause detected; applied endpoint correction marker")
    return True


def apply_rule_to_action(action: JSONDict, root_cause: str) -> Tuple[bool, str]:
    """Apply best matching rule; returns (applied, fix_name)."""
    rc = (root_cause or "").strip().lower()
    if rc == "payload_or_schema_error":
        return _fix_payload_or_schema_error(action), "payload_or_schema_error"
    if rc == "timeout":
        return _fix_timeout(action), "timeout_retry_policy"
    if rc == "throttling":
        return _fix_throttling(action), "throttling_retry_policy"
    if rc == "auth_or_authorization_error":
        return _fix_auth(action), "auth_connection_reference_marker"
    if rc == "not_found":
        return _fix_not_found(action), "not_found_endpoint_marker"
    return False, "no_rule_matched"
