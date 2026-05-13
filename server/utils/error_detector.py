from __future__ import annotations

import re

from typing import Optional, Sequence, Tuple, Dict, Any

# Local type aliases (previously from agent.observer.models.rca_model)
Action = Dict[str, Any]
JSONDict = Dict[str, Any]

# Import helpers from the new utils location
from utils.helpers import extract_missing_field, normalize_action_name

ROOT_CAUSE_NULL = "null_reference_error"
ROOT_CAUSE_TIMEOUT = "timeout"
ROOT_CAUSE_AUTH = "auth_or_authorization_error"
ROOT_CAUSE_DNS = "dns_resolution_error"
ROOT_CAUSE_CONN_REFUSED = "connection_refused"
ROOT_CAUSE_SCHEMA = "payload_or_schema_error"
ROOT_CAUSE_NOT_FOUND = "not_found"
ROOT_CAUSE_THROTTLING = "throttling"
ROOT_CAUSE_TRIGGER_DISABLED = "trigger_disabled"
ROOT_CAUSE_UNKNOWN = "unknown"


def infer_root_cause(code: str, message: str) -> str:
    """
    Infer the root cause category from an error code and message.

    Uses rule-based pattern matching across:
    - Azure Logic Apps error codes
    - HTTP status codes
    - Regex-based message patterns

    Args:
        code (str): Error code returned by Azure or downstream system.
        message (str): Error message text.

    Returns:
        str: Normalized root cause label such as:
            - null_reference_error
            - timeout
            - auth_or_authorization_error
            - dns_resolution_error
            - connection_refused
            - payload_or_schema_error
            - not_found
            - throttling
            - trigger_disabled
            - unknown
    """
    c = (code or "").upper()
    m = (message or "").upper()

    if "WORKFLOWTRIGGERISNOTENABLED" in c or "WORKFLOWTRIGGERISNOTENABLED" in m:
        return ROOT_CAUSE_TRIGGER_DISABLED
    if "TOOMANYCONCURRENTREQUESTS" in c or "TOOMANYCONCURRENTREQUESTS" in m:
        return ROOT_CAUSE_THROTTLING
    if re.search(r"\b429\b", m):
        return ROOT_CAUSE_THROTTLING

    specific_patterns: Sequence[Tuple[str, Sequence[str], Sequence[str]]] = (
        (
            ROOT_CAUSE_NULL,
            (
                r"CONTAINS\(\).*(NULL|NIL)",
                r"EXPECTS?.*(COLLECTION|ARRAY|OBJECT).*(NULL|NIL)",
                r"THE TEMPLATE LANGUAGE FUNCTION 'CONTAINS'.*NULL",
                r"CANNOT ACCESS.*ON A NULL VALUE",
            ),
            ("INVALIDTEMPLATE",),
        ),
        (
            ROOT_CAUSE_DNS,
            (
                r"UNRESOLVABLEHOSTNAME",
                r"NAME OR SERVICE NOT KNOWN",
                r"NO SUCH HOST",
                r"DNS.*(FAIL|RESOLV)",
            ),
            ("UNRESOLVABLEHOSTNAME",),
        ),
        (
            ROOT_CAUSE_CONN_REFUSED,
            (
                r"ECONNREFUSED",
                r"CONNECTION REFUSED",
                r"TARGET MACHINE ACTIVELY REFUSED",
            ),
            ("CONNECTIONREFUSED",),
        ),
        (
            ROOT_CAUSE_THROTTLING,
            (
                r"\b429\b",
                r"TOO MANY REQUESTS",
                r"RATE LIMIT",
                r"THROTTL",
            ),
            ("TOOMANYREQUESTS", "THROTTLED", "THROTTLING"),
        ),
        (
            ROOT_CAUSE_AUTH,
            (
                r"\b401\b",
                r"\b403\b",
                r"UNAUTHORIZED",
                r"FORBIDDEN",
                r"AUTHORIZATION FAILED",
                r"INSUFFICIENT PRIVILEGES",
                r"TOKEN.*(EXPIRED|INVALID)",
            ),
            ("UNAUTHORIZED", "FORBIDDEN", "AUTHORIZATIONFAILED"),
        ),
        (
            ROOT_CAUSE_TIMEOUT,
            (
                r"\b408\b",
                r"\b504\b",
                r"TIMED OUT",
                r"TIMEOUT",
                r"REQUEST TIMEOUT",
            ),
            ("TIMEOUT", "REQUESTTIMEOUT", "GATEWAYTIMEOUT"),
        ),
        (
            ROOT_CAUSE_NOT_FOUND,
            (
                r"\b404\b",
                r"NOT FOUND",
                r"DOES NOT EXIST",
                r"RESOURCE.*NOT FOUND",
            ),
            ("NOTFOUND", "RESOURCENOTFOUND"),
        ),
        (
            ROOT_CAUSE_SCHEMA,
            (
                r"BAD REQUEST",
                r"\b400\b",
                r"INVALID",
                r"MALFORMED",
                r"SCHEMA",
                r"IS REQUIRED",
            ),
            ("BADREQUEST", "INVALIDTEMPLATE", "INVALIDREQUESTCONTENT"),
        ),
    )

    for cause, message_patterns, code_tokens in specific_patterns:
        if any(token in c for token in code_tokens):
            if cause != ROOT_CAUSE_SCHEMA:
                return cause
            if ROOT_CAUSE_NULL not in (infer_root_cause("", message),):
                return cause
        if any(re.search(p, m, re.I) for p in message_patterns):
            return cause

    return ROOT_CAUSE_UNKNOWN


def detect_null_source(message: str, flow_context: Optional[JSONDict]) -> Optional[str]:
    """
    Attempt to identify the source expression that caused a null reference error.

    Args:
        message (str): Error message string.
        flow_context (Optional[JSONDict]): Execution context of workflow.

    Returns:
        Optional[str]: Expression causing null usage (if found), otherwise None.
    """
    expr = re.search(r"outputs\('([^']+)'\)", message or "", re.I)
    if expr:
        return f"outputs('{expr.group(1)}')"

    if not flow_context:
        return None
    for key in ("action_inputs_preview", "action_outputs_preview"):
        preview = str(flow_context.get(key) or "")
        expr2 = re.search(r"outputs\('([^']+)'\)", preview, re.I)
        if expr2:
            return f"outputs('{expr2.group(1)}')"
    return None


def extract_exact_issue(message: str, root_cause: str, flow_context: Optional[JSONDict] = None) -> str:
    """
    Generate a human-readable explanation of the failure.

    Args:
        message (str): Raw error message.
        root_cause (str): Classified root cause label.
        flow_context (Optional[JSONDict]): Optional workflow execution context.

    Returns:
        str: Simplified explanation of the failure suitable for RCA output.
    """
    m = (message or "").strip()
    if root_cause == ROOT_CAUSE_NULL:
        if re.search(r"contains\(\)", m, re.I) or re.search(r"function 'contains'", m, re.I):
            return "contains() received null instead of collection."
        src = detect_null_source(message=m, flow_context=flow_context)
        if src:
            return f"Expression is using a null value from {src}."
        return "Expression attempted to use null where array/object/string is required."
    if root_cause == ROOT_CAUSE_TIMEOUT:
        return "API did not respond within allowed time."
    if root_cause == ROOT_CAUSE_AUTH:
        return "Request failed due to invalid credentials or missing authorization."
    if root_cause == ROOT_CAUSE_DNS:
        return "Endpoint host cannot be resolved by DNS."
    if root_cause == ROOT_CAUSE_CONN_REFUSED:
        return "Target endpoint refused connection."
    if root_cause == ROOT_CAUSE_SCHEMA:
        missing = extract_missing_field(m)
        if missing:
            return f"Payload/schema validation failed; required field '{missing}' is missing or invalid."
        return "Payload/schema validation failed due to invalid or malformed request body."
    if root_cause == ROOT_CAUSE_NOT_FOUND:
        return "Requested resource or API route does not exist."
    if root_cause == ROOT_CAUSE_THROTTLING:
        return "Downstream API throttled the request."
    if root_cause == ROOT_CAUSE_TRIGGER_DISABLED:
        return "Workflow trigger is disabled and cannot execute."
    return (m[:240] + "...") if len(m) > 240 else (m or "Unable to determine exact issue from run error.")


def confidence_score(root_cause: str, code: str, message: str) -> float:
    """
    Compute confidence score for inferred root cause classification.

    Args:
        root_cause (str): Predicted root cause label.
        code (str): Error code.
        message (str): Error message.

    Returns:
        float: Confidence score between 0.0 and 0.99.
    """
    c = (code or "").upper()
    m = (message or "").upper()
    if root_cause == ROOT_CAUSE_UNKNOWN:
        return 0.35
    score = 0.72
    if root_cause == ROOT_CAUSE_NULL and ("INVALIDTEMPLATE" in c and "NULL" in m):
        score = 0.95
    elif root_cause == ROOT_CAUSE_AUTH and re.search(r"\b(401|403)\b", m):
        score = 0.93
    elif root_cause == ROOT_CAUSE_NOT_FOUND and re.search(r"\b404\b", m):
        score = 0.93
    elif root_cause == ROOT_CAUSE_TIMEOUT and re.search(r"\b(408|504)\b", m):
        score = 0.92
    elif root_cause == ROOT_CAUSE_THROTTLING and re.search(r"\b429\b", m):
        score = 0.92
    elif root_cause == ROOT_CAUSE_TRIGGER_DISABLED:
        score = 0.98
    elif root_cause in (ROOT_CAUSE_DNS, ROOT_CAUSE_CONN_REFUSED):
        score = 0.90
    elif root_cause == ROOT_CAUSE_SCHEMA:
        score = 0.86
    return round(max(0.0, min(0.99, score)), 2)


def extract_error_location(
    action: Action,
    message: str,
    flow_context: Optional[JSONDict] = None,
) -> Tuple[str, str]:
    """
    Identify the failing action name and type from error context.

    Args:
        action (Action): Azure Logic App action payload.
        message (str): Error message.
        flow_context (Optional[JSONDict]): Optional execution metadata.

    Returns:
        Tuple[str, str]:
            - action_name (str)
            - action_type (str)
    """
    action_name = normalize_action_name(str(action.get("name") or ""))
    action_type = extract_action_type(action, flow_context)

    quoted = re.search(r"action\s+'([^']+)'", message, re.I)
    if quoted:
        action_name = quoted.group(1).strip()
    quoted2 = re.search(r'action\s+"([^"]+)"', message, re.I)
    if quoted2:
        action_name = quoted2.group(1).strip()

    if flow_context:
        if not action_name or action_name == "unknown":
            action_name = str(flow_context.get("failed_action_name") or action_name or "unknown")
        if action_type == "unknown":
            action_type = str(flow_context.get("action_type") or "unknown")

    return (action_name or "unknown", action_type or "unknown")


def extract_error_code(action: Action) -> str:
    """
    Extract error code from nested Azure action response structures.

    Args:
        action (Action): Azure Logic App action object.

    Returns:
        str: Extracted error code or empty string if not found.
    """
    props = action.get("properties") or {}
    err = props.get("error") if isinstance(props.get("error"), dict) else action.get("error")
    if isinstance(err, dict) and err.get("code"):
        return str(err.get("code"))
    out = props.get("outputs") or action.get("outputs") or {}
    if isinstance(out, dict):
        out_err = out.get("error")
        if isinstance(out_err, dict) and out_err.get("code"):
            return str(out_err.get("code"))
        body = out.get("body")
        if isinstance(body, dict):
            body_err = body.get("error")
            if isinstance(body_err, dict) and body_err.get("code"):
                return str(body_err.get("code"))
    return ""


def extract_error_message(action: Action) -> str:
    """
    Extract error message from Azure action response.

    Args:
        action (Action): Azure Logic App action object.

    Returns:
        str: Error message string or empty string if unavailable.
    """
    props = action.get("properties") or {}
    err = props.get("error") if isinstance(props.get("error"), dict) else action.get("error")
    if isinstance(err, dict) and err.get("message"):
        return str(err.get("message"))
    out = props.get("outputs") or action.get("outputs") or {}
    if isinstance(out, dict):
        out_err = out.get("error")
        if isinstance(out_err, dict) and out_err.get("message"):
            return str(out_err.get("message"))
        body = out.get("body")
        if isinstance(body, dict):
            body_err = body.get("error")
            if isinstance(body_err, dict) and body_err.get("message"):
                return str(body_err.get("message"))
            if body.get("message"):
                return str(body.get("message"))
    return str(props.get("message") or "")


def extract_action_type(action: Action, flow_context: Optional[JSONDict]) -> str:
    """
    Determine the type of Azure Logic App action.

    Args:
        action (Action): Action object.
        flow_context (Optional[JSONDict]): Optional workflow context.

    Returns:
        str: Action type or 'unknown' if not available.
    """
    if action.get("type"):
        return str(action.get("type"))
    props = action.get("properties") or {}
    if props.get("type"):
        return str(props.get("type"))
    if props.get("actionType"):
        return str(props.get("actionType"))
    if flow_context and flow_context.get("action_type"):
        return str(flow_context.get("action_type"))
    return "unknown"


def is_complex_case(root_cause: str) -> bool:
    """
    Determine whether a root cause requires advanced handling.

    Args:
        root_cause (str): Root cause classification.

    Returns:
        bool: True if case is considered complex, otherwise False.
    """
    return root_cause in {
        ROOT_CAUSE_UNKNOWN,
        ROOT_CAUSE_SCHEMA,
        ROOT_CAUSE_DNS,
        ROOT_CAUSE_CONN_REFUSED,
    }
