"""
Error analysis with dynamic recommendation generation and optional Azure OpenAI enrichment.
"""

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from config import Settings, get_settings


def _infer_http_status_from_text(text: str) -> Optional[int]:
    """Pull 3-digit HTTP status from common Logic Apps / connector phrasing."""
    if not text:
        return None
    m = re.search(
        r"(?:status code|returned|response code|http)\s*[:=]?\s*(\d{3})\b",
        text,
        re.I,
    )
    if m:
        return int(m.group(1))
    m = re.search(r"\b(40[0-9]|41[0-9]|42[0-9]|43[0-9]|44[0-9]|45[0-9]|50[0-9]|502|503|504)\b", text)
    if m:
        return int(m.group(1))
    return None


def _text_blob(err: Any) -> str:
    if err is None:
        return ""
    if isinstance(err, str):
        return err
    try:
        return json.dumps(err, default=str)
    except TypeError:
        return str(err)


def _extract_signals(error_json: Dict[str, Any], message: str) -> Dict[str, Any]:
    """
    Extract actionable, real-time hints from current error payload text.
    """
    blob = _text_blob(error_json).replace("\\/", "/")

    def find(pattern: str, src: str, flags: int = re.I) -> Optional[str]:
        m = re.search(pattern, src, flags)
        if not m:
            return None
        # Some patterns use a capture group, others match the full token.
        if m.lastindex and m.lastindex >= 1:
            return m.group(1).strip()
        return m.group(0).strip()

    url = find(r"https?://[^\s\"'<>]+", blob, flags=re.I)
    method = find(r"\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b", blob, flags=re.I)
    timeout_value = (
        find(r"(?:timed?\s*out\s*after|timeout(?:\s*of)?)\s*[:=]?\s*([0-9]+(?:ms|s|m)?)", message)
        or find(r'"timeout"\s*:\s*"([^"]+)"', blob)
    )
    missing_field = (
        find(r"(?:missing|required)\s+(?:field|property|parameter)\s*[:=]?\s*['\"]?([A-Za-z0-9_.-]+)", message)
        or find(r"'([A-Za-z0-9_.-]+)'\s+is required", message)
        or find(r'"([A-Za-z0-9_.-]+)"\s*:\s*\[\s*"is required"', blob)
    )

    auth_hint = (
        "token"
        if re.search(r"\b(token|jwt|bearer|signature)\b", message, re.I)
        else "rbac"
        if re.search(r"\b(forbidden|insufficient|permission|scope|role)\b", message, re.I)
        else None
    )

    return {
        "url": url,
        "method": method,
        "timeout_value": timeout_value,
        "missing_field": missing_field,
        "auth_hint": auth_hint,
    }


def _dynamic_recommendation(error_type: str, signals: Dict[str, Any]) -> str:
    """
    Build recommendation from live run evidence instead of static templates.
    """
    url = signals.get("url")
    method = signals.get("method")
    timeout_value = signals.get("timeout_value")
    missing_field = signals.get("missing_field")
    auth_hint = signals.get("auth_hint")

    parts = []
    if error_type == "404":
        if url:
            parts.append(f"Target endpoint currently failing: {url}.")
            parts.append("Verify host/path and API version, then switch to a known-good fallback endpoint if needed.")
        else:
            parts.append("Endpoint appears unresolved (404). Validate URI host/path and API route mapping.")
    elif error_type == "401":
        if auth_hint == "rbac":
            parts.append("Authorization failure detected; validate RBAC role assignment and token scope/audience.")
        else:
            parts.append("Authentication failure detected; refresh token/API key or connection secret.")
        if method or url:
            parts.append(f"Failing call context: method={method or 'unknown'}, url={url or 'unknown'}.")
    elif error_type == "timeout":
        parts.append("Call timed out; increase request timeout and apply bounded retry policy.")
        if timeout_value:
            parts.append(f"Observed timeout signal: {timeout_value}.")
        if url:
            parts.append(f"Investigate latency/dependency for endpoint {url}.")
    elif error_type == "bad_request":
        parts.append("Payload/schema mismatch detected (400); validate request body against API contract.")
        if missing_field:
            parts.append(f"Populate required field: {missing_field}.")
        if method or url:
            parts.append(f"Failing call context: method={method or 'unknown'}, url={url or 'unknown'}.")
    else:
        parts.append("No deterministic pattern matched; inspect action inputs/outputs and connector-specific diagnostics.")
        if method or url:
            parts.append(f"Current call context: method={method or 'unknown'}, url={url or 'unknown'}.")

    return " ".join(parts)


def _root_cause_from_exact(code: str, message: str) -> str:
    c = (code or "").upper()
    m = (message or "").upper()
    if "UNRESOLVABLEHOSTNAME" in c or "COULD NOT BE RESOLVED" in m or "NAME OR SERVICE NOT KNOWN" in m:
        return "dns_resolution_error"
    if "CONNECTIONREFUSED" in c or "ECONNREFUSED" in m:
        return "connection_refused"
    if "CERTIFICATE" in c or "SSL" in m or "TLS" in m:
        return "tls_or_certificate_error"
    if "THROTTL" in c or "429" in m:
        return "throttling"
    if "UNAUTHORIZED" in c or "FORBIDDEN" in c:
        return "auth_or_authorization_error"
    if "TIMEOUT" in c or "TIMED OUT" in m:
        return "timeout"
    if "BADREQUEST" in c or "INVALID" in c:
        return "payload_or_schema_error"
    if "NOTFOUND" in c or "NOT FOUND" in m:
        return "not_found"
    return "unknown"


def analyze_error(
    error_json: Dict[str, Any],
    settings: Optional[Settings] = None,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Classify Logic App / HTTP style errors.

    When settings.rag_enabled and Azure OpenAI is configured, runs RAG over
    knowledge/chunks.jsonl and synthesizes exact_error_in_flow from live FLOW_CONTEXT.

    Returns:
        Analysis dict including error_type, fix_type, recommendation, exact fields,
        and optional rag_* keys.
    """
    settings = settings or get_settings()

    code = str(error_json.get("code") or "")
    message = _text_blob(error_json.get("message"))
    status_code = error_json.get("statusCode")
    signals = _extract_signals(error_json, message)
    root_cause = _root_cause_from_exact(code, message)
    # Logic Apps often embed HTTP status only inside the message string
    inferred = _infer_http_status_from_text(message)
    if status_code is None and inferred is not None:
        status_code = inferred
    combined = f"{code} {message} {status_code}".upper()

    common = {
        "exact_error_code": code or None,
        "exact_error_message": message or None,
        "root_cause": root_cause,
    }

    # Handle connector/runtime exact error codes first
    if root_cause == "dns_resolution_error":
        err_type = "404"
        base = {
            "error_type": err_type,
            "fix_type": "replace_endpoint",
            "recommendation": (
                "Host cannot be resolved. Update HTTP URI host/DNS and verify environment DNS/network path."
            ),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    # HTTP status from outputs
    if status_code == 404 or "404" in combined or "NOT FOUND" in combined:
        err_type = "404"
        base = {
            "error_type": err_type,
            "fix_type": "replace_endpoint",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if status_code == 403 or "403" in combined or "FORBIDDEN" in combined:
        err_type = "401"
        base = {
            "error_type": err_type,
            "fix_type": "refresh_auth",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code or 403},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if status_code == 401 or "401" in combined or "UNAUTHORIZED" in combined:
        err_type = "401"
        base = {
            "error_type": err_type,
            "fix_type": "refresh_auth",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if (
        status_code == 408
        or "TIMEOUT" in combined
        or "TIMED OUT" in combined
        or "REQUEST TIMEOUT" in combined
        or "504" in combined
        or status_code == 504
    ):
        err_type = "timeout"
        base = {
            "error_type": err_type,
            "fix_type": "add_retry",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if (
        status_code == 400
        or "400" in combined
        or "BAD REQUEST" in combined
        or "SCHEMA" in combined
        or "INVALID" in combined
        or "MALFORMED" in combined
    ):
        err_type = "bad_request"
        base = {
            "error_type": err_type,
            "fix_type": "fix_payload",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    err_type = "unknown"
    base = {
        "error_type": err_type,
        "fix_type": "manual_review",
        "recommendation": _dynamic_recommendation(err_type, signals),
        "raw_signals": {"code": code, "statusCode": status_code},
        "dynamic_signals": signals,
        "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
        **common,
    }
    return _finalize_analysis(error_json, base, settings, flow_context)


def _finalize_analysis(
    error_json: Dict[str, Any],
    base: Dict[str, Any],
    settings: Settings,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(base)
    if settings.rag_enabled:
        if settings.azure_openai_endpoint and settings.azure_openai_api_key:
            from agent import rag

            rag_out = rag.analyze_with_rag(
                error_json, flow_context or {}, out, settings
            )
            if rag_out:
                out.update(rag_out)
        return out
    if settings.azure_openai_endpoint and settings.azure_openai_api_key:
        out = _maybe_openai_enrich(error_json, out, settings)
    return out


def _maybe_openai_enrich(
    error_json: Dict[str, Any],
    base: Dict[str, Any],
    settings: Settings,
) -> Dict[str, Any]:
    if not settings.azure_openai_endpoint or not settings.azure_openai_api_key:
        return base

    try:
        url = (
            f"{settings.azure_openai_endpoint.rstrip('/')}/openai/deployments/"
            f"{settings.azure_openai_deployment}/chat/completions"
            f"?api-version={settings.azure_openai_api_version}"
        )
        system = (
            "You are an SRE assistant. Given a Logic Apps action error JSON and baseline analysis, "
            "return ONLY compact JSON with keys: "
            '{"error_type":"404|401|timeout|bad_request|unknown",'
            '"fix_type":"string","recommendation":"string","confidence":0.0}. '
            "Recommendation must be evidence-based, specific to the provided payload, "
            "and include concrete next action. No markdown."
        )
        payload = {
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"error_json": error_json, "baseline": base},
                        default=str,
                    )[:14000],
                },
            ],
            "temperature": 0.1,
            "max_tokens": 400,
        }
        r = requests.post(
            url,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            return base
        parsed = json.loads(m.group())
        merged = dict(base)
        for k in ("error_type", "fix_type", "recommendation"):
            if k in parsed and parsed[k]:
                merged[k] = parsed[k]
        if "confidence" in parsed:
            merged["confidence"] = parsed["confidence"]
        merged["openai_enriched"] = True
        return merged
    except Exception:
        return base
# NOTE: legacy block below retained to avoid behavior changes.

import re
from typing import Optional, Sequence, Tuple

from agent.observer.models.rca_model import Action, JSONDict
from agent.observer.utils.helpers import extract_missing_field, normalize_action_name

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
    return root_cause in {
        ROOT_CAUSE_UNKNOWN,
        ROOT_CAUSE_SCHEMA,
        ROOT_CAUSE_DNS,
        ROOT_CAUSE_CONN_REFUSED,
    }
