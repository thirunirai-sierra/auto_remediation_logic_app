# server/services/agents/classifier/analyzer.py
"""
Hybrid error analysis engine: rule‑based signal extraction + deterministic recommendations +
optional LLM enrichment. Returns structured analysis dict for the orchestrator.
"""
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from utils.llm_client import AICoreLLMClient
from utils.error_detector import (
    infer_root_cause,
    extract_exact_issue,
    confidence_score,
    is_complex_case,
)
from config import Settings

logger = logging.getLogger(__name__)


def _text_blob(err: Any) -> str:
    """
    Safely convert any error payload to a string.

    Args:
        err (Any): The error payload, which can be None, str, dict, or any object.

    Returns:
        str: A string representation of the error.
    """
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
    Extract key diagnostic signals from an error payload.

    Args:
        error_json (Dict[str, Any]): The raw error object, typically from an API or connector.
        message (str): The textual error message.

    Returns:
        Dict[str, Any]: Extracted signals including:
            - url: The endpoint URL involved.
            - method: HTTP method (GET, POST, etc.).
            - timeout_value: Observed timeout value if present.
            - missing_field: Required field missing in request payload.
            - auth_hint: Authentication or authorization hints.
    """

    blob = _text_blob(error_json).replace("\\/", "/")

    def find(pattern: str, src: str, flags: int = re.I) -> Optional[str]:
        m = re.search(pattern, src, flags)
        if not m:
            return None
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
        or find(r"'([^']+)'\s+is required", message)
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
    Generate a deterministic recommendation based on error type and extracted signals.

    Args:
        error_type (str): The category of error (e.g., '404', '401', 'timeout', 'bad_request').
        signals (Dict[str, Any]): Extracted runtime signals.

    Returns:
        str: Actionable recommendation string.
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


def _infer_http_status_from_text(text: str) -> Optional[int]:
    """
    Infer HTTP status code from error message text if not explicitly provided.

    Args:
        text (str): The error message text.

    Returns:
        Optional[int]: HTTP status code if found, else None.
    """
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


def _root_cause_from_exact(code: str, message: str) -> str:
    """
    Map exact connector error codes or messages to a deterministic root cause.

    Args:
        code (str): Error code from connector or API.
        message (str): Error message text.

    Returns:
        str: Root cause identifier (e.g., 'dns_resolution_error', 'timeout', 'auth_or_authorization_error').
    """
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

async def analyze_error(
    error_json: Dict[str, Any],
    settings: Settings,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Perform hybrid error analysis combining rule-based heuristics, signal extraction,
    and optional LLM enrichment for complex cases.

    Args:
        error_json (Dict[str, Any]): The raw error object from a connector or API.
        settings (Settings): Global system/configuration settings.
        flow_context (Optional[Dict[str, Any]]): Optional contextual information about the flow.

    Returns:
        Dict[str, Any]: Structured error analysis containing:
            - error_type: Categorized error type ('404', '401', 'timeout', 'bad_request', 'unknown').
            - root_cause: Determined root cause string.
            - recommendation: Actionable recommendation.
            - confidence: Confidence score (0-1) in the root cause and recommendation.
            - signals: Extracted diagnostic signals.
            - exact_error_code: Original error code.
            - exact_error_message: Original error message.
            - analysis_generated_at_utc: ISO timestamp of analysis generation.
    """
    code = str(error_json.get("code") or "")
    message = _text_blob(error_json.get("message"))
    status_code = error_json.get("statusCode")
    if status_code is None:
        status_code = _infer_http_status_from_text(message)

    # 1. Rule‑based root cause from exact codes
    root_cause = _root_cause_from_exact(code, message)
    # 2. Extract runtime signals
    signals = _extract_signals(error_json, message)
    # 3. Determine error category (404,401,timeout,bad_request,unknown)
    error_type = "unknown"
    if status_code == 404 or "404" in message or "not found" in message.lower():
        error_type = "404"
    elif status_code in (401,403) or "unauthorized" in message.lower() or "forbidden" in message.lower():
        error_type = "401"
    elif status_code in (408,504) or "timeout" in message.lower() or "timed out" in message.lower():
        error_type = "timeout"
    elif status_code == 400 or "bad request" in message.lower() or "invalid" in message.lower():
        error_type = "bad_request"

    # 4. Generate recommendation from signals (deterministic)
    recommendation = _dynamic_recommendation(error_type, signals)
    # 5. Compute confidence using the rule‑based heuristic
    confidence = confidence_score(root_cause, code, message)

    # 6. If confidence is low and this is a complex case, call LLM for enrichment
    if confidence < 0.7 and is_complex_case(root_cause):
        try:
            llm = AICoreLLMClient.from_env()
            system_prompt = (
                "You are an Azure Logic Apps error analyst. Given the error details, "
                "return a JSON object with keys: root_cause, exact_issue, recommendation, confidence (0-1)."
            )
            user_prompt = f"""
Error type: {error_type}
Error message: {message}
Error code: {code}
Root cause (rule): {root_cause}
Extracted signals: {json.dumps(signals)}
"""
            llm_result = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
            if llm_result:
                # Merge LLM output – but keep deterministic signals
                root_cause = llm_result.get("root_cause", root_cause)
                recommendation = llm_result.get("recommendation", recommendation)
                confidence = float(llm_result.get("confidence", confidence))
                logger.info("LLM enrichment applied for complex case")
        except Exception as e:
            logger.warning("LLM enrichment failed, using rule‑based result: %s", e)

    # 7. Build the final structured analysis dict
    return {
        "error_type": error_type,
        "root_cause": root_cause,
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": signals,
        "exact_error_code": code or None,
        "exact_error_message": message or None,
        "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

async def classify_error(
    error_message: str,
    error_code: str,
    status_code: Optional[int],
    settings: Settings,
) -> str:
    """
    Fast, lightweight error classifier returning only the error category.
    Used for routing and telemetry, not for detailed recommendations.

    Args:
        error_message (str): Raw error message text.
        error_code (str): Error code string.
        status_code (Optional[int]): HTTP status code if available.
        settings (Settings): System/configuration settings.

    Returns:
        str: Categorized error type: '404', '401', 'timeout', 'bad_request', or 'unknown'.
    """
    # Rule‑based fast path
    msg_lower = error_message.lower()
    if "404" in msg_lower or "not found" in msg_lower:
        return "404"
    if "401" in msg_lower or "unauthorized" in msg_lower:
        return "401"
    if "timeout" in msg_lower or "timed out" in msg_lower:
        return "timeout"
    if "400" in msg_lower or "bad request" in msg_lower or "invalid" in msg_lower:
        return "bad_request"

    # LLM fallback for ambiguous cases
    try:
        llm = AICoreLLMClient.from_env()
        system_prompt = (
    "You are an expert Azure Logic Apps error classification engine.\n"
    "\n"
    "TASK:\n"
    "Classify the given error into exactly ONE of the following categories:\n"
    " - 404 (resource not found)\n"
    " - 401 (authentication or authorization failure)\n"
    " - timeout (request timeout, gateway timeout, or delayed response)\n"
    " - bad_request (invalid input, schema error, malformed request, or missing fields)\n"
    "\n"
    "STRICT RULES:\n"
    "1. You MUST return exactly one label from the list above.\n"
    "2. Do NOT return explanations, punctuation, JSON, or extra text.\n"
    "3. Do NOT return 'unknown' or any other category outside the list.\n"
    "4. Output must be a single lowercase token or numeric code exactly as specified.\n"
    "5. If multiple categories apply, choose the MOST specific root cause.\n"
    "6. If the error is ambiguous, infer the closest match rather than refusing.\n"
    "\n"
    "OUTPUT FORMAT (MANDATORY):\n"
    "Return ONLY one of:\n"
    "404\n"
    "401\n"
    "timeout\n"
    "bad_request"
)
        user_prompt = f"Error message: {error_message}\nError code: {error_code}\nHTTP status: {status_code}"
        response = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
        if response and isinstance(response, dict) and "category" in response:
            cat = response["category"]
            if cat in ("404", "401", "timeout", "bad_request", "unknown"):
                return cat
    except Exception as e:
        logger.warning("LLM classification failed: %s", e)
    return "unknown"