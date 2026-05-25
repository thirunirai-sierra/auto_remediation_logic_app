"""
Hybrid error analysis engine: rule‑based signal extraction + deterministic recommendations +
LLM enrichment for accurate classification. Returns structured analysis dict for the orchestrator.
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
    """Safely convert any error payload to a string."""
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
    Extract structured diagnostic signals from the error payload.

    Args:
        error_json: Raw error payload from Logic Apps or connector.
        message: Human‑readable error message.

    Returns:
        Dictionary with keys: url, method, timeout_value, missing_field, auth_hint.
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
    Generate deterministic remediation guidance based on error type and signals.

    Args:
        error_type: One of "404", "401", "timeout", "bad_request", "unknown".
        signals: Extracted diagnostic signals.

    Returns:
        Human‑readable recommendation sentence.
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
    Infer HTTP status code from unstructured error text.

    Args:
        text: Raw error message.

    Returns:
        Status code if found, else None.
    """
    if not text:
        return None
    m = re.search(r"(?:status code|returned|response code|http)\s*[:=]?\s*(\d{3})\b", text, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(40[0-9]|41[0-9]|42[0-9]|43[0-9]|44[0-9]|45[0-9]|50[0-9]|502|503|504)\b", text)
    if m:
        return int(m.group(1))
    return None


async def analyze_error(
    error_json: Dict[str, Any],
    settings: Settings,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Perform hybrid error analysis combining rule‑based heuristics and LLM enrichment.

    Steps:
        1. Extract error details (code, message, status code).
        2. Detect root cause using rule‑based heuristics (infer_root_cause).
        3. Extract runtime signals (URL, method, missing field, etc.).
        4. Classify error into one of: 404, 401, timeout, bad_request, unknown.
        5. Generate deterministic recommendation.
        6. Compute initial confidence.
        7. Optionally enrich with LLM for low‑confidence or complex cases.
        8. Return structured analysis dictionary.

    Args:
        error_json: Raw error payload (must contain at least "code" and "message").
        settings: Application configuration.
        flow_context: Optional workflow context (workflow name, etc.).

    Returns:
        Dictionary with keys:
            - error_type (str)
            - root_cause (str)
            - recommendation (str)
            - confidence (float)
            - signals (dict)
            - exact_error_code (str|None)
            - exact_error_message (str|None)
            - analysis_generated_at_utc (str)
    """
    logger.info("=" * 60)
    logger.info("ANALYZER: Starting error analysis")
    logger.info("=" * 60)

    code = str(error_json.get("code") or "")
    message = _text_blob(error_json.get("message"))
    status_code = error_json.get("statusCode")
    if status_code is None:
        status_code = _infer_http_status_from_text(message)

    logger.info(f"ANALYZER Input:")
    logger.info(f"   Error code: {code}")
    logger.info(f"   Error message: {message[:300]}...")
    logger.info(f"   Status code: {status_code}")

    # 1. Rule‑based root cause (using shared library)
    root_cause = infer_root_cause(code, message)
    logger.info(f"Rule-based root cause: {root_cause}")

    # 2. Extract runtime signals
    signals = _extract_signals(error_json, message)
    logger.info(f"Extracted signals: {json.dumps(signals, default=str)}")

    # 3. Determine error category
    error_type = "unknown"
    if status_code == 404 or "404" in message or "not found" in message.lower():
        error_type = "404"
    elif status_code in (401, 403) or "unauthorized" in message.lower() or "forbidden" in message.lower():
        error_type = "401"
    elif status_code in (408, 504) or "timeout" in message.lower() or "timed out" in message.lower():
        error_type = "timeout"
    elif status_code == 400 or "bad request" in message.lower() or "invalid" in message.lower():
        error_type = "bad_request"

    logger.info(f"Rule-based error type: {error_type}")

    # 4. Generate deterministic recommendation
    recommendation = _dynamic_recommendation(error_type, signals)

    # 5. Compute initial confidence
    confidence = confidence_score(root_cause, code, message)
    logger.info(f"Rule-based confidence: {confidence}")

    # 6. LLM enrichment for ambiguous cases
    use_llm = (confidence < 0.7 or error_type == "unknown") and is_complex_case(root_cause)
    if use_llm:
        logger.info("LLM enrichment triggered (confidence low or error unknown)")
        try:
            llm = AICoreLLMClient.from_env()
            system_prompt = (
                "You are an Azure Logic Apps error analyst. Given the error details, "
                "return a JSON object with keys: error_type, root_cause, exact_issue, recommendation, confidence (0-1). "
                "error_type must be one of: '404', '401', 'timeout', 'bad_request'."
            )
            user_prompt = f"""
Error type (rule-based): {error_type}
Error message: {message}
Error code: {code}
Status code: {status_code}
Root cause (rule): {root_cause}
Extracted signals: {json.dumps(signals)}
"""
            llm_result = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)

            if llm_result:
                logger.info(f"LLM enrichment applied: {json.dumps(llm_result)}")
                new_error_type = llm_result.get("error_type")
                if new_error_type and new_error_type in ("404", "401", "timeout", "bad_request"):
                    error_type = new_error_type
                root_cause = llm_result.get("root_cause", root_cause)
                recommendation = llm_result.get("recommendation", recommendation)
                confidence = float(llm_result.get("confidence", confidence))
            else:
                logger.warning("LLM returned no result, keeping rule-based values")
        except Exception as e:
            logger.error(f"LLM enrichment failed: {e}", exc_info=True)
    else:
        logger.info("LLM enrichment skipped (confidence sufficient)")

    # 7. Build final result
    result = {
        "error_type": error_type,
        "root_cause": root_cause,
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": signals,
        "exact_error_code": code or None,
        "exact_error_message": message or None,
        "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("ANALYZER Output:")
    logger.info(f"   error_type: {error_type}")
    logger.info(f"   root_cause: {root_cause}")
    logger.info(f"   confidence: {confidence}")
    logger.info(f"   recommendation: {recommendation[:200]}...")
    logger.info("=" * 60)

    return result


async def classify_error(
    error_message: str,
    error_code: str,
    status_code: Optional[int],
    settings: Settings,
) -> str:
    """
    Classify an error into one of four categories using a 4‑tier fallback strategy.

    Tiers:
        1. Rule‑based keyword and status matching.
        2. LLM classification (when signals are present).
        3. Pattern fallback (simple keyword matching).
        4. Default to "bad_request".

    Args:
        error_message: Human‑readable error message.
        error_code: Connector or platform error code.
        status_code: HTTP status code (if available).
        settings: Application configuration (unused but kept for consistency).

    Returns:
        One of: "404", "401", "timeout", "bad_request".
    """
    logger.info("=" * 80)
    logger.info("CLASSIFIER AGENT")
    logger.info("=" * 80)
    logger.info(f"INPUT: message='{error_message[:250]}', code='{error_code}', status={status_code}")
    logger.info("")

    msg_lower = error_message.lower()
    code_lower = error_code.lower()

    # Tier 1: Rule‑based
    logger.info("TIER 1: Rule-Based Classification")
    tier1 = {
        "404": (["404", "not found", "does not exist", "resource not found"], 404),
        "401": (["401", "403", "unauthorized", "forbidden", "permission"], (401, 403)),
        "timeout": (["timeout", "timed out", "408", "504", "deadline exceeded"], (408, 504)),
        "bad_request": (["400", "bad request", "invalid", "null", "contains", "float", "div"], 400),
    }
    for cat, (keywords, status) in tier1.items():
        kw_match = any(kw in msg_lower or kw in code_lower for kw in keywords)
        stat_match = (status_code == status) if isinstance(status, int) else (status_code in status)
        if kw_match or stat_match:
            logger.info(f"   MATCHED: {cat} (keywords={kw_match}, status={stat_match})")
            logger.info(f"OUTPUT: {cat} (Tier 1)")
            return cat
    logger.info("   No rule matched\n")

    # Tier 2: LLM (only if there is at least some signal)
    if (error_message.strip() or error_code.strip() or status_code):
        logger.info("TIER 2: LLM Classification")
        try:
            llm = AICoreLLMClient.from_env()
            system_prompt = (
                "You are an Azure Logic Apps error classification expert.\n"
                "Return JSON: {\"category\": \"404|401|timeout|bad_request\"}\n"
                "NEVER return 'unknown'."
            )
            user_prompt = f"Error: {error_message}\nCode: {error_code}\nHTTP status: {status_code}"
            response = await llm.complete_json(system_prompt=system_prompt, user_prompt=user_prompt, required_keys=["category"])
            if response and (cat := response.get("category", "").strip().lower()) in ("404", "401", "timeout", "bad_request"):
                logger.info(f"OUTPUT: {cat} (Tier 2 - LLM)")
                return cat
        except Exception as e:
            logger.warning(f"LLM classification failed: {e}")
        logger.info("")

    # Tier 3: Pattern fallback
    logger.info("TIER 3: Pattern Fallback")
    fallback = [
        ("bad_request", ["float", "div", "contains", "null", "invalid", "expression", "template"]),
        ("timeout", ["timeout", "timed out", "deadline", "408", "504"]),
        ("404", ["not found", "does not exist", "404"]),
        ("401", ["unauthorized", "forbidden", "401", "403"]),
    ]
    for cat, kw_list in fallback:
        if any(kw in msg_lower or kw in code_lower for kw in kw_list):
            logger.info(f"OUTPUT: {cat} (Tier 3 - Pattern Fallback)")
            return cat

    # Tier 4: Default
    logger.info("TIER 4: Default Fallback -> bad_request")
    return "bad_request"