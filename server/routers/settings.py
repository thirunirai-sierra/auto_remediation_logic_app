"""
Runtime settings API — read/write configurable thresholds and pipeline behaviour.
Values start from environment variables (or hardcoded defaults) and can be
overridden at runtime; overrides are held in memory and reset on restart.
"""
from __future__ import annotations

import os
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])

# ── Default values (env vars take precedence over hardcoded defaults) ─────────
_DEFAULTS: dict[str, Any] = {
    "AUTO_FIX_CONFIDENCE":      float(os.getenv("AUTO_FIX_CONFIDENCE",      "0.9")),
    "SUGGEST_FIX_CONFIDENCE":   float(os.getenv("SUGGEST_FIX_CONFIDENCE",   "0.7")),
    "MAX_CONCURRENCY":          int(os.getenv("MAX_CONCURRENCY",             "2")),
    "POLL_INTERVAL_SECONDS":    int(os.getenv("CPI_POLL_INTERVAL_SECONDS",   "30")),
    "LOOKBACK_HOURS":           float(os.getenv("LOOKBACK_HOURS",            "0.25")),
    "TOP_N_RUNS":               int(os.getenv("TOP_N_RUNS",                  "20")),
    "RCA_TIMEOUT_SECONDS":      int(os.getenv("RCA_TIMEOUT_SECONDS",         "120")),
    "FIX_TIMEOUT_SECONDS":      int(os.getenv("FIX_TIMEOUT_SECONDS",         "180")),
    "MAX_FIX_RETRIES":          int(os.getenv("MAX_FIX_RETRIES",             "2")),
    "AUTO_FIX_ENABLED":         os.getenv("AUTO_FIX_ENABLED", "true").lower() == "true",
    "ENABLE_RCA":               os.getenv("ENABLE_RCA",        "true").lower() == "true",
    "LOG_ONLY":                 os.getenv("LOG_ONLY",           "false").lower() == "true",
}

# In-memory overrides (survive for the lifetime of the process)
_overrides: dict[str, Any] = {}


# ── Metadata: category, impact, description, unit ─────────────────────────────
SETTING_META: dict[str, dict] = {
    "AUTO_FIX_CONFIDENCE": {
        "label": "Auto-Fix Confidence Threshold",
        "category": "Fix Behaviour",
        "impact": "HIGH",
        "description": "Minimum RCA confidence score (0–1) required to apply a fix automatically without human approval.",
        "takes_effect": "Next incident that reaches the fix-gate decision. Already-in-progress incidents are not affected.",
        "unit": "",
        "type": "float",
        "min": 0.0,
        "max": 1.0,
    },
    "SUGGEST_FIX_CONFIDENCE": {
        "label": "Suggest-Fix Confidence Threshold",
        "category": "Fix Behaviour",
        "impact": "HIGH",
        "description": "Minimum confidence score to queue a fix for human approval instead of discarding it.",
        "takes_effect": "Next incident that reaches the fix-gate decision.",
        "unit": "",
        "type": "float",
        "min": 0.0,
        "max": 1.0,
    },
    "AUTO_FIX_ENABLED": {
        "label": "Enable Autonomous Fixing",
        "category": "Fix Behaviour",
        "impact": "HIGH",
        "description": "When false, all fixes require manual approval regardless of confidence score.",
        "takes_effect": "Immediately — applies to all agents across all in-progress incidents.",
        "unit": "",
        "type": "bool",
    },
    "MAX_FIX_RETRIES": {
        "label": "Max Fix Retries",
        "category": "Fix Behaviour",
        "impact": "MEDIUM",
        "description": "How many times the Fixer agent retries a failed deploy before marking the incident FIX_FAILED.",
        "takes_effect": "Next fix attempt.",
        "unit": "",
        "type": "int",
        "min": 0,
        "max": 5,
    },
    "MAX_CONCURRENCY": {
        "label": "Max Pipeline Concurrency",
        "category": "Throughput",
        "impact": "MEDIUM",
        "description": "Maximum number of incidents the pipeline processes in parallel per polling cycle.",
        "takes_effect": "Next polling cycle.",
        "unit": "incidents",
        "type": "int",
        "min": 1,
        "max": 10,
    },
    "TOP_N_RUNS": {
        "label": "Top N Runs Per Cycle",
        "category": "Throughput",
        "impact": "MEDIUM",
        "description": "Maximum number of failed runs fetched from Log Analytics per polling cycle.",
        "takes_effect": "Next polling cycle.",
        "unit": "runs",
        "type": "int",
        "min": 1,
        "max": 100,
    },
    "POLL_INTERVAL_SECONDS": {
        "label": "Poll Interval",
        "category": "Timing",
        "impact": "LOW",
        "description": "Seconds between Log Analytics / Event Mesh polling cycles.",
        "takes_effect": "After the current sleep interval completes.",
        "unit": "sec",
        "type": "int",
        "min": 10,
        "max": 300,
    },
    "LOOKBACK_HOURS": {
        "label": "Lookback Window",
        "category": "Timing",
        "impact": "LOW",
        "description": "How many hours back the Observer queries Log Analytics for new failures.",
        "takes_effect": "Next polling cycle.",
        "unit": "hours",
        "type": "float",
        "min": 0.1,
        "max": 24.0,
    },
    "RCA_TIMEOUT_SECONDS": {
        "label": "RCA Timeout",
        "category": "Timing",
        "impact": "LOW",
        "description": "Maximum seconds the RCA agent is allowed to run before the incident is marked as timed-out.",
        "takes_effect": "Next RCA execution.",
        "unit": "sec",
        "type": "int",
        "min": 30,
        "max": 600,
    },
    "FIX_TIMEOUT_SECONDS": {
        "label": "Fix Timeout",
        "category": "Timing",
        "impact": "LOW",
        "description": "Maximum seconds the Fixer agent is allowed to run before the incident is marked FIX_FAILED.",
        "takes_effect": "Next fix execution.",
        "unit": "sec",
        "type": "int",
        "min": 30,
        "max": 600,
    },
    "ENABLE_RCA": {
        "label": "Enable RCA",
        "category": "Remediation Policies",
        "impact": "MEDIUM",
        "description": "When false, incidents skip the RCA agent and go directly to the Fixer with rule-based diagnosis.",
        "takes_effect": "Next incident after classification.",
        "unit": "",
        "type": "bool",
    },
    "LOG_ONLY": {
        "label": "Log-Only Mode",
        "category": "Remediation Policies",
        "impact": "HIGH",
        "description": "When true, the pipeline detects and classifies incidents but never applies any fix. Safe for read-only observation.",
        "takes_effect": "Immediately — applies to all agents.",
        "unit": "",
        "type": "bool",
    },
}


def get_value(key: str) -> Any:
    return _overrides.get(key, _DEFAULTS.get(key))


def _coerce(key: str, raw: Any) -> Any:
    meta = SETTING_META.get(key, {})
    t = meta.get("type", "str")
    if t == "float":
        return float(raw)
    if t == "int":
        return int(raw)
    if t == "bool":
        if isinstance(raw, bool):
            return raw
        return str(raw).lower() in ("true", "1", "yes")
    return raw


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("")
async def list_settings():
    """Return all settings with metadata, current value, and default."""
    result = []
    for key, meta in SETTING_META.items():
        result.append({
            "key": key,
            "label": meta["label"],
            "category": meta["category"],
            "impact": meta["impact"],
            "description": meta["description"],
            "takes_effect": meta["takes_effect"],
            "unit": meta.get("unit", ""),
            "type": meta["type"],
            "min": meta.get("min"),
            "max": meta.get("max"),
            "default": _DEFAULTS.get(key),
            "value": get_value(key),
            "overridden": key in _overrides,
        })
    return {"settings": result}


class SettingUpdate(BaseModel):
    value: Any


@router.patch("/{key}")
async def update_setting(key: str, body: SettingUpdate):
    """Override a runtime setting. Persists until server restart."""
    if key not in SETTING_META:
        raise HTTPException(status_code=404, detail=f"Unknown setting: {key}")
    try:
        coerced = _coerce(key, body.value)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid value: {e}")

    meta = SETTING_META[key]
    if "min" in meta and coerced < meta["min"]:
        raise HTTPException(status_code=422, detail=f"Value {coerced} below minimum {meta['min']}")
    if "max" in meta and coerced > meta["max"]:
        raise HTTPException(status_code=422, detail=f"Value {coerced} above maximum {meta['max']}")

    _overrides[key] = coerced
    logger.info("Runtime setting updated: %s = %r", key, coerced)
    return {"key": key, "value": coerced, "overridden": True}


@router.delete("/{key}/reset")
async def reset_setting(key: str):
    """Reset a setting back to its default (env var or hardcoded)."""
    if key not in SETTING_META:
        raise HTTPException(status_code=404, detail=f"Unknown setting: {key}")
    _overrides.pop(key, None)
    return {"key": key, "value": get_value(key), "overridden": False}


# ── Remediation Policies ────────────────────────────────────────────────────────
# Maps each known Azure Logic Apps error type to a default remediation action.
# Actions: AUTO_FIX | RETRY | TICKET_CREATED | AWAITING_APPROVAL

POLICY_ACTIONS = ["AUTO_FIX", "RETRY", "TICKET_CREATED", "AWAITING_APPROVAL"]

_POLICY_DEFAULTS: dict[str, str] = {
    "EXPRESSION_ERROR":         "AUTO_FIX",
    "MAPPING_ERROR":            "AUTO_FIX",
    "TRIGGER_ERROR":            "AUTO_FIX",
    "CONNECTOR_ERROR":          "AUTO_FIX",
    "WORKFLOW_DEFINITION_ERROR":"AUTO_FIX",
    "AUTH_CONFIG_ERROR":        "AUTO_FIX",
    "HTTP_ERROR":               "AUTO_FIX",
    "ODATA_ERROR":              "AUTO_FIX",
    "BACKEND_ERROR":            "TICKET_CREATED",
    "THROTTLING_ERROR":         "TICKET_CREATED",
    "RESOURCE_LIMIT_ERROR":     "TICKET_CREATED",
    "SSL_ERROR":                "TICKET_CREATED",
    "DEPENDENCY_ERROR":         "TICKET_CREATED",
    "AUTH_ERROR":               "AWAITING_APPROVAL",
    "UNKNOWN_ERROR":            "AWAITING_APPROVAL",
    "CONNECTIVITY_ERROR":       "RETRY",
    "TIMEOUT_ERROR":            "RETRY",
}

_POLICY_DESCRIPTIONS: dict[str, str] = {
    "EXPRESSION_ERROR":         "Workflow expression evaluation failed — syntax error or missing property",
    "MAPPING_ERROR":            "Data transformation issue — field mismatch or schema incompatibility",
    "TRIGGER_ERROR":            "Logic App trigger is misconfigured — wrong endpoint or missing parameter",
    "CONNECTOR_ERROR":          "Managed connector configuration issue — wrong connection reference",
    "WORKFLOW_DEFINITION_ERROR":"Invalid Logic App workflow JSON — schema violation or unsupported action",
    "AUTH_CONFIG_ERROR":        "Wrong credential or connection reference — API key or OAuth mismatch",
    "HTTP_ERROR":               "HTTP action received a 4xx client error — bad request or wrong endpoint",
    "ODATA_ERROR":              "OData connector action failed — entity path or query options wrong",
    "BACKEND_ERROR":            "Target service returned HTTP 5xx — downstream system is down",
    "THROTTLING_ERROR":         "Azure or connector rate limit hit — too many requests",
    "RESOURCE_LIMIT_ERROR":     "Run exceeded execution limits — duration, actions, or memory",
    "SSL_ERROR":                "TLS certificate error — expired, untrusted CA, or wrong anchor",
    "DEPENDENCY_ERROR":         "Required Azure resource (Key Vault, Service Bus) unavailable",
    "AUTH_ERROR":               "Authentication failed — OAuth token expired or API key revoked",
    "UNKNOWN_ERROR":            "Unclassified error — human review required before any fix",
    "CONNECTIVITY_ERROR":       "Transient network issue — connection refused or timed out",
    "TIMEOUT_ERROR":            "Action exceeded timeout — transient target slowness",
}

# In-memory policy overrides
_policy_overrides: dict[str, str] = {}


def get_error_policy(error_type: str) -> str:
    """Return the current remediation action for the given error type."""
    return _policy_overrides.get(error_type, _POLICY_DEFAULTS.get(error_type, "AWAITING_APPROVAL"))


@router.get("/policies")
async def list_policies():
    """Return all error-type remediation policies."""
    result = []
    for error_type, default_action in _POLICY_DEFAULTS.items():
        result.append({
            "error_type": error_type,
            "description": _POLICY_DESCRIPTIONS.get(error_type, ""),
            "action": get_error_policy(error_type),
            "default_action": default_action,
            "overridden": error_type in _policy_overrides,
        })
    return {"policies": result}


class PolicyUpdate(BaseModel):
    action: str


@router.patch("/policies/{error_type}")
async def update_policy(error_type: str, body: PolicyUpdate):
    """Override the remediation action for a specific error type."""
    if error_type not in _POLICY_DEFAULTS:
        raise HTTPException(status_code=404, detail=f"Unknown error type: {error_type}")
    if body.action not in POLICY_ACTIONS:
        raise HTTPException(status_code=422, detail=f"Invalid action. Must be one of: {POLICY_ACTIONS}")
    _policy_overrides[error_type] = body.action
    logger.info("Policy updated: %s → %s", error_type, body.action)
    return {"error_type": error_type, "action": body.action, "overridden": True}


@router.delete("/policies/{error_type}/reset")
async def reset_policy(error_type: str):
    """Reset an error type's policy to its default."""
    if error_type not in _POLICY_DEFAULTS:
        raise HTTPException(status_code=404, detail=f"Unknown error type: {error_type}")
    _policy_overrides.pop(error_type, None)
    return {"error_type": error_type, "action": get_error_policy(error_type), "overridden": False}