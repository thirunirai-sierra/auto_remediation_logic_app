from __future__ import annotations

from typing import Dict, Optional

from logic_app_remediator.rca.analyzer import (
    ROOT_CAUSE_AUTH,
    ROOT_CAUSE_CONN_REFUSED,
    ROOT_CAUSE_DNS,
    ROOT_CAUSE_NOT_FOUND,
    ROOT_CAUSE_NULL,
    ROOT_CAUSE_SCHEMA,
    ROOT_CAUSE_THROTTLING,
    ROOT_CAUSE_TIMEOUT,
    ROOT_CAUSE_TRIGGER_DISABLED,
    ROOT_CAUSE_UNKNOWN,
    detect_null_source,
)
from logic_app_remediator.rca.models.rca_model import JSONDict


def recommendation_for_root_cause(root_cause: str, flow_context: Optional[JSONDict] = None) -> str:
    base: Dict[str, str] = {
        ROOT_CAUSE_NULL: "Use coalesce() or add null check before using value; validate upstream action output shape.",
        ROOT_CAUSE_TIMEOUT: "Increase timeout and add retry policy with exponential backoff and jitter.",
        ROOT_CAUSE_AUTH: "Refresh token/connection secret and verify managed identity/service principal RBAC scope.",
        ROOT_CAUSE_DNS: "Fix hostname and DNS resolution path; verify private endpoint or VNet DNS configuration.",
        ROOT_CAUSE_CONN_REFUSED: "Verify service health, network path, NSG/firewall rules, and correct destination port.",
        ROOT_CAUSE_SCHEMA: "Align payload with API contract, enforce required fields, and validate data types before call.",
        ROOT_CAUSE_NOT_FOUND: "Validate resource id/path/API version; ensure target resource exists in correct subscription/rg.",
        ROOT_CAUSE_THROTTLING: "Implement client-side rate limit and honor Retry-After with bounded retries.",
        ROOT_CAUSE_TRIGGER_DISABLED: "Enable the Logic App trigger before invoking run, or use an enabled trigger name.",
        ROOT_CAUSE_UNKNOWN: "Inspect action inputs/outputs and dependency logs; enable debug telemetry for next run.",
    }
    rec = base.get(root_cause, base[ROOT_CAUSE_UNKNOWN])
    if root_cause == ROOT_CAUSE_NULL:
        src = detect_null_source(message="", flow_context=flow_context)
        if src:
            rec += f" Null source candidate: {src}."
    return rec


def solution_for_root_cause(root_cause: str, flow_context: Optional[JSONDict] = None) -> str:
    return recommendation_for_root_cause(root_cause, flow_context)
