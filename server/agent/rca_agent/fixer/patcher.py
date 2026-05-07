from __future__ import annotations

import json
import re
from typing import Optional

from agent.observer.error_detector import (
    ROOT_CAUSE_NULL,
    ROOT_CAUSE_SCHEMA,
    ROOT_CAUSE_TIMEOUT,
)
from agent.observer.models.rca_model import JSONDict


def build_auto_remediation_patch(workflow_resource: JSONDict, rca: JSONDict, action_name: str) -> JSONDict:
    patched = json.loads(json.dumps(workflow_resource))
    definition = (patched.get("properties") or {}).get("definition") or {}
    actions = definition.get("actions") or {}
    node = _find_action_node(actions, action_name)
    if not isinstance(node, dict):
        return patched

    cause = str(rca.get("root_cause") or "")
    node_type = str(node.get("type") or "").lower()

    if cause == ROOT_CAUSE_TIMEOUT:
        policy = {"type": "fixed", "count": 3, "interval": "PT30S"}
        node["retryPolicy"] = policy
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                inputs["retryPolicy"] = policy
    elif cause == ROOT_CAUSE_NULL and node_type == "if":
        expr = node.get("expression")
        if isinstance(expr, str) and "contains(" in expr and "coalesce(" not in expr:
            node["expression"] = re.sub(
                r"contains\(([^,]+),",
                r"contains(coalesce(\1, createArray()),",
                expr,
            )
    elif cause == ROOT_CAUSE_SCHEMA and node_type in ("http", "httpwebhook"):
        inputs2 = node.setdefault("inputs", {})
        if isinstance(inputs2, dict) and isinstance(inputs2.get("body"), str):
            inputs2["body"] = {}

    return patched


def _find_action_node(actions: JSONDict, target_name: str) -> Optional[JSONDict]:
    if target_name in actions and isinstance(actions[target_name], dict):
        return actions[target_name]
    for _, node in actions.items():
        if not isinstance(node, dict):
            continue
        nested = node.get("actions")
        if isinstance(nested, dict):
            hit = _find_action_node(nested, target_name)
            if hit is not None:
                return hit
    return None
