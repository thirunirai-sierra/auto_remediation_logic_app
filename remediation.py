"""
Apply minimal patches to a Logic App workflow definition for a single failed action.
"""

import copy
import re
from typing import Any, Dict, List, Optional, Tuple

from config import Settings

import logging

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def _fix_compose_add_string_expression(
    node: Dict[str, Any], analysis: Optional[Dict[str, Any]]
) -> bool:
    """
    Targeted fix for InvalidTemplate in Compose:
    add() second parameter is a string -> wrap with int(...).
    """
    if not isinstance(node, dict):
        return False
    inp = node.get("inputs")
    if not isinstance(inp, str):
        return False
    msg = str((analysis or {}).get("exact_error_message") or "")
    if "function 'add' expects its second parameter" not in msg:
        return False
    if "type 'String'" not in msg and "type \"String\"" not in msg:
        return False
    # Conservative transform: add(X, '2') => add(X, int('2'))
    pat = r"add\((.*?),\s*'([^']+)'\s*\)"
    new_inp, count = re.subn(pat, r"add(\1, int('\2'))", inp)
    if count > 0:
        node["inputs"] = new_inp
        return True
    return False


def locate_action_node(
    definition: Dict[str, Any], action_name: str
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Find action by name in top-level actions or nested scope/for-each containers.
    Returns (path_keys, node_ref) where node_ref is the dict to mutate.
    """

    def walk(actions_obj: Any, path_prefix: List[str]) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        if not isinstance(actions_obj, dict):
            return None
        if action_name in actions_obj:
            return path_prefix + [action_name], actions_obj[action_name]
        for parent_name, parent in actions_obj.items():
            if not isinstance(parent, dict):
                continue
            inner = parent.get("actions")
            if isinstance(inner, dict):
                hit = walk(inner, path_prefix + [parent_name, "actions"])
                if hit:
                    return hit
            else_block = parent.get("else")
            if isinstance(else_block, dict):
                inner_else = else_block.get("actions")
                if isinstance(inner_else, dict):
                    hit = walk(inner_else, path_prefix + [parent_name, "else", "actions"])
                    if hit:
                        return hit
        return None

    actions = definition.get("actions")
    if not isinstance(actions, dict):
        raise ValueError("Invalid definition: missing actions")

    found = walk(actions, ["actions"])
    if not found:
        raise KeyError(f"Action '{action_name}' not found in workflow definition.")
    return found


def apply_remediation_patch(
    workflow_resource: Dict[str, Any],
    action_name: str,
    error_type: str,
    settings: Settings,
    analysis: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deep-copy workflow resource and patch only the target action (minimal change).
    """
    patched = copy.deepcopy(workflow_resource)
    definition = patched.get("properties", {}).get("definition")
    if not isinstance(definition, dict):
        raise ValueError("Workflow resource missing properties.definition")

    _path, node = locate_action_node(definition, action_name)
    if not isinstance(node, dict):
        raise ValueError(f"Action node {action_name} is not an object")

    node_type = (node.get("type") or "").lower()

    if error_type == "404":
        # HTTP / HttpWebhook style actions use 'inputs' with method, uri, headers, body
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                inputs["uri"] = settings.fallback_http_url

    elif error_type == "401":
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                headers = inputs.setdefault("headers", {})
                if isinstance(headers, dict) and settings.auth_header_value:
                    headers[settings.auth_header_name] = settings.auth_header_value

    elif error_type == "timeout":
        # Action-level retry; HTTP inputs may also carry retryPolicy (consumption / standard)
        policy = {
            "type": "fixed",
            "count": 3,
            "interval": "PT30S",
        }
        node["retryPolicy"] = policy
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                inputs["retryPolicy"] = policy
                to = settings.http_timeout_iso or "PT2M"
                inputs.setdefault("runtimeConfiguration", {})
                rc = inputs["runtimeConfiguration"]
                if isinstance(rc, dict):
                    rc.setdefault("contentTransfer", {})
                    ct = rc["contentTransfer"]
                    if isinstance(ct, dict):
                        ct["transferMode"] = ct.get("transferMode") or "Chunked"
                    # Request timeout hint (honored on supported runtimes)
                    ro = rc.setdefault("requestOptions", {})
                    if isinstance(ro, dict):
                        ro.setdefault("timeout", to)

    elif error_type == "bad_request":
        if node_type == "compose":
            if _fix_compose_add_string_expression(node, analysis):
                return patched
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                # Ensure body is object not string when common mistake
                body = inputs.get("body")
                if isinstance(body, str):
                    try:
                        import json

                        inputs["body"] = json.loads(body)
                    except Exception:
                        inputs["body"] = {}

    return patched


def strip_read_only_for_put(workflow_get_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare GET response for PUT: drop read-only properties that commonly break updates.
    """
    body = copy.deepcopy(workflow_get_response)
    props = body.get("properties")
    if isinstance(props, dict):
        for ro in (
            "createdTime",
            "changedTime",
            "state",
            "version",
            "accessEndpoint",
            "endpointsConfiguration",
            "integrationAccount",
            "integrationServiceEnvironment",
        ):
            props.pop(ro, None)
    return body


def _contains_needs_null_guard(expr: str) -> bool:
    x = (expr or "").lower()
    return "contains(" in x and "coalesce(" not in x


def _add_null_guard_to_contains(expr: str) -> Tuple[str, bool]:
    """Wrap first argument of contains(a, b) with coalesce(a, '') when safe."""
    if not _contains_needs_null_guard(expr):
        return expr, False
    pattern = re.compile(r"contains\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", re.IGNORECASE)

    def repl(match: re.Match) -> str:
        first = match.group(1).strip()
        second = match.group(2).strip()
        guarded = f"coalesce({first}, '')"
        return f"contains({guarded}, {second})"

    updated, count = pattern.subn(repl, expr)
    return updated, count > 0


def _recursively_patch_contains(node: Any) -> bool:
    """Patch contains() in string fields under node (single action subtree)."""
    changed = False
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if isinstance(value, str):
                patched, did = _add_null_guard_to_contains(value)
                if did:
                    node[key] = patched
                    changed = True
            elif isinstance(value, (dict, list)):
                if _recursively_patch_contains(value):
                    changed = True
    elif isinstance(node, list):
        for i, value in enumerate(node):
            if isinstance(value, str):
                patched, did = _add_null_guard_to_contains(value)
                if did:
                    node[i] = patched
                    changed = True
            elif isinstance(value, (dict, list)):
                if _recursively_patch_contains(value):
                    changed = True
    return changed


def _signals_contains_null_issue(
    error_json: Optional[Dict[str, Any]],
    analysis: Optional[Dict[str, Any]],
    rca: Optional[Dict[str, Any]],
) -> bool:
    """True when telemetry/RCA indicates InvalidTemplate / contains / null."""
    parts = [
        str((error_json or {}).get("message") or ""),
        str((error_json or {}).get("code") or ""),
        str((analysis or {}).get("exact_error_message") or ""),
        str((analysis or {}).get("root_cause") or ""),
        str((rca or {}).get("exact_issue") or ""),
        str((rca or {}).get("solution") or ""),
        str((rca or {}).get("root_cause") or ""),
    ]
    blob = " ".join(parts).lower()
    rc = str((rca or {}).get("root_cause") or "").lower()
    if rc in ("payload_or_schema_error", "null_reference_error"):
        return True
    ar = str((analysis or {}).get("root_cause") or "").lower()
    if ar in ("payload_or_schema_error", "null_reference_error"):
        return True
    if "contains" in blob and ("null" in blob or "invalidtemplate" in blob):
        return True
    return False


def fix_condition_contains_null(
    node: Dict[str, Any],
    analysis: Optional[Dict[str, Any]] = None,
    *,
    error_json: Optional[Dict[str, Any]] = None,
    rca: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Fix If/Condition (and similar) actions where contains() receives null.
    Patches string expression fields anywhere under the action node.
    """
    if not isinstance(node, dict):
        return False
    if not _signals_contains_null_issue(error_json, analysis, rca):
        return False
    changed = _recursively_patch_contains(node)
    if changed:
        logger.info("[FIX] Applied contains() null-safety (coalesce) within failed action")
    return changed