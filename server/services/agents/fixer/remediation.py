# server/services/agents/fixer/remediation.py
"""
Apply minimal patches to a Logic App workflow definition for a single failed action.
"""
import copy
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from config import Settings
import logging

logger = logging.getLogger(__name__)

def _fix_compose_add_string_expression(node: Dict[str, Any], analysis: Optional[Dict[str, Any]]) -> bool:
    """
    Patch a Compose node's 'add' expression if it fails due to a string type issue.

    Args:
        node (Dict[str, Any]): The Compose action node from the workflow definition.
        analysis (Optional[Dict[str, Any]]): Error analysis dictionary containing `exact_error_message`.

    Returns:
        bool: True if the node was modified, False otherwise.
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
    pat = r"add\((.*?),\s*'([^']+)'\s*\)"
    new_inp, count = re.subn(pat, r"add(\1, int('\2'))", inp)
    if count > 0:
        node["inputs"] = new_inp
        return True
    return False

def locate_action_node(definition: Dict[str, Any], action_name: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Locate an action node in a Logic App workflow definition.

    Args:
        definition (Dict[str, Any]): The workflow definition dictionary.
        action_name (str): Name of the action to locate.

    Returns:
        Tuple[List[str], Dict[str, Any]]: 
            - Path to the action as a list of keys.
            - The action node dictionary.

    Raises:
        ValueError: If `actions` key is missing in the definition.
        KeyError: If the action name is not found in the workflow.
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
    Apply minimal remediation patches to a failed action in a Logic App workflow.

    Args:
        workflow_resource (Dict[str, Any]): The Logic App workflow resource.
        action_name (str): Name of the failed action to patch.
        error_type (str): Error category (e.g., '404', '401', 'timeout', 'bad_request').
        settings (Settings): Global system/configuration settings.
        analysis (Optional[Dict[str, Any]]): Optional error analysis dictionary.

    Returns:
        Dict[str, Any]: Patched workflow resource dictionary.
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
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                inputs["uri"] = settings.FALLBACK_HTTP_URL   # uppercase
    elif error_type == "401":
        # The auth header settings may not exist; skip if not present.
        pass
    elif error_type == "timeout":
        policy = {"type": "fixed", "count": 3, "interval": "PT30S"}
        node["retryPolicy"] = policy
        if node_type in ("http", "httpwebhook"):
            inputs = node.setdefault("inputs", {})
            if isinstance(inputs, dict):
                inputs["retryPolicy"] = policy
                to = getattr(settings, "HTTP_TIMEOUT_ISO", "PT2M")   # uppercase
                inputs.setdefault("runtimeConfiguration", {})
                rc = inputs["runtimeConfiguration"]
                if isinstance(rc, dict):
                    rc.setdefault("contentTransfer", {})
                    ct = rc["contentTransfer"]
                    if isinstance(ct, dict):
                        ct["transferMode"] = ct.get("transferMode") or "Chunked"
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
                body = inputs.get("body")
                if isinstance(body, str):
                    try:
                        inputs["body"] = json.loads(body)
                    except Exception:
                        inputs["body"] = {}
    return patched

def strip_read_only_for_put(workflow_get_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove read-only properties from a workflow response before a PUT operation.

    Args:
        workflow_get_response (Dict[str, Any]): The workflow GET response dictionary.

    Returns:
        Dict[str, Any]: Copy of the workflow dictionary with read-only fields removed.
    """
    body = copy.deepcopy(workflow_get_response)
    props = body.get("properties")
    if isinstance(props, dict):
        for ro in (
            "createdTime", "changedTime", "state", "version",
            "accessEndpoint", "endpointsConfiguration",
            "integrationAccount", "integrationServiceEnvironment",
        ):
            props.pop(ro, None)
    return body

def _contains_needs_null_guard(expr: str) -> bool:
    """
    Check if a contains() expression in a condition needs null guarding.

    Args:
        expr (str): Expression string to check.

    Returns:
        bool: True if null-guard is needed, False otherwise.
    """
    x = (expr or "").lower()
    return "contains(" in x and "coalesce(" not in x

def _add_null_guard_to_contains(expr: str) -> Tuple[str, bool]:
    """
    Add a null-safety guard (coalesce) to a contains() expression.

    Args:
        expr (str): Expression string.

    Returns:
        Tuple[str, bool]: Patched expression and a boolean indicating if a change was made.
    """
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
    """
    Recursively patch all contains() expressions within a node to add null safety.

    Args:
        node (Any): Node (dict or list) to patch.

    Returns:
        bool: True if any changes were made, False otherwise.
    """
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
    """
    Check if the error signals indicate a null-reference or payload/schema issue related to contains().

    Args:
        error_json (Optional[Dict[str, Any]]): Raw error object.
        analysis (Optional[Dict[str, Any]]): Error analysis dictionary.
        rca (Optional[Dict[str, Any]]): Root cause analysis dictionary.

    Returns:
        bool: True if a null-related issue is detected, False otherwise.
    """
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
    Apply null-safety patches to conditions with contains() in a failed action node.

    Args:
        node (Dict[str, Any]): Action node dictionary to patch.
        analysis (Optional[Dict[str, Any]]): Error analysis dictionary.
        error_json (Optional[Dict[str, Any]]): Raw error object.
        rca (Optional[Dict[str, Any]]): Root cause analysis dictionary.

    Returns:
        bool: True if changes were applied, False otherwise.
    """
    if not isinstance(node, dict):
        return False
    if not _signals_contains_null_issue(error_json, analysis, rca):
        return False
    changed = _recursively_patch_contains(node)
    if changed:
        logger.info("[FIX] Applied contains() null-safety (coalesce) within failed action")
    return changed