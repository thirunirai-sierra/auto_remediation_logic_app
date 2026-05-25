"""
Workflow remediation utilities for Azure Logic Apps.

Provides reliable navigation, path discovery, and safe patching of workflow definitions.
All functions are deterministic and defensive.
"""

import copy
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def find_action_path(definition: Dict[str, Any], action_name: str) -> Optional[List[str]]:
    """
    Return a JSON‑pointer style path to the first occurrence of an action by name.

    Example:
        ['actions', 'Scope1', 'actions', 'Http_1']

    Args:
        definition: Workflow definition dictionary.
        action_name: Name of the action to locate.

    Returns:
        List of path segments, or None if not found.
    """
    if not action_name:
        return None

    def search(obj: Any, path: List[str]) -> Optional[List[str]]:
        if not isinstance(obj, dict):
            return None
        if "actions" in obj and isinstance(obj["actions"], dict):
            if action_name in obj["actions"]:
                return path + ["actions", action_name]
            for name, child in obj["actions"].items():
                res = search(child, path + ["actions", name])
                if res:
                    return res
        if "else" in obj and isinstance(obj["else"], dict):
            res = search(obj["else"], path + ["else"])
            if res:
                return res
        return None

    if "actions" in definition and isinstance(definition["actions"], dict):
        if action_name in definition["actions"]:
            return ["actions", action_name]
        for name, action in definition["actions"].items():
            res = search(action, ["actions", name])
            if res:
                return res
    return None


def navigate_path(root: Dict[str, Any], path: List[str]) -> Any:
    """
    Follow a path of segments through a workflow definition.

    The path is typically returned by find_action_path.

    Args:
        root: Workflow definition (or sub‑object).
        path: List of keys to traverse.

    Returns:
        The value at the target path.

    Raises:
        KeyError: If any segment does not exist.
    """
    current: Any = root
    for key in path:
        if isinstance(current, dict):
            if key == "actions" and "actions" in current:
                current = current["actions"]
            else:
                current = current[key]
        else:
            raise KeyError(f"Cannot follow path segment {key!r} at {path}")
    return current


def find_parent_conditions(definition: Dict[str, Any], action_path: List[str]) -> List[List[str]]:
    """
    Find all If/Condition ancestors that runAfter or contain the given action.

    Args:
        definition: Workflow definition.
        action_path: Path to the action (from find_action_path).

    Returns:
        List of paths to the condition ancestors.
    """
    parents: List[List[str]] = []
    action_name = action_path[-1]

    def walk(obj: Any, path: List[str]) -> None:
        if not isinstance(obj, dict):
            return
        if obj.get("type") in ("If", "Condition"):
            run_after = obj.get("runAfter", {})
            if action_name in run_after:
                parents.append(path[:])
            elif "actions" in obj and isinstance(obj["actions"], dict):
                if action_name in obj["actions"]:
                    parents.append(path[:])
        if "actions" in obj and isinstance(obj["actions"], dict):
            for name, child in obj["actions"].items():
                walk(child, path + ["actions", name])
        if "else" in obj and isinstance(obj["else"], dict):
            walk(obj["else"], path + ["else"])

    if "actions" in definition and isinstance(definition["actions"], dict):
        for name, action in definition["actions"].items():
            walk(action, ["actions", name])
    return parents


def update_condition_runafter(definition: Dict[str, Any], condition_path: List[str]) -> bool:
    """
    Broaden runAfter outcomes on an If/Condition so downstream branches still run.

    Args:
        definition: Workflow definition (modified in‑place).
        condition_path: Path to the condition node.

    Returns:
        True if updated, False on error.
    """
    try:
        current = definition
        for key in condition_path:
            if key == "actions":
                current = current["actions"]
            else:
                current = current[key]
        if not isinstance(current, dict):
            return False
        run_after = current.get("runAfter", {})
        for dep in list(run_after.keys()):
            run_after[dep] = ["Succeeded", "Failed", "Skipped", "TimedOut"]
        current["runAfter"] = run_after
        return True
    except Exception as e:
        logger.warning("update_condition_runafter failed: %s", e)
        return False


def strip_read_only_for_put(workflow_get_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove read‑only Azure properties and internal metadata (keys starting with '_').

    Args:
        workflow_get_response: Workflow resource as returned by GET.

    Returns:
        A deep copy ready for PUT deployment.
    """
    body = copy.deepcopy(workflow_get_response)
    props = body.get("properties")
    if isinstance(props, dict):
        # Azure read‑only fields
        for ro in (
            "createdTime", "changedTime", "state", "version",
            "accessEndpoint", "endpointsConfiguration",
            "integrationAccount", "integrationServiceEnvironment",
        ):
            props.pop(ro, None)

    # Remove all underscore‑prefixed keys recursively
    def clean_underscores(obj: Any) -> None:
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                if key.startswith("_"):
                    del obj[key]
                elif isinstance(obj[key], (dict, list)):
                    clean_underscores(obj[key])
        elif isinstance(obj, list):
            for item in obj:
                clean_underscores(item)

    definition = props.get("definition") if props else None
    if definition and isinstance(definition, dict):
        clean_underscores(definition)

    return body


def locate_action_node_by_path(definition: Dict[str, Any], action_path: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Locate an action using a slash‑separated path (legacy compatibility).

    Example: "Condition/True/actions/Get_file_content_using_path"

    Args:
        definition: Workflow definition.
        action_path: Slash‑separated path.

    Returns:
        (path_keys, node_dict).

    Raises:
        KeyError: If any part of the path is invalid.
    """
    if not action_path:
        raise ValueError("Empty action path")
    parts = action_path.split("/")
    current = definition.get("actions")
    if not isinstance(current, dict):
        raise ValueError("Definition missing top‑level 'actions'")

    path_keys = ["actions"]
    for i, part in enumerate(parts):
        if i == 0:
            if part not in current:
                raise KeyError(f"Top‑level action '{part}' not found")
            current = current[part]
            path_keys.append(part)
        else:
            if part == "actions":
                if not isinstance(current, dict) or "actions" not in current:
                    raise KeyError(f"No 'actions' inside {path_keys[-1]}")
                current = current["actions"]
                path_keys.append("actions")
            elif part == "else":
                if not isinstance(current, dict) or "else" not in current:
                    raise KeyError(f"No 'else' branch inside {path_keys[-1]}")
                current = current["else"]
                path_keys.append("else")
            elif part == "foreach":
                if not isinstance(current, dict) or "foreach" not in current:
                    raise KeyError(f"No 'foreach' inside {path_keys[-1]}")
                current = current["foreach"]
                path_keys.append("foreach")
            else:
                if not isinstance(current, dict) or part not in current:
                    raise KeyError(f"Action '{part}' not found inside {path_keys[-1]}")
                current = current[part]
                path_keys.append(part)
    return path_keys, current