"""Utility helpers for fixer agent (kept separate from remediation to avoid cycles)."""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, Optional, Tuple

JSONDict = Dict[str, Any]


def extract_failed_action_name(error_message: str) -> Optional[str]:
    """Extract failed action name from common Logic Apps error strings."""
    patterns = [
        r"for action '([^']+)' is not satisfied",
        r"action '([^']+)' failed",
        r"Action '([^']+)' failed",
    ]
    for pattern in patterns:
        match = re.search(pattern, error_message or "")
        if match:
            return match.group(1)
    return None


def clone_workflow_definition(workflow_definition: JSONDict) -> JSONDict:
    """Deep copy workflow definition for safe in-place edits."""
    return copy.deepcopy(workflow_definition or {})


def locate_action(actions: JSONDict, action_name: str) -> Optional[JSONDict]:
    """Recursively locate an action node by name in nested actions."""
    if not isinstance(actions, dict):
        return None
    for name, cfg in actions.items():
        if name == action_name and isinstance(cfg, dict):
            return cfg
        if isinstance(cfg, dict):
            nested = locate_action(cfg.get("actions", {}), action_name)
            if nested is not None:
                return nested
            else_actions = (cfg.get("else") or {}).get("actions", {})
            nested = locate_action(else_actions, action_name)
            if nested is not None:
                return nested
    return None


def contains_needs_null_guard(expr: str) -> bool:
    """Return True if expression includes contains() without coalesce()."""
    x = (expr or "").lower()
    return "contains(" in x and "coalesce(" not in x


def add_null_guard_to_contains(expr: str) -> Tuple[str, bool]:
    """Add null guard to first arg of contains(arg, value)."""
    if not contains_needs_null_guard(expr):
        return expr, False
    pattern = re.compile(r"contains\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", re.IGNORECASE)

    def repl(match: re.Match) -> str:
        first = match.group(1).strip()
        second = match.group(2).strip()
        guarded = f"coalesce({first}, '')"
        return f"contains({guarded}, {second})"

    updated, count = pattern.subn(repl, expr)
    return updated, count > 0


def recursively_patch_contains(node: Any) -> bool:
    """Patch contains() expressions in string fields recursively."""
    changed = False
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if isinstance(value, str):
                patched, did = add_null_guard_to_contains(value)
                if did:
                    node[key] = patched
                    changed = True
            elif isinstance(value, (dict, list)):
                if recursively_patch_contains(value):
                    changed = True
    elif isinstance(node, list):
        for i, value in enumerate(node):
            if isinstance(value, str):
                patched, did = add_null_guard_to_contains(value)
                if did:
                    node[i] = patched
                    changed = True
            elif isinstance(value, (dict, list)):
                if recursively_patch_contains(value):
                    changed = True
    return changed
