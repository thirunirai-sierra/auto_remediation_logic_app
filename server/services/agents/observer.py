"""
Observer agent: fetches run details, identifies the primary failed action,
and builds error context. Collects errors from ALL actions in the run
(neighbouring components, parents, error handlers) to extract the best
available error message and code.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from services.workflow_service import get_run, list_run_actions
from services.auth import get_arm_token
from config import Settings

logger = logging.getLogger(__name__)


class Observer:
    """
    Observer agent responsible for fetching and analyzing a failed Logic App run.
    Finds the deepest failed action and collects error details from all actions.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the Observer with application settings.

        Args:
            settings: Application configuration (used for Azure AD credentials).
        """
        self.settings = settings

    def _get_arm_token(self) -> str:
        """Obtain an Azure Resource Manager bearer token using configured credentials."""
        return get_arm_token(
            self.settings.AZURE_TENANT_ID,
            self.settings.AZURE_CLIENT_ID,
            self.settings.AZURE_CLIENT_SECRET,
        )

    def _extract_error_blob(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract error message, code, and status code from an action resource.

        Args:
            action: Azure Logic App action dictionary (as returned by list_run_actions).

        Returns:
            Dictionary with keys: error_message, error_code, status_code.
        """
        props = action.get("properties", {})
        error = props.get("error") if isinstance(props.get("error"), dict) else action.get("error")
        outputs = props.get("outputs") if isinstance(props.get("outputs"), dict) else {}
        body = outputs.get("body") if isinstance(outputs.get("body"), dict) else {}

        error_message = ""
        error_code = ""
        status_code = outputs.get("statusCode") or props.get("statusCode") or 0

        if isinstance(error, dict):
            error_message = error.get("message") or ""
            error_code = error.get("code") or ""
        if not error_message and isinstance(outputs.get("error"), dict):
            error_message = outputs["error"].get("message") or ""
            error_code = outputs["error"].get("code") or ""
        if not error_message and isinstance(body, dict):
            error_message = body.get("message") or body.get("error") or ""
            error_code = body.get("code") or ""
        if not error_message and isinstance(body.get("error"), dict):
            error_message = body["error"].get("message") or ""
            error_code = body["error"].get("code") or ""
        if not error_message:
            error_message = props.get("message") or props.get("errorMessage") or ""
        if not error_message and status_code:
            error_message = f"HTTP {status_code} error"

        return {
            "error_message": error_message,
            "error_code": error_code,
            "status_code": status_code,
        }

    def _collect_all_errors(
        self,
        actions: List[Dict[str, Any]],
        run_error: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Collect error information from all actions and optionally the run itself.

        Args:
            actions: List of action resources.
            run_error: Optional run‑level error dictionary (from run properties).

        Returns:
            List of error dictionaries, each containing:
                - action_name (str)
                - action_type (str)
                - error_message (str)
                - error_code (str)
                - status_code (int or None)
        """
        all_errors = []
        for action in actions:
            err_blob = self._extract_error_blob(action)
            if err_blob["error_message"] or err_blob["error_code"]:
                all_errors.append({
                    "action_name": action.get("name", "unknown"),
                    "action_type": action.get("type", "unknown"),
                    **err_blob
                })
        if run_error and run_error.get("message"):
            all_errors.append({
                "action_name": "_workflow_run",
                "action_type": "run",
                "error_message": run_error.get("message", ""),
                "error_code": run_error.get("code", ""),
                "status_code": None,
            })
        return all_errors

    def _merge_error_context(
        self,
        actions: List[Dict[str, Any]],
        deepest_failed_action: Dict[str, Any],
        run_error: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Choose the best error message and code from all available sources.

        Priority:
            1. The deepest failed action's own error.
            2. Any action's error message.
            3. The workflow‑run level error.

        Args:
            actions: List of all actions.
            deepest_failed_action: The action identified as the deepest failure.
            run_error: Optional run‑level error.

        Returns:
            Dictionary with keys: error_message, error_code, status_code.
        """
        all_errors = self._collect_all_errors(actions, run_error)
        if not all_errors:
            return {"error_message": "", "error_code": "", "status_code": 0}

        deepest_name = deepest_failed_action.get("name", "")
        for err in all_errors:
            if err["action_name"] == deepest_name and err["error_message"]:
                return {
                    "error_message": err["error_message"],
                    "error_code": err["error_code"],
                    "status_code": err["status_code"],
                }
        for err in all_errors:
            if err["error_message"]:
                return {
                    "error_message": err["error_message"],
                    "error_code": err["error_code"],
                    "status_code": err["status_code"],
                }
        for err in all_errors:
            if err["action_name"] == "_workflow_run":
                return {
                    "error_message": err["error_message"],
                    "error_code": err["error_code"],
                    "status_code": err["status_code"],
                }
        return {"error_message": "", "error_code": "", "status_code": 0}

    def _is_wrapper_action(self, action_type: str, action_name: str) -> bool:
        """
        Determine if an action is a structural wrapper (If, Condition, Scope, etc.).

        Args:
            action_type: Azure action type (e.g., "If", "Condition").
            action_name: Name of the action.

        Returns:
            True if the action is a container/wrapper, False otherwise.
        """
        wrapper_types = ["If", "Condition", "Scope", "Until", "Foreach", "For_each"]
        if action_type in wrapper_types:
            return True
        if action_name.lower() in ["condition", "scope", "until", "foreach"]:
            return True
        return False

    def _get_all_failed_actions(
        self,
        actions: List[Dict[str, Any]],
        parent_segments: Optional[List[str]] = None,
    ) -> List[Tuple[Dict[str, Any], List[str], str]]:
        """
        Recursively find all failed actions and their path segments.

        Args:
            actions: List of action resources (may contain nested actions).
            parent_segments: Path segments from the parent (used in recursion).

        Returns:
            List of tuples, each containing:
                - action_node (dict)
                - path_segments (list of strings)
                - formatted_path_string (str)
        """
        if parent_segments is None:
            parent_segments = []
        failed_actions = []
        for action in actions:
            base_name = action.get("name", "").split("/")[-1]
            props = action.get("properties", {})
            status = props.get("status", "")
            current_segments = parent_segments + [base_name]
            formatted_path = "/".join(current_segments)

            if status == "Failed":
                failed_actions.append((action, current_segments, formatted_path))

            # Nested actions (inside a wrapper)
            nested_actions = props.get("actions", {})
            if nested_actions and isinstance(nested_actions, dict):
                nested_list = [
                    {"name": f"{formatted_path}/actions/{k}", "properties": v, "type": v.get("type")}
                    for k, v in nested_actions.items()
                ]
                deeper_failed = self._get_all_failed_actions(nested_list, current_segments + ["actions"])
                failed_actions.extend(deeper_failed)

            # Else branch (for If/Condition)
            else_branch = props.get("else", {})
            if else_branch and isinstance(else_branch, dict):
                else_actions = else_branch.get("actions", {})
                if else_actions and isinstance(else_actions, dict):
                    nested_list = [
                        {"name": f"{formatted_path}/else/{k}", "properties": v, "type": v.get("type")}
                        for k, v in else_actions.items()
                    ]
                    deeper_failed = self._get_all_failed_actions(nested_list, current_segments + ["else"])
                    failed_actions.extend(deeper_failed)
        return failed_actions

    def _pick_deepest_failed_action(
        self,
        actions: List[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Select the deepest non‑wrapper failed action from the hierarchy.

        Args:
            actions: List of action resources.

        Returns:
            Tuple (action_node, full_path_string) or (None, None) if no failed action.
        """
        all_failed = self._get_all_failed_actions(actions)
        if not all_failed:
            return None, None

        logger.info(f"   Found {len(all_failed)} failed action(s) in hierarchy:")
        for action, segments, path in all_failed:
            action_type = action.get("type", "unknown")
            depth = len(segments)
            logger.info(f"      - {path} (type={action_type}, depth={depth})")

        # Sort by depth descending
        all_failed.sort(key=lambda x: len(x[1]), reverse=True)
        best_action, best_segments, best_path = all_failed[0]
        best_depth = len(best_segments)

        # If multiple at same depth, prefer non‑wrapper
        same_depth = [(a, s, p) for a, s, p in all_failed if len(s) == best_depth]
        if len(same_depth) > 1:
            for action, segments, path in same_depth:
                action_type = action.get("type", "unknown")
                if not self._is_wrapper_action(action_type, segments[-1] if segments else ""):
                    best_action, best_segments, best_path = action, segments, path
                    logger.info(f"   Preferring non-wrapper action: {best_path}")
                    break

        logger.info(f"   Selected deepest failed action: {best_path}")
        logger.info(f"      Depth: {len(best_segments)}")
        return best_action, best_path

    def analyze_failed_run(
        self,
        subscription_id: str,
        resource_group: str,
        workflow_name: str,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Analyze a failed workflow run and return structured error context.

        Args:
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group name.
            workflow_name: Name of the Logic App workflow.
            run_id: ID of the run to analyze.

        Returns:
            Dictionary with status and error context. On success:
                - status: "failed_action_found"
                - run_status: original run status
                - failed_action: action node
                - failed_action_name: name of failed action
                - failed_action_path: full hierarchical path
                - error_context: dict with workflow_name, run_id, failed_action_name,
                  failed_action_path, action_type, error_message, error_code,
                  status_code, action_inputs, action_outputs
        """
        token = self._get_arm_token()
        try:
            run = get_run(token, subscription_id, resource_group, workflow_name, run_id)
            run_status = run.get("properties", {}).get("status", "")
            run_error = run.get("properties", {}).get("error")
        except Exception as e:
            logger.error("Failed to fetch run %s: %s", run_id, e)
            return {"status": "error", "error": str(e)}

        if run_status.lower() != "failed":
            return {"status": "run_not_failed", "run_status": run_status}

        try:
            actions = list_run_actions(token, subscription_id, resource_group, workflow_name, run_id)
        except Exception as e:
            logger.error("Failed to list actions for run %s: %s", run_id, e)
            return {"status": "error", "error": str(e)}

        deepest_action, full_path = self._pick_deepest_failed_action(actions)
        if not deepest_action:
            return {"status": "no_failed_action", "run_status": run_status}

        action_name = full_path.split("/")[-1] if full_path else deepest_action.get("name", "")
        # Patch the action node name so _merge_error_context can match it
        deepest_action_normalized = dict(deepest_action)
        deepest_action_normalized["name"] = action_name
        merged_error = self._merge_error_context(actions, deepest_action_normalized, run_error)

        context = {
            "workflow_name": workflow_name,
            "run_id": run_id,
            "failed_action_name": action_name,
            "failed_action_path": full_path,
            "action_type": deepest_action.get("type", ""),
            "error_message": merged_error["error_message"],
            "error_code": merged_error["error_code"],
            "status_code": merged_error["status_code"],
            "action_inputs": deepest_action.get("properties", {}).get("inputs"),
            "action_outputs": deepest_action.get("properties", {}).get("outputs"),
        }

        logger.info("")
        logger.info("OBSERVER RESULT:")
        logger.info(f"   Failed Action: {action_name}")
        logger.info(f"   Full Path: {full_path}")
        logger.info(f"   Error Message: {merged_error['error_message'][:200] if merged_error['error_message'] else '(empty)'}")
        logger.info(f"   Error Code: {merged_error['error_code']}")
        logger.info("")

        return {
            "status": "failed_action_found",
            "run_status": run_status,
            "failed_action": deepest_action,
            "failed_action_name": action_name,
            "failed_action_path": full_path,
            "error_context": context,
        }