# server/services/agents/observer.py
"""
Observer agent: fetches run details, identifies the primary failed action,
and builds error context.
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
    """

    def __init__(self, settings: Settings):
        """
        Initialize the Observer.

        Args:
            settings: Application settings instance.
        """
        self.settings = settings

    def _get_arm_token(self) -> str:
        """Obtain ARM token using service principal."""
        return get_arm_token(
            self.settings.AZURE_TENANT_ID,
            self.settings.AZURE_CLIENT_ID,
            self.settings.AZURE_CLIENT_SECRET,
        )

    def _extract_error_blob(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract error message, code, and status code from an action resource.

        Args:
            action: Raw action dictionary from Azure API.

        Returns:
            Dict with keys: error_message, error_code, status_code.
        """
        props = action.get("properties", {})
        error = props.get("error") or {}
        outputs = props.get("outputs") or {}
        body = outputs.get("body") or {}
        return {
            "error_message": error.get("message") or body.get("message") or "",
            "error_code": error.get("code") or body.get("code") or "",
            "status_code": body.get("statusCode") or outputs.get("statusCode") or 0,
        }

    def _pick_failed_action(
        self, actions: List[Dict[str, Any]]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Select the most meaningful failed action from the list.

        Args:
            actions: List of action resources.

        Returns:
            Tuple (failed_action_dict, action_name) or (None, None) if none.
        """
        failed = [a for a in actions if a.get("properties", {}).get("status") == "Failed"]
        if not failed:
            return None, None

        def score(a: Dict[str, Any]) -> int:
            err = self._extract_error_blob(a)
            return (1 if err["status_code"] else 0) + (1 if err["error_code"] else 0)

        best = max(failed, key=score)
        name = best.get("name", "").split("/")[-1]
        return best, name

    def analyze_failed_run(
        self,
        subscription_id: str,
        resource_group: str,
        workflow_name: str,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Analyze a specific run and return structured error context.

        Args:
            subscription_id: Azure subscription ID.
            resource_group: Resource group name.
            workflow_name: Logic App workflow name.
            run_id: Run ID to analyze.

        Returns:
            Dict with keys:
                - status: "failed_action_found", "run_not_failed", "no_failed_action"
                - run_status: current run status
                - failed_action: the failed action dict (if found)
                - failed_action_name: name of failed action
                - error_context: structured error details
        """
        token = self._get_arm_token()
        try:
            run = get_run(token, subscription_id, resource_group, workflow_name, run_id)
            run_status = run.get("properties", {}).get("status", "")
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

        failed_action, action_name = self._pick_failed_action(actions)
        if not failed_action:
            return {"status": "no_failed_action", "run_status": run_status}

        error_blob = self._extract_error_blob(failed_action)
        context = {
            "workflow_name": workflow_name,
            "run_id": run_id,
            "failed_action_name": action_name,
            "action_type": failed_action.get("type", ""),
            "error_message": error_blob["error_message"],
            "error_code": error_blob["error_code"],
            "status_code": error_blob["status_code"],
            "action_inputs": failed_action.get("properties", {}).get("inputs"),
            "action_outputs": failed_action.get("properties", {}).get("outputs"),
        }

        return {
            "status": "failed_action_found",
            "run_status": run_status,
            "failed_action": failed_action,
            "failed_action_name": action_name,
            "error_context": context,
        }