"""Route mapping for workflow operations."""

from __future__ import annotations

from typing import Callable, Dict

from logic_app_remediator.api.controllers import workflow_controller


ROUTES: Dict[str, Callable] = {
    "GET /workflows/{workflow}/runs/{run_id}": workflow_controller.get_run,
    "GET /workflows/{workflow}/runs/{run_id}/actions": workflow_controller.list_run_actions,
    "GET /workflows/{workflow}": workflow_controller.get_workflow,
    "PUT /workflows/{workflow}": workflow_controller.put_workflow,
    "POST /workflows/{workflow}/triggers/{trigger}/run": workflow_controller.post_trigger_run,
}
