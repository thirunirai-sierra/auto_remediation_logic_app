"""Thin controllers for workflow API operations."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from logic_app_remediator.api.services.workflow_service import (
    find_manual_or_recurrence_trigger as svc_find_manual_or_recurrence_trigger,
)
from logic_app_remediator.api.services.workflow_service import (
    get_run as svc_get_run,
    get_workflow as svc_get_workflow,
    list_run_actions as svc_list_run_actions,
    post_trigger_run as svc_post_trigger_run,
    put_workflow as svc_put_workflow,
    workflow_base_url as svc_workflow_base_url,
)


def workflow_base_url(subscription_id: str, resource_group: str, workflow_name: str) -> str:
    return svc_workflow_base_url(subscription_id, resource_group, workflow_name)


def get_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> Dict[str, Any]:
    return svc_get_run(token, subscription_id, resource_group, workflow_name, run_id)


def list_run_actions(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    return svc_list_run_actions(token, subscription_id, resource_group, workflow_name, run_id)


def get_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
) -> Dict[str, Any]:
    return svc_get_workflow(token, subscription_id, resource_group, workflow_name)


def put_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    workflow_body: Dict[str, Any],
    etag: Optional[str] = None,
) -> Dict[str, Any]:
    if not isinstance(workflow_body, dict):
        raise ValueError("workflow_body must be a JSON object")
    return svc_put_workflow(token, subscription_id, resource_group, workflow_name, workflow_body, etag=etag)


def post_trigger_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
    body: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    if not trigger_name:
        raise ValueError("trigger_name is required")
    return svc_post_trigger_run(
        token,
        subscription_id,
        resource_group,
        workflow_name,
        trigger_name,
        body=body,
    )


def find_manual_or_recurrence_trigger(definition: Dict[str, Any]) -> Optional[str]:
    return svc_find_manual_or_recurrence_trigger(definition)
