"""Business logic for Azure Logic Apps ARM interactions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from config import get_settings

ARM_BASE = "https://management.azure.com"


def build_headers(token: str) -> Dict[str, str]:
    """
    Build HTTP headers for Azure ARM requests.

    Args:
        token (str): Azure Bearer token.

    Returns:
        Dict[str, str]: Headers including authorization and content type.
    """
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def workflow_base_url(subscription_id: str, resource_group: str, workflow_name: str) -> str:
    """
    Construct the base URL for a specific workflow.

    Args:
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Logic App workflow name.

    Returns:
        str: ARM URL for the workflow.
    """
    return (
        f"{ARM_BASE}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Logic/workflows/{workflow_name}"
    )


def _api_versions() -> tuple[str, str, str]:
    """
    Retrieve configured API versions for runs, workflows, and triggers.

    Returns:
        tuple[str, str, str]: (runs_api_version, workflow_api_version, trigger_api_version)
    """
    settings = get_settings()
    return (
        settings.AZURE_API_RUNS_VERSION,        # was azure_api_runs_version
        settings.AZURE_API_WORKFLOW_VERSION,    # was azure_api_workflow_version
        settings.AZURE_API_TRIGGER_RUN_VERSION, # was azure_api_trigger_run_version
    )


def get_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Get details of a specific workflow run.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.
        run_id (str): Workflow run ID.

    Returns:
        Dict[str, Any]: JSON response from the ARM API.
    """
    runs_api, _, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}/runs/{run_id}?api-version={runs_api}"
    r = requests.get(url, headers=build_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def list_run_actions(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    List all actions executed during a workflow run.

    Handles pagination using `nextLink` or `@odata.nextLink`.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.
        run_id (str): Workflow run ID.

    Returns:
        List[Dict[str, Any]]: List of action objects.
    """
    runs_api, _, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}/runs/{run_id}/actions?api-version={runs_api}"
    items: List[Dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=build_headers(token), timeout=120)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("value", []))
        url = data.get("nextLink") or data.get("@odata.nextLink")
    return items


def get_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
) -> Dict[str, Any]:
    """
    Retrieve the definition of a workflow.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.

    Returns:
        Dict[str, Any]: Workflow JSON definition.
    """
    _, workflow_api, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}?api-version={workflow_api}"
    r = requests.get(url, headers=build_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def put_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    workflow_body: Dict[str, Any],
    etag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update or create a workflow via PUT request.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.
        workflow_body (Dict[str, Any]): Workflow JSON body.
        etag (Optional[str]): Optional ETag for concurrency control.

    Returns:
        Dict[str, Any]: ARM response (empty dict if no content).
    """
    _, workflow_api, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}?api-version={workflow_api}"
    headers = build_headers(token)
    if etag:
        headers["If-Match"] = etag
    r = requests.put(url, headers=headers, json=workflow_body, timeout=300)
    r.raise_for_status()
    return r.json() if r.text else {}


def post_trigger_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
    body: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    """
    Manually trigger a workflow run.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.
        trigger_name (str): Trigger name.
        body (Optional[Dict[str, Any]]): Optional request body.

    Returns:
        requests.Response: Raw HTTP response.
    """
    _, _, trigger_api = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}/run?api-version={trigger_api}"
    )
    return requests.post(
        url,
        headers=build_headers(token),
        json=body if body is not None else {},
        timeout=120,
    )


def get_trigger(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
) -> Dict[str, Any]:
    """
    Retrieve details of a specific workflow trigger.

    Args:
        token (str): Azure Bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Resource group name.
        workflow_name (str): Workflow name.
        trigger_name (str): Trigger name.

    Returns:
        Dict[str, Any]: Trigger JSON definition.
    """
    _, workflow_api, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}?api-version={workflow_api}"
    )
    r = requests.get(url, headers=build_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def find_manual_or_recurrence_trigger(definition: Dict[str, Any]) -> Optional[str]:
    """
    Find the main trigger in a workflow definition.

    Prioritizes manual/request triggers first, then recurrence triggers,
    then any other trigger as a fallback.

    Args:
        definition (Dict[str, Any]): Workflow JSON definition.

    Returns:
        Optional[str]: Trigger name or None if no triggers exist.
    """
    triggers = definition.get("triggers") or {}
    for name, trig in triggers.items():
        if not isinstance(trig, dict):
            continue
        ttype = (trig.get("type") or "").lower()
        if ttype in ("request", "manual"):
            return name
    for name, trig in triggers.items():
        if isinstance(trig, dict) and (trig.get("type") or "").lower() == "recurrence":
            return name
    if triggers:
        return next(iter(triggers.keys()), None)
    return None
