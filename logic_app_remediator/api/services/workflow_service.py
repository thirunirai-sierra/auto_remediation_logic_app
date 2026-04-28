"""Business logic for Azure Logic Apps ARM interactions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from logic_app_remediator.config import get_settings

ARM_BASE = "https://management.azure.com"


def build_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def workflow_base_url(subscription_id: str, resource_group: str, workflow_name: str) -> str:
    return (
        f"{ARM_BASE}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Logic/workflows/{workflow_name}"
    )


def _api_versions() -> tuple[str, str, str]:
    settings = get_settings()
    return (
        settings.azure_api_runs_version,
        settings.azure_api_workflow_version,
        settings.azure_api_trigger_run_version,
    )


def get_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> Dict[str, Any]:
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


def find_manual_or_recurrence_trigger(definition: Dict[str, Any]) -> Optional[str]:
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
