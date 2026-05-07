"""Business logic for Azure Logic Apps ARM interactions."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import get_settings

logger = logging.getLogger(__name__)

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


def _payload_for_validate_endpoint(workflow_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build JSON body for Workflows - Validate Workflow (ARM).
    https://learn.microsoft.com/en-us/rest/api/logic/workflows/validate-workflow
    """
    props = workflow_body.get("properties")
    if not isinstance(props, dict):
        raise ValueError("workflow_body.properties must be an object")
    definition = props.get("definition")
    if not isinstance(definition, dict):
        raise ValueError("workflow_body.properties.definition must be an object")

    vprops: Dict[str, Any] = {"definition": definition}
    for key in ("parameters", "integrationAccount", "sku", "state"):
        if key in props and props[key] is not None:
            vprops[key] = props[key]

    payload: Dict[str, Any] = {
        "location": workflow_body.get("location") or "",
        "properties": vprops,
    }
    tags = workflow_body.get("tags")
    if isinstance(tags, dict) and tags:
        payload["tags"] = tags
    return payload


def validate_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    workflow_body: Dict[str, Any],
) -> Tuple[bool, str]:
    """
    Call Azure ARM Validate Workflow before PUT. Returns (True, "") if ARM returns 200.
    """
    _, workflow_api, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/validate?api-version={workflow_api}"
    )
    try:
        payload = _payload_for_validate_endpoint(workflow_body)
    except ValueError as e:
        return False, str(e)

    if not str(payload.get("location") or "").strip():
        return (
            False,
            "Cannot validate workflow: missing resource 'location'. "
            "Fetch the workflow via GET and merge updates into that JSON before PUT.",
        )

    try:
        r = requests.post(url, headers=build_headers(token), json=payload, timeout=120)
    except requests.RequestException as e:
        return False, f"Validate request failed: {e}"

    if r.status_code in (200, 204):
        return True, ""

    text = (r.text or "").strip()
    msg = text
    try:
        data = r.json()
        err = data.get("error") or {}
        msg = str(err.get("message") or err.get("code") or text)[:2000]
    except Exception:
        msg = text[:2000] if text else f"HTTP {r.status_code}"

    logger.warning(
        "Workflow validate failed (%s): %s",
        r.status_code,
        msg[:500],
    )
    return False, f"Workflow validation failed ({r.status_code}): {msg}"


def put_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    workflow_body: Dict[str, Any],
    etag: Optional[str] = None,
    *,
    skip_validation: bool = False,
) -> Dict[str, Any]:
    _, workflow_api, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}?api-version={workflow_api}"
    headers = build_headers(token)
    if etag:
        headers["If-Match"] = etag
    if not skip_validation:
        ok, verr = validate_workflow(
            token, subscription_id, resource_group, workflow_name, workflow_body
        )
        if not ok:
            resp = requests.Response()
            resp.status_code = 400
            resp.headers["Content-Type"] = "application/json"
            resp.url = url
            body = {"error": {"code": "WorkflowValidationFailed", "message": verr}}
            resp._content = json.dumps(body).encode("utf-8")
            req_err = requests.HTTPError(verr)
            req_err.response = resp
            raise req_err
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


def get_trigger(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
) -> Dict[str, Any]:
    _, workflow_api, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}?api-version={workflow_api}"
    )
    r = requests.get(url, headers=build_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def get_latest_run_status(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
) -> str:
    """
    Get the status of the workflow's most recent run.
    Returns: 'Succeeded', 'Failed', 'Running', 'Cancelled', or 'Unknown'
    """
    runs_api, _, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}/runs"
        f"?$top=1&$orderby=startTime%20desc&api-version={runs_api}"
    )
    try:
        r = requests.get(url, headers=build_headers(token), timeout=60)
        r.raise_for_status()
        data = r.json()
        runs = data.get("value", [])
        if runs:
            return runs[0].get("properties", {}).get("status", "Unknown")
        return "Unknown"
    except Exception:
        return "Unknown"


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
