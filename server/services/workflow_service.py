"""Business logic for Azure Logic Apps ARM interactions."""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import json
import logging
import requests
from datetime import datetime, timezone

from config import get_settings

logger = logging.getLogger(__name__)

ARM_BASE = "https://management.azure.com"


def build_headers(token: str) -> Dict[str, str]:
    """
    Build HTTP headers for Azure ARM requests.

    Args:
        token (str): Azure AD bearer token.

    Returns:
        Dict[str, str]: Headers dictionary with Authorization and Content-Type.
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
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.

    Returns:
        str: Base ARM URL for the workflow.
    """
    return (
        f"{ARM_BASE}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Logic/workflows/{workflow_name}"
    )


def _api_versions() -> Tuple[str, str, str]:
    """
    Retrieve configured API versions for runs, workflows, and triggers.

    Returns:
        Tuple[str, str, str]: (runs_api_version, workflow_api_version, trigger_api_version)
    """
    settings = get_settings()
    return (
        settings.AZURE_API_RUNS_VERSION,
        settings.AZURE_API_WORKFLOW_VERSION,
        settings.AZURE_API_TRIGGER_RUN_VERSION,
    )


def _safe_json_dumps(payload: Any, limit: int = 8000) -> str:
    """
    Safely dump a Python object to JSON, truncating if too long.

    Args:
        payload (Any): Data to serialize.
        limit (int, optional): Maximum string length. Defaults to 8000.

    Returns:
        str: JSON string (truncated if necessary).
    """
    try:
        text = json.dumps(payload, indent=2, ensure_ascii=False)
    except Exception:
        text = str(payload)

    if len(text) > limit:
        return text[:limit] + "\n... [truncated]"
    return text


def _response_payload(response: requests.Response) -> str:
    """
    Extract response body as a safe JSON string for logging.

    Args:
        response (requests.Response): HTTP response object.

    Returns:
        str: Response body (truncated JSON or plain text).
    """
    try:
        if not response.text:
            return ""
        try:
            return _safe_json_dumps(response.json(), limit=8000)
        except Exception:
            text = response.text
            return text[:8000] + ("\n... [truncated]" if len(text) > 8000 else "")
    except Exception as exc:
        return f"<failed to read response body: {exc}>"


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
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        run_id (str): Run ID of the workflow execution.

    Returns:
        Dict[str, Any]: Run resource JSON.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    runs_api, _, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}/runs/{run_id}?api-version={runs_api}"

    logger.debug("GET run URL: %s", url)
    r = requests.get(url, headers=build_headers(token), timeout=120)

    if not r.ok:
        logger.error("GET run failed: %s %s", r.status_code, r.reason)
        logger.error("GET run response body: %s", _response_payload(r))

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
    List all actions executed during a workflow run (handles pagination).

    Args:
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        run_id (str): Run ID of the workflow execution.

    Returns:
        List[Dict[str, Any]]: List of action resources.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    runs_api, _, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}/runs/{run_id}/actions?api-version={runs_api}"
    items: List[Dict[str, Any]] = []

    while url:
        logger.debug("GET run actions URL: %s", url)
        r = requests.get(url, headers=build_headers(token), timeout=120)

        if not r.ok:
            logger.error("LIST run actions failed: %s %s", r.status_code, r.reason)
            logger.error("LIST run actions response body: %s", _response_payload(r))

        r.raise_for_status()
        data = r.json()
        items.extend(data.get("value", []))
        url = data.get("nextLink") or data.get("@odata.nextLink")

    return items


def list_runs(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    top: int = 50,
) -> List[Dict[str, Any]]:
    """
    List workflow runs ordered by latest start time first.

    Args:
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        top (int, optional): Maximum number of runs to return. Defaults to 50.

    Returns:
        List[Dict[str, Any]]: List of run resources.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    runs_api, _, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/runs?api-version={runs_api}&$top={top}&$orderby=properties.startTime desc"
    )

    logger.debug("LIST runs URL: %s", url)
    resp = requests.get(url, headers=build_headers(token), timeout=120)

    if not resp.ok:
        logger.error("LIST runs failed: %s %s", resp.status_code, resp.reason)
        logger.error("LIST runs response body: %s", _response_payload(resp))

    resp.raise_for_status()
    return resp.json().get("value", [])


def get_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
) -> Dict[str, Any]:
    """
    Retrieve the full definition of a workflow.

    Args:
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.

    Returns:
        Dict[str, Any]: Workflow resource JSON.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    _, workflow_api, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}?api-version={workflow_api}"

    logger.debug("GET workflow URL: %s", url)
    r = requests.get(url, headers=build_headers(token), timeout=120)

    if not r.ok:
        logger.error("GET workflow failed: %s %s", r.status_code, r.reason)
        logger.error("GET workflow response body: %s", _response_payload(r))

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
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        workflow_body (Dict[str, Any]): Full workflow resource to PUT.
        etag (Optional[str]): If provided, adds If-Match header for optimistic concurrency.

    Returns:
        Dict[str, Any]: Updated workflow resource JSON.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    _, workflow_api, _ = _api_versions()
    url = f"{workflow_base_url(subscription_id, resource_group, workflow_name)}?api-version={workflow_api}"
    headers = build_headers(token)

    if etag:
        headers["If-Match"] = etag

    logger.info("PUT workflow URL: %s", url)
    logger.info("PUT workflow etag: %s", etag or "(none)")
    logger.debug("PUT workflow body: %s", _safe_json_dumps(workflow_body, limit=15000))

    r = requests.put(url, headers=headers, json=workflow_body, timeout=300)

    logger.info("PUT workflow status: %s %s", r.status_code, r.reason)
    logger.info("PUT workflow response body: %s", _response_payload(r))

    try:
        r.raise_for_status()
    except requests.HTTPError:
        logger.exception("PUT workflow failed for %s", workflow_name)
        raise

    return r.json() if r.text and r.text.strip() else {}


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
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        trigger_name (str): Name of the trigger to invoke.
        body (Optional[Dict[str, Any]], optional): HTTP request body for the trigger. Defaults to None.

    Returns:
        requests.Response: Raw HTTP response (status 200/202 indicates accepted).
    """
    _, _, trigger_api = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}/run?api-version={trigger_api}"
    )

    logger.info("POST trigger run URL: %s", url)
    logger.debug("POST trigger run body: %s", _safe_json_dumps(body if body is not None else {}, limit=8000))

    r = requests.post(
        url,
        headers=build_headers(token),
        json=body if body is not None else {},
        timeout=120,
    )

    logger.info("POST trigger run status: %s %s", r.status_code, r.reason)
    if not r.ok:
        logger.error("POST trigger run response body: %s", _response_payload(r))

    return r


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
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        trigger_name (str): Name of the trigger.

    Returns:
        Dict[str, Any]: Trigger resource JSON.

    Raises:
        requests.HTTPError: If the API call fails.
    """
    _, workflow_api, _ = _api_versions()
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}?api-version={workflow_api}"
    )

    logger.debug("GET trigger URL: %s", url)
    r = requests.get(url, headers=build_headers(token), timeout=120)

    if not r.ok:
        logger.error("GET trigger failed: %s %s", r.status_code, r.reason)
        logger.error("GET trigger response body: %s", _response_payload(r))

    r.raise_for_status()
    return r.json()


def find_manual_or_recurrence_trigger(definition: Dict[str, Any]) -> Optional[str]:
    """
    Find the main trigger in a workflow definition.

    Prioritizes 'request' or 'manual' triggers, then 'recurrence', then any other.

    Args:
        definition (Dict[str, Any]): Workflow definition dictionary.

    Returns:
        Optional[str]: Name of the chosen trigger, or None if no triggers exist.
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
# Helper for skip logic

def _parse_run_start_time(props: Dict[str, Any]) -> Optional[datetime]:
    """
    Extract start time from run properties.

    Args:
        props (Dict[str, Any]): Run properties dictionary.

    Returns:
        Optional[datetime]: Parsed datetime (UTC) or None if not available.
    """
    start_str = props.get("startTime") or props.get("trigger", {}).get("startTime")
    if not start_str:
        return None
    try:
        # Azure returns ISO 8601 strings
        return datetime.fromisoformat(start_str.replace('Z', '+00:00'))
    except Exception:
        return None


def list_workflow_runs(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    top: int = 50,
) -> List[Dict[str, Any]]:
    """
    Alias for list_runs, kept for compatibility.

    Args:
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        top (int, optional): Maximum number of runs to return. Defaults to 50.

    Returns:
        List[Dict[str, Any]]: List of run resources.
    """
    return list_runs(token, subscription_id, resource_group, workflow_name, top)


def should_skip_remediate_newer_succeeded(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    failed_run_id: str,
    list_top: int = 50,
) -> Tuple[bool, Optional[str]]:
    """
    Determine whether remediation for a failed run should be skipped because a newer run succeeded.

    If the workflow has a different run that started after the failed run and succeeded,
    remediation for the old failure is unnecessary.

    Args:
        token (str): Azure ARM bearer token.
        subscription_id (str): Azure subscription ID.
        resource_group (str): Azure resource group name.
        workflow_name (str): Logic App workflow name.
        failed_run_id (str): ID of the failed run.
        list_top (int, optional): Max runs to examine. Defaults to 50.

    Returns:
        Tuple[bool, Optional[str]]:
            - bool: True if remediation should be skipped.
            - Optional[str]: Reason code ("newer_run_succeeded") or None.
    """
    try:
        failed_run = get_run(
            token, subscription_id, resource_group, workflow_name, failed_run_id
        )
    except requests.HTTPError:
        return False, None
    except Exception:
        return False, None

    failed_props = failed_run.get("properties") or {}
    failed_start = _parse_run_start_time(failed_props)
    if failed_start is None:
        return False, None
    if failed_start.tzinfo is None:
        failed_start = failed_start.replace(tzinfo=timezone.utc)

    try:
        runs = list_workflow_runs(
            token, subscription_id, resource_group, workflow_name, top=list_top
        )
    except Exception:
        return False, None

    parsed: List[Tuple[datetime, str, str]] = []
    for item in runs:
        rid = item.get("name") or ""
        if not rid:
            continue
        props = item.get("properties") or {}
        st = _parse_run_start_time(props)
        if st is None:
            continue
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        status = str(props.get("status") or "")
        parsed.append((st, rid, status))

    if not parsed:
        return False, None

    parsed.sort(key=lambda x: x[0], reverse=True)
    latest_start, latest_id, latest_status = parsed[0]

    if latest_id == failed_run_id:
        return False, None
    if latest_start > failed_start and latest_status.lower() == "succeeded":
        return True, "newer_run_succeeded"
    return False, None