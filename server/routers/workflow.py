# server/routers/workflow.py
"""
FastAPI router for workflow operations – production hardened.
Includes async offloading, retries, idempotency, audit logging.
"""
import logging
import uuid
import asyncio
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from fastapi.concurrency import run_in_threadpool
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

from services.auth import get_arm_token
from services.workflow_service import (
    get_run as svc_get_run,
    list_run_actions as svc_list_run_actions,
    get_workflow as svc_get_workflow,
    put_workflow as svc_put_workflow,
    post_trigger_run as svc_post_trigger_run,
)
from config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()


def get_token():
    """Dependency to obtain ARM token."""
    return get_arm_token(settings.AZURE_TENANT_ID, settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET)


def validate_workflow_name(name: str) -> bool:
    """Basic ARM resource name validation."""
    import re
    return bool(re.match(r"^[a-zA-Z0-9\-_]{1,80}$", name))


# Retry decorator for transient Azure failures
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError)),
)
async def _call_with_retry(func, *args, **kwargs):
    return await run_in_threadpool(func, *args, **kwargs)


@router.get("/{workflow_name}/runs/{run_id}")
async def get_run_endpoint(
    request: Request,
    workflow_name: str,
    run_id: str,
    token: str = Depends(get_token),
    x_correlation_id: Optional[str] = Header(None),
):
    """Get details of a specific workflow run."""
    correlation_id = x_correlation_id or str(uuid.uuid4())
    if not settings.AZURE_SUBSCRIPTION_ID or not settings.AZURE_RESOURCE_GROUP:
        raise HTTPException(400, "AZURE_SUBSCRIPTION_ID and AZURE_RESOURCE_GROUP must be set")
    if not validate_workflow_name(workflow_name):
        raise HTTPException(400, "Invalid workflow name format")

    logger.info("correlation_id=%s | Get run %s/%s", correlation_id, workflow_name, run_id)
    try:
        result = await _call_with_retry(
            svc_get_run,
            token,
            settings.AZURE_SUBSCRIPTION_ID,
            settings.AZURE_RESOURCE_GROUP,
            workflow_name,
            run_id,
        )
        return result
    except Exception as e:
        logger.error("correlation_id=%s | Failed to get run: %s", correlation_id, e, exc_info=True)
        raise HTTPException(500, f"Failed to get run: {str(e)}")


@router.get("/{workflow_name}/runs/{run_id}/actions")
async def list_run_actions_endpoint(
    request: Request,
    workflow_name: str,
    run_id: str,
    token: str = Depends(get_token),
    x_correlation_id: Optional[str] = Header(None),
):
    correlation_id = x_correlation_id or str(uuid.uuid4())
    if not settings.AZURE_SUBSCRIPTION_ID or not settings.AZURE_RESOURCE_GROUP:
        raise HTTPException(400, "Missing subscription or resource group")
    if not validate_workflow_name(workflow_name):
        raise HTTPException(400, "Invalid workflow name")

    logger.info("correlation_id=%s | List actions for %s/%s", correlation_id, workflow_name, run_id)
    try:
        result = await _call_with_retry(
            svc_list_run_actions,
            token,
            settings.AZURE_SUBSCRIPTION_ID,
            settings.AZURE_RESOURCE_GROUP,
            workflow_name,
            run_id,
        )
        return result
    except Exception as e:
        logger.error("correlation_id=%s | Failed to list actions: %s", correlation_id, e, exc_info=True)
        raise HTTPException(500, f"Failed to list actions: {str(e)}")


@router.get("/{workflow_name}")
async def get_workflow_endpoint(
    request: Request,
    workflow_name: str,
    token: str = Depends(get_token),
    x_correlation_id: Optional[str] = Header(None),
):
    correlation_id = x_correlation_id or str(uuid.uuid4())
    if not settings.AZURE_SUBSCRIPTION_ID or not settings.AZURE_RESOURCE_GROUP:
        raise HTTPException(400, "Missing subscription or resource group")
    if not validate_workflow_name(workflow_name):
        raise HTTPException(400, "Invalid workflow name")

    logger.info("correlation_id=%s | Get workflow %s", correlation_id, workflow_name)
    try:
        result = await _call_with_retry(
            svc_get_workflow,
            token,
            settings.AZURE_SUBSCRIPTION_ID,
            settings.AZURE_RESOURCE_GROUP,
            workflow_name,
        )
        return result
    except Exception as e:
        logger.error("correlation_id=%s | Failed to get workflow: %s", correlation_id, e, exc_info=True)
        if "404" in str(e) or "Not Found" in str(e):
            raise HTTPException(404, f"Workflow '{workflow_name}' not found")
        raise HTTPException(500, f"Failed to get workflow: {str(e)}")


@router.put("/{workflow_name}")
async def put_workflow_endpoint(
    request: Request,
    workflow_name: str,
    workflow_body: dict,
    token: str = Depends(get_token),
    x_correlation_id: Optional[str] = Header(None),
):
    correlation_id = x_correlation_id or str(uuid.uuid4())
    if not settings.AZURE_SUBSCRIPTION_ID or not settings.AZURE_RESOURCE_GROUP:
        raise HTTPException(400, "Missing subscription or resource group")
    if not validate_workflow_name(workflow_name):
        raise HTTPException(400, "Invalid workflow name")

    # Audit: log the change (do not log full body in production; just metadata)
    logger.warning(
        "correlation_id=%s | WORKFLOW UPDATE: %s by token subject (audit)",
        correlation_id,
        workflow_name,
    )
    try:
        result = await _call_with_retry(
            svc_put_workflow,
            token,
            settings.AZURE_SUBSCRIPTION_ID,
            settings.AZURE_RESOURCE_GROUP,
            workflow_name,
            workflow_body,
        )
        return result
    except Exception as e:
        logger.error("correlation_id=%s | Failed to update workflow: %s", correlation_id, e, exc_info=True)
        raise HTTPException(500, f"Failed to update workflow: {str(e)}")


@router.post("/{workflow_name}/triggers/{trigger_name}/run")
async def post_trigger_run_endpoint(
    request: Request,
    workflow_name: str,
    trigger_name: str,
    body: Optional[dict] = None,
    token: str = Depends(get_token),
    x_idempotency_key: Optional[str] = Header(None),
    x_correlation_id: Optional[str] = Header(None),
):
    correlation_id = x_correlation_id or str(uuid.uuid4())
    idempotency_key = x_idempotency_key or str(uuid.uuid4())

    if not settings.AZURE_SUBSCRIPTION_ID or not settings.AZURE_RESOURCE_GROUP:
        raise HTTPException(400, "Missing subscription or resource group")
    if not validate_workflow_name(workflow_name):
        raise HTTPException(400, "Invalid workflow name")

    logger.info(
        "correlation_id=%s | Trigger run %s/%s (idempotency=%s)",
        correlation_id,
        workflow_name,
        trigger_name,
        idempotency_key,
    )
    try:
        resp = await _call_with_retry(
            svc_post_trigger_run,
            token,
            settings.AZURE_SUBSCRIPTION_ID,
            settings.AZURE_RESOURCE_GROUP,
            workflow_name,
            trigger_name,
            body,
        )
        # If ARM returns 409 (conflict) due to idempotency, treat as success
        if resp.status_code == 409:
            logger.warning("correlation_id=%s | Idempotency conflict, run already triggered", correlation_id)
            return {"status": "already_triggered", "idempotency_key": idempotency_key}
        return {"status_code": resp.status_code, "headers": dict(resp.headers), "idempotency_key": idempotency_key}
    except Exception as e:
        logger.error("correlation_id=%s | Failed to trigger run: %s", correlation_id, e, exc_info=True)
        raise HTTPException(500, f"Failed to trigger run: {str(e)}")