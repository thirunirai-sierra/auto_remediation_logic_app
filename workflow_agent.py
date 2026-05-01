"""
Orchestrates fetch → analyze → remediate → re-run → structured output.
Now with LLM-powered fix generation!
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from agent.classifier.analyzer import analyze_error
from api import (
    find_manual_or_recurrence_trigger,
    get_run,
    get_trigger,
    get_workflow,
    list_run_actions,
    post_trigger_run,
    put_workflow,
)
from auth import get_arm_token
from config import Settings, get_settings
from agent.rca_agent import generate_rca, generate_rca_from_error
from remediation import (
    apply_remediation_patch,
    locate_action_node,
    strip_read_only_for_put,
    fix_condition_contains_null,
)

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_CONTAINER_TYPES = frozenset(
    {
        "scope",
        "foreach",
        "until",
        "if",
        "switch",
        "parallel",
        "parallelbranch",
    }
)

_NON_RETRYABLE_ARM_CODES = frozenset(
    {
        "ReadOnlyDisabledSubscription",
        "SubscriptionNotRegistered",
        "AuthorizationFailed",
    }
)


def _end_time(action: Dict[str, Any]) -> str:
    return (action.get("properties") or {}).get("endTime") or ""


def _deep_find_status_code(obj: Any, depth: int = 0) -> Optional[int]:
    if depth > 14:
        return None
    if isinstance(obj, dict):
        sc = obj.get("statusCode")
        if isinstance(sc, int) and 100 <= sc <= 599:
            return sc
        if isinstance(sc, str) and sc.isdigit():
            v = int(sc)
            if 100 <= v <= 599:
                return v
        for v in obj.values():
            hit = _deep_find_status_code(v, depth + 1)
            if hit is not None:
                return hit
    elif isinstance(obj, list):
        for it in obj:
            hit = _deep_find_status_code(it, depth + 1)
            if hit is not None:
                return hit
    return None


def _get_action_uri(workflow: Dict[str, Any]) -> str:
    """Extract action URI for debugging."""
    try:
        actions = workflow.get("properties", {}).get("definition", {}).get("actions", {})
        for action in actions.values():
            if action.get("type", "").lower() in ("http", "httpwebhook"):
                inputs = action.get("inputs", {})
                if isinstance(inputs, dict):
                    uri = inputs.get("uri", "")
                    if uri:
                        return uri[:100]
        return "unknown"
    except Exception:
        return "unknown"


def _get_llm_fix_for_workflow(
    workflow_definition: Dict[str, Any],
    failed_action_name: str,
    error_message: str,
    error_type: str,
    settings: Settings,
) -> Optional[Dict[str, Any]]:
    """
    Call LLM to generate the fixed workflow definition.
    This is the key function that enables LLM to actually fix the workflow!
    """
    if not settings.azure_openai_endpoint or not settings.azure_openai_api_key:
        logger.warning("[LLM] Azure OpenAI not configured, cannot generate LLM fix")
        return None
    
    try:
        url = (
            f"{settings.azure_openai_endpoint.rstrip('/')}/openai/deployments/"
            f"{settings.azure_openai_deployment}/chat/completions"
            f"?api-version={settings.azure_openai_api_version}"
        )
        
        system_prompt = """You are an Azure Logic Apps expert. Your task is to FIX the workflow definition.
Given the current workflow definition and an error, return the COMPLETE FIXED workflow definition.
Focus on fixing the specific action that failed.

CRITICAL RULES:
1. Return ONLY valid JSON - the complete workflow definition
2. For contains() errors with null values, wrap the first argument with coalesce():
   - String: contains(coalesce(expr, ''), 'value')
   - Array: contains(coalesce(expr, createArray()), 'value')
   - Object: contains(keys(coalesce(expr, {})), 'key')
3. Keep all other parts of the workflow identical
4. Do not add any explanation or markdown - ONLY the JSON workflow definition
5. Preserve all existing actions, triggers, and parameters

EXAMPLES:
Wrong: @contains(triggerBody()?['field'], 'error')
Fixed: @contains(coalesce(triggerBody()?['field'], ''), 'error')

Wrong: @contains(body('GetItems')?['value'], 'item')
Fixed: @contains(coalesce(body('GetItems')?['value'], createArray()), 'item')

Return the complete fixed workflow definition as JSON only."""
        
        user_prompt = f"""Error Type: {error_type}
Error Message: {error_message}
Failed Action: {failed_action_name}

Current Workflow Definition:
{json.dumps(workflow_definition, indent=2)[:15000]}

Return the COMPLETE FIXED workflow definition as JSON."""
        
        logger.info(f"[LLM] Generating fix for {failed_action_name}...")
        
        response = requests.post(
            url,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 8000,
            },
            timeout=120,
        )
        
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].strip()
        
        # Extract JSON from response
        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            fixed_definition = json.loads(json_match.group())
            logger.info("[LLM] ✅ Successfully generated fixed workflow definition")
            return fixed_definition
        else:
            logger.warning("[LLM] No valid JSON found in LLM response")
            return None
        
    except Exception as e:
        logger.error(f"[LLM] Fix generation failed: {e}")
        return None


def _score_failed_action(
    action: Dict[str, Any], definition: Optional[Dict[str, Any]]
) -> int:
    blob = _extract_error_blob(action)
    score = 0
    if blob.get("statusCode") is not None:
        score += 200
    msg = str(blob.get("message") or "")
    if "No dependent actions succeeded" in msg:
        score -= 180
    elif blob.get("code") == "ActionFailed" and "An action failed" in msg and len(msg) < 220:
        score -= 120
    if blob.get("code") == "ActionFailed" and blob.get("statusCode") is None:
        score -= 25

    if not definition:
        return score

    name = _action_display_name(action)
    try:
        _, node = locate_action_node(definition, name)
        t = (node.get("type") or "").lower()
        if t in ("http", "httpwebhook"):
            score += 90
        if t in _CONTAINER_TYPES:
            score -= 110
    except KeyError:
        pass
    return score


def _pick_failed_action(
    actions: List[Dict[str, Any]],
    definition: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    failed: List[Dict[str, Any]] = []
    for a in actions:
        props = a.get("properties") or {}
        if (props.get("status") or "").lower() == "failed":
            failed.append(a)
    if not failed:
        return None
    return max(
        failed,
        key=lambda a: (_score_failed_action(a, definition), _end_time(a)),
    )


def _action_display_name(action_resource: Dict[str, Any]) -> str:
    raw = action_resource.get("name") or ""
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw


def _extract_error_blob(action_resource: Dict[str, Any]) -> Dict[str, Any]:
    props = action_resource.get("properties") or {}
    merged: Dict[str, Any] = {}

    def absorb(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            if v is not None and k not in merged:
                merged[k] = v

    absorb(props.get("error"))
    out = props.get("outputs")
    if isinstance(out, dict):
        absorb(out.get("error"))
        body = out.get("body")
        if isinstance(body, dict):
            absorb(body.get("error"))
            if "statusCode" in body:
                merged.setdefault("statusCode", body.get("statusCode"))
            if "message" in body and "message" not in merged:
                merged["message"] = body.get("message")
        deep_sc = _deep_find_status_code(out)
        if deep_sc is not None:
            merged.setdefault("statusCode", deep_sc)

    if not merged:
        merged["message"] = props.get("status") or "Unknown failure"
    return merged


def _summarize_detected_error(
    action_name: str, error_json: Dict[str, Any], analysis: Dict[str, Any]
) -> str:
    return (
        f"action={action_name} type={analysis.get('error_type')} "
        f"code={error_json.get('code')} status={error_json.get('statusCode')} "
        f"msg={str(error_json.get('message'))[:200]}"
    )


def _build_flow_context(
    failed: Dict[str, Any],
    workflow_name: str,
    run_id: str,
    definition: Optional[Dict[str, Any]],
    action_name: str,
    preview_limit: int = 6000,
) -> Dict[str, Any]:
    props = failed.get("properties") or {}
    ctx: Dict[str, Any] = {
        "workflow_name": workflow_name,
        "run_id": run_id,
        "failed_action_name": action_name,
        "action_status": props.get("status"),
        "action_start_time": props.get("startTime"),
        "action_end_time": props.get("endTime"),
    }
    if definition and action_name:
        try:
            _, node = locate_action_node(definition, action_name)
            ctx["action_type"] = node.get("type")
        except KeyError:
            ctx["action_type"] = None
    else:
        ctx["action_type"] = None

    for key in ("inputs", "outputs"):
        raw = props.get(key)
        blob = json.dumps(raw, default=str) if raw is not None else ""
        if len(blob) > preview_limit:
            blob = blob[:preview_limit] + "...(truncated)"
        ctx[f"action_{key}_preview"] = blob
    return ctx


def _analysis_extras(analysis: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: analysis.get(k)
        for k in (
            "exact_error_in_flow",
            "exact_error_code",
            "exact_error_message",
            "root_cause",
            "rag_enriched",
            "rag_retrieval_mode",
            "retrieved_sources",
            "rag_cited_source_ids",
        )
        if analysis.get(k) is not None
    }


def _rca_extras(rca: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: rca.get(k)
        for k in (
            "error_location",
            "action_type",
            "error_code",
            "root_cause",
            "exact_issue",
            "solution",
            "confidence",
        )
        if rca.get(k) is not None
    }


def _extract_arm_error_code(resp_text: str) -> Optional[str]:
    if not resp_text:
        return None
    try:
        data = json.loads(resp_text)
        err = data.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            if code:
                return str(code)
    except Exception:
        return None
    return None


def _is_trigger_enabled(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
) -> bool:
    try:
        trig = get_trigger(token, subscription_id, resource_group, workflow_name, trigger_name)
        state = str((trig.get("properties") or {}).get("state") or "").lower()
        return state in ("", "enabled")
    except Exception:
        return False


def run_remediation(
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
    settings: Optional[Settings] = None,
    backup_dir: Optional[str] = None,
    trigger_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    End-to-end remediation with LLM-powered fixes!
    """
    settings = settings or get_settings()
    token = get_arm_token(
        settings.tenant_id, settings.client_id, settings.client_secret
    )

    run = get_run(
        token, subscription_id, resource_group, workflow_name, run_id
    )
    run_status = (run.get("properties") or {}).get("status") or ""

    actions = list_run_actions(
        token, subscription_id, resource_group, workflow_name, run_id
    )
    wf_early = get_workflow(token, subscription_id, resource_group, workflow_name)
    definition_early = (wf_early.get("properties") or {}).get("definition")
    if not isinstance(definition_early, dict):
        definition_early = None

    failed = _pick_failed_action(actions, definition_early)
    if not failed:
        return {
            "run_id": run_id,
            "error_type": "none",
            "fix_applied": "none",
            "status": "no_error",
            "workflow_run_status": run_status,
            "message": "No error in flow.",
        }

    action_name = _action_display_name(failed)
    error_json = _extract_error_blob(failed)
    flow_context = _build_flow_context(
        failed, workflow_name, run_id, definition_early, action_name
    )
    rca = generate_rca(actions, flow_context=flow_context)
    analysis = analyze_error(error_json, settings, flow_context=flow_context)
    error_type = analysis.get("error_type") or "unknown"

    logger.info("Detected: %s", _summarize_detected_error(action_name, error_json, analysis))

    if error_type == "unknown":
        return {
            "run_id": run_id,
            "error_type": error_type,
            "fix_applied": "skipped",
            "status": "needs_manual_review",
            "workflow_run_status": run_status,
            "failed_action": action_name,
            "recommendation": analysis.get("recommendation"),
            **_rca_extras(rca),
            **_analysis_extras(analysis),
        }

    backup_enabled = bool(backup_dir)
    ts: Optional[str] = None
    if backup_enabled:
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    last_result: Dict[str, Any] = {}
    force_wildcard_etag = False
    fix_applied = False
    
    for attempt in range(1, settings.max_remediation_attempts + 1):
        wf = get_workflow(token, subscription_id, resource_group, workflow_name)
        backup_path: Optional[str] = None
        if backup_enabled and backup_dir and ts:
            backup_path = os.path.join(
                backup_dir, f"workflow_backup_{workflow_name}_{ts}_a{attempt}.json"
            )
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(wf, f, indent=2)
            logger.info("Backed up workflow to %s", backup_path)

        put_body = strip_read_only_for_put(wf)
        etag = "*" if force_wildcard_etag else wf.get("etag")
        
        # ================================================================
        # STEP 1: Try LLM-based fix (INTELLIGENT FIX)
        # ================================================================
        patched = None
        llm_fix_used = False
        
        if settings.azure_openai_endpoint and settings.azure_openai_api_key:
            logger.info(f"[LLM] Attempting intelligent fix for {action_name}...")
            
            llm_fixed_definition = _get_llm_fix_for_workflow(
                definition_early or {},
                action_name,
                error_json.get("message", ""),
                error_type,
                settings,
            )
            
            if llm_fixed_definition:
                logger.info("[LLM] ✅ Using LLM-generated workflow definition")
                patched = put_body.copy()
                if "properties" not in patched:
                    patched["properties"] = {}
                if "definition" not in patched["properties"]:
                    patched["properties"]["definition"] = {}
                patched["properties"]["definition"] = llm_fixed_definition
                llm_fix_used = True
            else:
                logger.warning("[LLM] LLM fix failed, falling back to rule-based patch")
        
        # ================================================================
        # STEP 2: Fallback to rule-based patch
        # ================================================================
        if patched is None:
            logger.info(f"[RULE] Using rule-based patch for {action_name}")
            try:
                # Log before patch
                old_uri = _get_action_uri(put_body)
                logger.info(f"[RULE] Current URI: {old_uri}")
                
                patched = apply_remediation_patch(
                    put_body, action_name, error_type, settings, analysis=analysis
                )
                
                # Special handling for Condition contains() null errors
                if error_type == "bad_request" and "contains" in str(error_json.get("message", "")).lower():
                    logger.info("[RULE] Attempting to fix Condition contains() null error...")
                    try:
                        _, node = locate_action_node(patched.get("properties", {}).get("definition", {}), action_name)
                        if fix_condition_contains_null(node, analysis):
                            logger.info("[RULE] ✅ Condition expression fixed!")
                    except Exception as e:
                        logger.warning(f"[RULE] Could not fix Condition: {e}")
                
                new_uri = _get_action_uri(patched)
                logger.info(f"[RULE] New URI: {new_uri}")
                
            except Exception as ex:
                logger.exception("Patch failed")
                return {
                    "run_id": run_id,
                    "error_type": error_type,
                    "fix_applied": "patch_failed",
                    "status": "failed",
                    "failed_action": action_name,
                    "detail": str(ex),
                    "backup_path": backup_path,
                    **_rca_extras(rca),
                    **_analysis_extras(analysis),
                }

        # ================================================================
        # STEP 3: Deploy the fix to Azure
        # ================================================================
        logger.info(
            "Applying fix to %s (attempt %s/%s, %s fix)",
            action_name,
            attempt,
            settings.max_remediation_attempts,
            "LLM" if llm_fix_used else "rule-based",
        )
        
        try:
            put_workflow(
                token,
                subscription_id,
                resource_group,
                workflow_name,
                patched,
                etag=etag,
            )
            logger.info(f"[DEPLOY] ✅ Workflow deployed successfully!")
            fix_applied = True
            
        except requests.HTTPError as he:
            resp_text = ""
            if he.response is not None and he.response.text:
                resp_text = he.response.text[:1000]
            arm_code = _extract_arm_error_code(resp_text)
            deploy_rca = generate_rca_from_error(
                error_code=arm_code or str(getattr(he.response, "status_code", "")),
                error_message=resp_text or str(he),
                error_location=workflow_name,
                action_type="workflow_put",
                flow_context=flow_context,
            )
            logger.warning("PUT workflow failed (attempt %s): %s", attempt, he)
            
            if arm_code in _NON_RETRYABLE_ARM_CODES:
                return {
                    "run_id": run_id,
                    "error_type": error_type,
                    "fix_applied": analysis.get("fix_type", ""),
                    "status": "subscription_read_only" if arm_code == "ReadOnlyDisabledSubscription" else "deploy_blocked",
                    "failed_action": action_name,
                    "detail": str(he),
                    "error_body": resp_text,
                    "arm_error_code": arm_code,
                    "backup_path": backup_path,
                    "http_status": getattr(he.response, "status_code", None),
                    **_rca_extras(deploy_rca),
                    **_analysis_extras(analysis),
                }
            
            if attempt < settings.max_remediation_attempts and he.response is not None:
                code = he.response.status_code
                if code in (409, 412, 429) or 500 <= code < 600:
                    if code in (409, 412):
                        force_wildcard_etag = True
                    time.sleep(3 * attempt)
                    continue
            
            return {
                "run_id": run_id,
                "error_type": error_type,
                "fix_applied": analysis.get("fix_type", ""),
                "status": "deploy_failed",
                "failed_action": action_name,
                "detail": str(he),
                "error_body": resp_text,
                "backup_path": backup_path,
                "http_status": getattr(he.response, "status_code", None),
                **_rca_extras(deploy_rca),
                **_analysis_extras(analysis),
            }

        # ================================================================
        # STEP 4: Trigger test run to verify fix
        # ================================================================
        definition = patched.get("properties", {}).get("definition") or {}
        trig = trigger_name or find_manual_or_recurrence_trigger(definition)
        
        if trig and not _is_trigger_enabled(
            token, subscription_id, resource_group, workflow_name, trig
        ):
            logger.warning("Trigger '%s' is disabled; skipping trigger run.", trig)
            trig = None
            
        new_run_status = "trigger_skipped"
        new_run_id = None
        
        if trig:
            logger.info(f"[VERIFY] Triggering test run with '{trig}'...")
            resp = post_trigger_run(
                token,
                subscription_id,
                resource_group,
                workflow_name,
                trig,
                body={},
            )
            if resp.status_code in (200, 202):
                try:
                    loc = resp.headers.get("Location") or ""
                    parts = [p for p in loc.rstrip("/").split("/") if p]
                    if parts:
                        new_run_id = parts[-1]
                        logger.info(f"[VERIFY] Test run triggered: {new_run_id}")
                except Exception:
                    pass
                new_run_status = "trigger_accepted"
            else:
                new_run_status = f"trigger_http_{resp.status_code}"
                logger.warning("Trigger run response: %s", resp.status_code)
        else:
            logger.warning("No trigger found; workflow updated but not re-run.")

        fix_desc = f"{analysis.get('fix_type')} on {action_name} (LLM: {llm_fix_used})"
        last_result = {
            "run_id": run_id,
            "error_type": error_type,
            "fix_applied": fix_desc,
            "fix_method": "llm" if llm_fix_used else "rule_based",
            "status": "remediated",
            "workflow_run_status": run_status,
            "failed_action": action_name,
            "backup_path": backup_path,
            "new_trigger_status": new_run_status,
            "new_run_id": new_run_id,
            "remediation_attempt": attempt,
            "recommendation": analysis.get("recommendation"),
            "llm_fix_generated": llm_fix_used,
            **_rca_extras(rca),
            **_analysis_extras(analysis),
        }
        logger.info("Result: %s", json.dumps(last_result, default=str))
        break

    if not fix_applied and not last_result:
        last_result = {
            "run_id": run_id,
            "error_type": error_type,
            "fix_applied": "none",
            "status": "failed",
            "failed_action": action_name,
            "message": "No fix was applied",
        }

    return last_result