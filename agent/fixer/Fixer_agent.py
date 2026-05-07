"""
Enhanced Fixer Agent - Uses LLM to generate fixes based on RCAResult.
Then deploys to Azure.
"""

from __future__ import annotations

import copy
import json
import logging
import re
import time
from typing import Any, Dict, Optional
from datetime import datetime, timezone

import requests

# Add parent directory to path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.services.workflow_service import get_workflow, put_workflow
from auth import get_arm_token
from config import Settings, get_settings
from remediation import strip_read_only_for_put, locate_action_node, fix_condition_contains_null

# Import LLM client for dynamic fix generation
try:
    from common.llm.llm_client import AICoreLLMClient
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False

logger = logging.getLogger(__name__)


def _arm_http_error_detail(exc: requests.HTTPError, max_len: int = 1800) -> str:
    """Include Azure ARM error JSON/text so 400 responses are actionable."""
    base = str(exc)
    resp = getattr(exc, "response", None)
    if resp is None or not getattr(resp, "text", "").strip():
        return base[:max_len]
    text = resp.text.strip()
    try:
        data = resp.json()
        err = data.get("error") or {}
        msg = err.get("message") or err.get("code")
        inner = err.get("details") or []
        if isinstance(inner, list) and inner:
            first = inner[0] if isinstance(inner[0], dict) else {}
            msg = msg or first.get("message") or first.get("code")
        if msg:
            return f"{base} | ARM: {msg}"[:max_len]
    except Exception:
        pass
    return f"{base} | ARM body: {text[:1200]}"[:max_len]


# Connector input keys whose string values are often Drive-like paths (leading '/' matters).
_PATH_KEYS_LOWER = frozenset(
    {
        "path",
        "filepath",
        "folderpath",
        "itempath",
        "sourcepath",
        "destinationpath",
        "file_path",
        "folder_path",
    }
)


class FixerAgent:
    """
    Intelligent Fixer - Uses LLM to generate fixes based on RCAResult.
    Then deploys to Azure.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._token: Optional[str] = None
    
    @property
    def token(self) -> str:
        if not self._token:
            self._token = get_arm_token(
                self.settings.tenant_id,
                self.settings.client_id,
                self.settings.client_secret,
            )
        return self._token
    
    @staticmethod
    def _normalize_leading_slash_path_fields_in_place(obj: Any) -> bool:
        """
        Prepend '/' to literal path-like connector inputs when missing (e.g. Drive path segments).
        Skips expressions (@...) and absolute URLs.
        """
        changed = False
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                kl = k.lower() if isinstance(k, str) else ""
                if isinstance(v, str) and kl in _PATH_KEYS_LOWER:
                    s = v.strip()
                    if (
                        s
                        and not s.startswith(("@", "/", "http://", "https://"))
                        and not s.startswith("ftp://")
                    ):
                        obj[k] = "/" + s.lstrip("/")
                        changed = True
                elif isinstance(v, (dict, list)):
                    if FixerAgent._normalize_leading_slash_path_fields_in_place(v):
                        changed = True
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    if FixerAgent._normalize_leading_slash_path_fields_in_place(item):
                        changed = True
        return changed
    
    def fix(self, rca_result: Dict[str, Any], workflow_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to generate fix strategy from RCA, then apply to Azure.
        """
        workflow_name = workflow_context.get("workflow_name")
        run_id = workflow_context.get("run_id")
        failed_action_name = workflow_context.get("failed_action_name")
        
        logger.info(f"[FIXER] Generating fix for {failed_action_name}")
        
        try:
            # 1. Get current workflow
            workflow = get_workflow(
                token=self.token,
                subscription_id=workflow_context.get("subscription_id"),
                resource_group=workflow_context.get("resource_group"),
                workflow_name=workflow_name,
            )
            
            # 2. Get the failed action details
            definition = workflow.get("properties", {}).get("definition", {})
            try:
                _, failed_action = locate_action_node(definition, failed_action_name)
            except Exception:
                failed_action = {}
            action_type = (failed_action or {}).get("type", "unknown")
            
            # 3. Generate fix strategy based on error type
            root_cause = rca_result.get("root_cause", "unknown")
            exact_issue = rca_result.get("exact_issue", "")
            runtime_hint = str(workflow_context.get("failed_action_runtime_json") or "").strip()
            
            fix_strategy = self._generate_fix_strategy(
                root_cause=root_cause,
                exact_issue=exact_issue,
                action_type=action_type,
                action_config=failed_action,
                error_message=(
                    f"{rca_result.get('exact_issue', '')} {rca_result.get('solution', '')}"
                ),
                runtime_hint=runtime_hint,
            )
            
            logger.info(f"[FIXER] Fix strategy: {fix_strategy.get('strategy_description', 'N/A')}")
            
            # 4. Apply the fix
            fixed_workflow = self._apply_fix_to_workflow(
                workflow=workflow,
                fix_strategy=fix_strategy,
                failed_action_name=failed_action_name,
            )
            
            if not fixed_workflow:
                return {
                    "success": False,
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "error": "Failed to apply fix to workflow",
                }
            
            if not fix_strategy.get("changes_applied", True):
                return {
                    "success": False,
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "error": "No applicable fix changes generated",
                }

            # 5. Deploy to Azure
            result = self._deploy_workflow_fix(
                subscription_id=workflow_context.get("subscription_id"),
                resource_group=workflow_context.get("resource_group"),
                workflow_name=workflow_name,
                fixed_workflow=fixed_workflow,
                etag=workflow.get("etag"),
            )
            
            if result.get("success"):
                logger.info(f"[FIXER] Fix deployed successfully")
                return {
                    "success": True,
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "root_cause": root_cause,
                    "fix_strategy": fix_strategy,
                    "changes_applied": fix_strategy.get("changes", {}),
                }
            else:
                return {
                    "success": False,
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "error": result.get("error", "Deployment failed"),
                }
            
        except Exception as e:
            logger.error(f"[FIXER] Error: {e}", exc_info=True)
            return {
                "success": False,
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error": str(e),
            }
    
    def _generate_fix_strategy(
        self,
        root_cause: str,
        exact_issue: str,
        action_type: str,
        action_config: Dict[str, Any],
        error_message: str,
        runtime_hint: str = "",
    ) -> Dict[str, Any]:
        """Generate fix strategy based on error type."""
        
        root = str(root_cause or "").lower()
        details = str(error_message or "").lower()
        signal = f"{root} {details}"

        # Connector / file NotFound phrases (RCA often omits literal "404")
        connector_path_not_found = any(
            p in signal
            for p in (
                "could not be matched",
                "path-based lookup",
                "specified path could not",
                "didn't resolve to",
                "notfound because",
                "within the authenticated drive",
            )
        )

        # Strategy for contains() null / payload schema issues
        if root in ("payload_or_schema_error", "null_reference_error") or (
            "contains" in signal and "null" in signal
        ):
            return {
                "strategy_description": "Fix contains() null error by wrapping first argument with coalesce()",
                "fix_type": "contains_null_guard",
                "changes": {},
                "explanation": "The contains() function received null. Wrapping with coalesce() provides a default empty string.",
                "risk": "low",
                "changes_applied": True,
            }
        
        # Strategy for missing property error
        elif "property" in signal and "doesn't exist" in signal:
            # Extract property name from error
            import re
            prop_match = re.search(r"'([^']+)'", error_message)
            prop_name = prop_match.group(1) if prop_match else "unknown"
            
            return {
                "strategy_description": f"Fix missing property '{prop_name}' with safe navigation",
                "changes": {
                    "inputs": self._fix_missing_property_inputs(action_config.get("inputs", {}), prop_name)
                },
                "explanation": f"The property '{prop_name}' was missing. Using safe navigation (?['{prop_name}']) prevents the error.",
                "risk": "low",
                "changes_applied": True,
            }
        
        # Strategy for 404 — HTTP vs connectors (do not overwrite Drive/API inputs with uri)
        elif (
            root == "not_found"
            or "404" in signal
            or "not found" in signal
            or connector_path_not_found
        ):
            inputs_cfg = action_config.get("inputs") or {}
            action_lower = str(action_type or "").lower()
            is_http_like = (
                action_lower == "http"
                or "httprequest" in action_lower
                or bool(inputs_cfg.get("uri"))
            )
            if is_http_like:
                return {
                    "strategy_description": "Fix 404 error by updating HTTP URI",
                    "changes": {"inputs": {"uri": self.settings.fallback_http_url}},
                    "explanation": "The endpoint returned 404. Updated to configured fallback HTTP URL.",
                    "risk": "medium",
                    "changes_applied": True,
                }
            probe = copy.deepcopy(inputs_cfg)
            if self._normalize_leading_slash_path_fields_in_place(probe):
                return {
                    "strategy_description": "Normalize connector path with leading '/' for 404 NotFound",
                    "fix_type": "path_leading_slash_normalize",
                    "changes": {},
                    "explanation": "Relative path literal detected on connector input; prepended '/' per provider conventions.",
                    "risk": "low",
                    "changes_applied": True,
                }
            return {
                "strategy_description": "Add retry policy for connector 404",
                "fix_type": "retry_fixed",
                "changes": {"retryPolicy": {"type": "fixed", "count": 4, "interval": "PT20S"}},
                "explanation": "404 on connector; retry only (cannot infer correct resource path). See runtime snapshot for resolved inputs.",
                "risk": "low",
                "changes_applied": True,
            }
        
        elif root == "timeout" or "timed out" in signal or "timeout" in signal:
            return {
                "strategy_description": "Add fixed retry policy for timeout errors",
                "fix_type": "retry_fixed",
                "changes": {"retryPolicy": {"type": "fixed", "count": 4, "interval": "PT20S"}},
                "explanation": "Timeout detected; added fixed retry policy.",
                "risk": "low",
                "changes_applied": True,
            }
        
        elif root == "throttling" or "429" in signal or "throttl" in signal:
            return {
                "strategy_description": "Add exponential retry policy for throttling errors",
                "fix_type": "retry_exponential",
                "changes": {
                    "retryPolicy": {
                        "type": "exponential",
                        "count": 6,
                        "interval": "PT10S",
                        "minimumInterval": "PT5S",
                        "maximumInterval": "PT1M",
                    }
                },
                "explanation": "Throttling detected; added exponential backoff retry policy.",
                "risk": "low",
                "changes_applied": True,
            }

        elif root == "auth_or_authorization_error" or "unauthorized" in signal or "forbidden" in signal or "authorization" in signal:
            return {
                "strategy_description": "Mark action for auth/connection verification",
                "fix_type": "auth_connection_check",
                "changes": {
                    "_auto_fix_metadata": {
                        "note": "Verify connection reference, identity, and secret bindings."
                    }
                },
                "explanation": "Auth issue detected; action marked for connection reference verification.",
                "risk": "low",
                "changes_applied": True,
            }

        elif ("divide" in signal and "zero" in signal) or "function 'div'" in signal or " div(" in signal:
            return {
                "strategy_description": "Guard div() denominator to prevent divide-by-zero in Compose expression",
                "fix_type": "div_zero_guard",
                "changes": {},
                "explanation": "Division by zero detected; wrap denominator check and return safe fallback.",
                "risk": "low",
                "changes_applied": True,
            }
        
        # NEW: Use LLM to generate fix for unknown error patterns
        else:
            logger.info(f"[FIXER] No known pattern matched for '{root_cause}', trying LLM...")
            
            # Try to get LLM-generated fix
            llm_fix = self._generate_fix_with_llm(
                action_config=action_config,
                action_name=action_config.get("name", "unknown"),
                action_type=action_type,
                error_message=error_message,
                root_cause=root_cause,
                exact_issue=exact_issue,
                runtime_hint=runtime_hint,
            )
            
            if llm_fix:
                return {
                    "strategy_description": f"LLM-generated fix for {root_cause}",
                    "fix_type": "llm_generated",
                    "changes": llm_fix,  # The entire fixed action config
                    "explanation": f"LLM analyzed the error and generated a fix: {exact_issue}",
                    "risk": "medium",
                    "changes_applied": True,
                }
            
            # Fallback: no fix available
            return {
                "strategy_description": f"No fix available for {root_cause}",
                "changes": {},
                "explanation": exact_issue,
                "risk": "low",
                "changes_applied": False,
            }
    
    def _fix_contains_expression(self, expression: Any) -> str:
        """Fix contains() expression with coalesce."""
        if not isinstance(expression, str):
            return expression
        
        import re
        pattern = r"contains\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)"
        
        def wrap_first_arg(match):
            first_arg = match.group(1).strip()
            second_arg = match.group(2).strip()
            return f"contains(coalesce({first_arg}, ''), {second_arg})"
        
        return re.sub(pattern, wrap_first_arg, expression)

    def _fix_div_zero_expression(self, expression: Any) -> Any:
        """Guard div(numerator, denominator) calls with denominator zero check."""
        if not isinstance(expression, str):
            return expression
        s = expression
        i = 0
        out: list[str] = []
        changed = False

        while i < len(s):
            if s[i : i + 4].lower() == "div(":
                start = i
                j = i + 4
                depth = 1
                while j < len(s) and depth > 0:
                    ch = s[j]
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    j += 1
                if depth != 0:
                    # Unbalanced expression; keep original tail as-is.
                    out.append(s[i:])
                    break

                inner = s[i + 4 : j - 1]
                # Split numerator/denominator on top-level comma.
                k = 0
                part_depth = 0
                comma_idx = -1
                while k < len(inner):
                    ch = inner[k]
                    if ch == "(":
                        part_depth += 1
                    elif ch == ")":
                        part_depth -= 1
                    elif ch == "," and part_depth == 0:
                        comma_idx = k
                        break
                    k += 1

                if comma_idx == -1:
                    # Not a standard div(a,b), keep original segment.
                    out.append(s[start:j])
                    i = j
                    continue

                num = inner[:comma_idx].strip()
                den = inner[comma_idx + 1 :].strip()
                guarded = (
                    f"if(equals(coalesce({den}, 0), 0), 0, div({num}, {den}))"
                )
                out.append(guarded)
                i = j
                changed = True
            else:
                out.append(s[i])
                i += 1

        return "".join(out) if changed else expression

    def _apply_div_zero_guard_recursive(self, node: Any) -> bool:
        changed = False
        if isinstance(node, dict):
            for k, v in list(node.items()):
                if isinstance(v, str):
                    nv = self._fix_div_zero_expression(v)
                    if nv != v:
                        node[k] = nv
                        changed = True
                elif isinstance(v, (dict, list)):
                    if self._apply_div_zero_guard_recursive(v):
                        changed = True
        elif isinstance(node, list):
            for i, v in enumerate(node):
                if isinstance(v, str):
                    nv = self._fix_div_zero_expression(v)
                    if nv != v:
                        node[i] = nv
                        changed = True
                elif isinstance(v, (dict, list)):
                    if self._apply_div_zero_guard_recursive(v):
                        changed = True
        return changed
    
    def _fix_missing_property_inputs(self, inputs: Dict[str, Any], prop_name: str) -> Dict[str, Any]:
        """Fix inputs that reference missing property."""
        fixed = inputs.copy() if isinstance(inputs, dict) else {}
        
        for key, value in fixed.items():
            if isinstance(value, str) and f"['{prop_name}']" in value:
                # Add safe navigation
                fixed[key] = value.replace(f"['{prop_name}']", f"?['{prop_name}']")
                # Wrap with coalesce
                fixed[key] = fixed[key].replace(f"?['{prop_name}']", f"?['{prop_name}']")
                fixed[key] = f"coalesce({fixed[key]}, '')"
        
        return fixed
    
    def _generate_fix_with_llm(
        self,
        action_config: Dict[str, Any],
        action_name: str,
        action_type: str,
        error_message: str,
        root_cause: str,
        exact_issue: str,
        runtime_hint: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Use LLM to generate a fix for the failed action.
        Returns the fixed action configuration or None if LLM fails.
        """
        if not LLM_AVAILABLE:
            logger.warning("[FIXER-LLM] LLM client not available")
            return None
        
        try:
            llm_client = AICoreLLMClient.from_env()
        except Exception as e:
            logger.warning(f"[FIXER-LLM] Failed to initialize LLM client: {e}")
            return None
        
        # Create the prompt for LLM
        system_prompt = """You are an Azure Logic Apps expert who fixes workflow errors.

Given a failed action configuration and error details, return a FIXED version of the action configuration as JSON.

Rules:
1. Only return the fixed action JSON object, nothing else
2. Keep the structure intact, only modify what's needed to fix the error
3. Common fixes include:
   - Wrapping expressions with coalesce() for null errors
   - Adding retry policies for timeout/throttling
   - Fixing invalid expressions or syntax
   - Adding error handling with runAfter configurations
   - Fixing URL or path issues (including leading '/' on connector path fields when inputs show a relative path)
4. If runtime_inputs_outputs_json is present, use it to see evaluated paths/payloads at failure time; align expressions or literals accordingly.
5. Do NOT change the action type
6. Return valid JSON only, no markdown, no explanation

Example output format:
{"type": "Http", "inputs": {"method": "GET", "uri": "https://..."}, "retryPolicy": {"type": "fixed", "count": 3}}
"""

        payload = {
            "action_name": action_name,
            "action_type": action_type,
            "current_action_config": action_config,
            "error_details": {
                "root_cause": root_cause,
                "exact_issue": exact_issue,
                "error_message": error_message,
            },
            "instruction": "Fix this action to resolve the error. Return only the fixed action JSON.",
        }
        if runtime_hint:
            payload["runtime_inputs_outputs_json"] = runtime_hint

        user_prompt = json.dumps(payload, indent=2, default=str)
        
        logger.info(f"[FIXER-LLM] Asking LLM to generate fix for {action_name}")
        
        try:
            result = llm_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                required_keys=None,  # Action structure varies
            )
            
            if result and isinstance(result, dict):
                # Validate the response has basic structure
                if "type" in result or "inputs" in result or "expression" in result:
                    logger.info(f"[FIXER-LLM] LLM generated fix successfully")
                    return result
                else:
                    logger.warning("[FIXER-LLM] LLM response missing expected action fields")
                    return None
            else:
                logger.warning("[FIXER-LLM] LLM returned invalid response")
                return None
                
        except Exception as e:
            logger.error(f"[FIXER-LLM] LLM call failed: {e}")
            return None
    
    def _apply_fix_to_workflow(
        self,
        workflow: Dict[str, Any],
        fix_strategy: Dict[str, Any],
        failed_action_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Apply the fix to the workflow definition."""
        try:
            fixed_workflow = copy.deepcopy(workflow)
            definition = fixed_workflow.get("properties", {}).get("definition", {})
            try:
                _, action_node = locate_action_node(definition, failed_action_name)
            except Exception:
                logger.error(f"[FIXER] Action {failed_action_name} not found")
                return None

            changes = fix_strategy.get("changes", {})
            fix_type = fix_strategy.get("fix_type")
            
            # Apply changes
            if fix_type == "contains_null_guard":
                rca_stub = {
                    "root_cause": "payload_or_schema_error",
                    "exact_issue": fix_strategy.get("explanation", ""),
                    "solution": fix_strategy.get("strategy_description", ""),
                }
                if not fix_condition_contains_null(action_node, None, error_json=None, rca=rca_stub):
                    logger.warning("[FIXER] contains_null_guard strategy produced no node changes")
                    return None
            elif fix_type == "div_zero_guard":
                if not self._apply_div_zero_guard_recursive(action_node):
                    logger.warning("[FIXER] div_zero_guard strategy produced no node changes")
                    return None
            elif fix_type == "path_leading_slash_normalize":
                inputs_obj = action_node.get("inputs")
                if not isinstance(inputs_obj, dict):
                    logger.warning("[FIXER] path_leading_slash_normalize: no inputs dict")
                    return None
                if not self._normalize_leading_slash_path_fields_in_place(inputs_obj):
                    logger.warning("[FIXER] path_leading_slash_normalize produced no changes")
                    return None
                logger.info(f"[FIXER] Leading-slash path normalize applied to {failed_action_name}")
            elif fix_type == "llm_generated":
                # LLM generated a complete fixed action - merge carefully
                logger.info("[FIXER] Applying LLM-generated fix")
                llm_changes = changes  # This is the full action config from LLM
                
                # Preserve critical fields from original action
                original_type = action_node.get("type")
                original_run_after = action_node.get("runAfter")
                
                # Update action with LLM changes
                for field, value in llm_changes.items():
                    if field == "inputs" and isinstance(action_node.get("inputs"), dict) and isinstance(value, dict):
                        # Merge inputs rather than replace
                        action_node["inputs"].update(value)
                    elif field == "type":
                        # Don't change action type - keep original
                        continue
                    elif field == "runAfter" and original_run_after:
                        # Preserve original runAfter unless LLM explicitly set it
                        continue
                    else:
                        action_node[field] = value
                
                # Ensure type is preserved
                if original_type:
                    action_node["type"] = original_type
                
                logger.info(f"[FIXER] LLM fix applied to {failed_action_name}")
            else:
                for field, value in changes.items():
                    if field == "expression":
                        action_node["expression"] = value
                    elif field == "inputs" and isinstance(action_node.get("inputs"), dict):
                        action_node["inputs"].update(value)
                    else:
                        action_node[field] = value
            
            logger.info(f"[FIXER] Applied fix to {failed_action_name}")
            return fixed_workflow
            
        except Exception as e:
            logger.error(f"[FIXER] Failed to apply fix: {e}")
            return None
    
    def _deploy_workflow_fix(
        self,
        subscription_id: str,
        resource_group: str,
        workflow_name: str,
        fixed_workflow: Dict[str, Any],
        etag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Deploy fixed workflow to Azure."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[FIXER] Deploying to Azure (attempt {attempt + 1})...")
                body = strip_read_only_for_put(fixed_workflow)
                
                result = put_workflow(
                    token=self.token,
                    subscription_id=subscription_id,
                    resource_group=resource_group,
                    workflow_name=workflow_name,
                    workflow_body=body,
                    etag=etag if attempt == 0 else "*",
                )
                
                logger.info(f"[FIXER] Successfully deployed")
                return {"success": True, "updated_workflow": result}
                
            except requests.HTTPError as e:
                if e.response and e.response.status_code in (409, 412) and attempt < max_retries - 1:
                    logger.warning(f"[FIXER] Conflict, retrying...")
                    time.sleep(2 ** attempt)
                    continue
                detail = _arm_http_error_detail(e)
                logger.error("[FIXER] PUT workflow failed: %s", detail)
                return {"success": False, "error": detail}
            
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        return {"success": False, "error": "Max retries exceeded"}
    
# Singleton
_fixer_instance = None

def get_fixer(settings: Optional[Settings] = None) -> FixerAgent:
    global _fixer_instance
    if _fixer_instance is None:
        _fixer_instance = FixerAgent(settings)
    return _fixer_instance