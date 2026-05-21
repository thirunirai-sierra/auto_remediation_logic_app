# server/services/agents/fixer/Fixer_agent.py
"""
Enhanced Fixer Agent - Uses LLM to generate fixes based on RCAResult.
Then deploys to Azure.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, Optional

import requests

from services.auth import get_arm_token
from config import Settings, get_settings
from services.agents.fixer.remediation import strip_read_only_for_put, locate_action_node, fix_condition_contains_null
from services.workflow_service import get_workflow, put_workflow

logger = logging.getLogger(__name__)

# Action types that support retryPolicy on Azure Logic Apps
_RETRY_ELIGIBLE_TYPES = frozenset({
    "http", "httpwebhook", "apiconnection", "apiconnectionwebhook",
    "apiconnectionnotification", "function", "serviceprovider", "workflow",
})


def _collect_all_action_nodes(definition: Dict[str, Any]) -> list:
    """Return a flat list of (name, node) for every action in the workflow definition."""
    results: list = []

    def walk(actions_obj: Any) -> None:
        if not isinstance(actions_obj, dict):
            return
        for name, node in actions_obj.items():
            if not isinstance(node, dict):
                continue
            results.append((name, node))
            if isinstance(node.get("actions"), dict):
                walk(node["actions"])
            else_block = node.get("else")
            if isinstance(else_block, dict) and isinstance(else_block.get("actions"), dict):
                walk(else_block["actions"])

    walk(definition.get("actions") or {})
    return results


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
                self.settings.AZURE_TENANT_ID,        # uppercase
                self.settings.AZURE_CLIENT_ID,        # uppercase
                self.settings.AZURE_CLIENT_SECRET,    # uppercase
            )
        return self._token

    def fix(self, rca_result: Dict[str, Any], workflow_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to generate fix strategy from RCA, then apply to Azure.
        """
        workflow_name = workflow_context.get("workflow_name")
        run_id = workflow_context.get("run_id")
        failed_action_name = workflow_context.get("failed_action_name")

        logger.info("[FIXER] Generating fix for %s", failed_action_name)

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

            fix_strategy = self._generate_fix_strategy(
                root_cause=root_cause,
                exact_issue=exact_issue,
                action_type=action_type,
                action_config=failed_action,
                error_message=(
                    f"{rca_result.get('exact_issue', '')} {rca_result.get('solution', '')}"
                ),
            )

            logger.info("[FIXER] Fix strategy: %s", fix_strategy.get("strategy_description", "N/A"))

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

            # 🔍 LOG THE PATCHED WORKFLOW JSON BEFORE DEPLOYMENT
            patched_json = json.dumps(fixed_workflow, indent=2)
            logger.info("=== PATCHED WORKFLOW JSON (first 3000 chars) ===")
            logger.info(patched_json[:3000])
            if len(patched_json) > 3000:
                logger.info("... (truncated)")

            # Optional dry-run (prevent actual deployment)
            if getattr(self.settings, "DRY_RUN", False):
                logger.warning("[FIXER] DRY RUN – skipping actual deployment")
                return {
                    "success": False,
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "error": "Dry run, no deployment",
                    "patched_workflow": fixed_workflow,
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
                logger.info("[FIXER] Fix deployed successfully")
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
            logger.error("[FIXER] Error: %s", e, exc_info=True)
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
    ) -> Dict[str, Any]:
        """Generate fix strategy based on error type."""
        root = str(root_cause or "").lower()
        details = str(error_message or "").lower()
        signal = f"{root} {details}"

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

        # Strategy for 404 error – use uppercase FALLBACK_HTTP_URL
        elif root == "not_found" or "404" in signal or "not found" in signal:
            return {
                "strategy_description": "Fix 404 error by updating API endpoint",
                "changes": {
                    "inputs": {"uri": self.settings.FALLBACK_HTTP_URL}   # uppercase
                },
                "explanation": "The endpoint returned 404. Updated to a working fallback endpoint.",
                "risk": "medium",
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

        # Generic strategy
        else:
            return {
                "strategy_description": f"Apply standard fix for {root_cause}",
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
    
    def _apply_fix_to_workflow(
        self,
        workflow: Dict[str, Any],
        fix_strategy: Dict[str, Any],
        failed_action_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Apply the fix to the workflow definition."""
        import copy
        
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
            else:
                action_type = (action_node.get("type") or "").lower()
                for field, value in changes.items():
                    if field.startswith("_"):
                        continue  # skip internal metadata — Azure rejects unknown properties
                    if field == "retryPolicy" and action_type not in _RETRY_ELIGIBLE_TYPES:
                        logger.warning(
                            "[FIXER] Skipping retryPolicy on action '%s' (type=%s) — not supported by ARM",
                            failed_action_name, action_type,
                        )
                        continue
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

    def _apply_fallback_patch(
        self,
        fixed_workflow: Dict[str, Any],
        definition: Dict[str, Any],
        fix_strategy: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Last-resort patch when the specific failed action cannot be located.
        For retry/throttle/timeout fixes: adds retryPolicy to every HTTP action.
        For auth fixes: no structural change — returns workflow for re-deploy attempt.
        """
        fix_type = fix_strategy.get("fix_type", "")
        changes = fix_strategy.get("changes", {})
        retry_policy = changes.get("retryPolicy")

        if retry_policy and fix_type in ("retry_fixed", "retry_exponential", "fallback_retry", ""):
            all_actions = _collect_all_action_nodes(definition)
            patched = 0
            for name, node in all_actions:
                node_type = (node.get("type") or "").lower()
                if node_type in ("http", "httpwebhook", "apiconnection"):
                    node["retryPolicy"] = retry_policy
                    patched += 1
            if patched:
                logger.info("[FIXER] Fallback: applied retryPolicy to %d HTTP action(s)", patched)
                return fixed_workflow
            top_actions = definition.get("actions") or {}
            patched_top = 0
            for node in top_actions.values():
                if isinstance(node, dict):
                    node_type = (node.get("type") or "").lower()
                    if node_type in _RETRY_ELIGIBLE_TYPES:
                        node["retryPolicy"] = retry_policy
                        patched_top += 1
            logger.info("[FIXER] Fallback: applied retryPolicy to %d eligible top-level actions", patched_top)
            return fixed_workflow

        if fix_type in ("auth_connection_check",) or not any(
            not k.startswith("_") for k in changes
        ):
            logger.info("[FIXER] Fallback: re-deploying workflow without structural changes (auth/metadata fix)")
            return fixed_workflow

        all_actions = _collect_all_action_nodes(definition)
        patched = 0
        for name, node in all_actions:
            node_type = (node.get("type") or "").lower()
            for field, value in changes.items():
                if field.startswith("_"):
                    continue
                if field == "retryPolicy" and node_type not in _RETRY_ELIGIBLE_TYPES:
                    continue
                if field == "inputs" and isinstance(node.get("inputs"), dict):
                    node["inputs"].update(value)
                else:
                    node[field] = value
            patched += 1
        if patched:
            logger.info("[FIXER] Fallback: applied changes to %d action(s)", patched)
            return fixed_workflow

        logger.error("[FIXER] Fallback exhausted — no applicable patch found")
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
                return {"success": False, "error": str(e)[:200]}
            
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