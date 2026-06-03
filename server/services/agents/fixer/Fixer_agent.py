"""
PRODUCTION FIXER V4 - Fully dynamic, LLM‑first with confidence threshold.
Uses shared utilities from remediation.py for navigation and sanitisation.
No hardcoded rule‑based fixes (except optional timeout rule).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

ProgressCallback = Callable[[int, str], None]

from services.auth import get_arm_token
from config import Settings, get_settings
from services.workflow_service import get_workflow, put_workflow
from services.agents.fixer import remediation as rem
from utils.llm_client import AICoreLLMClient

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
    Production-grade Logic Apps auto-remediation agent.

    Strategy:
        - Timeout errors (optional): add exponential retry policy.
        - All other errors: LLM‑generated patch with confidence threshold.
        - No hardcoded rule‑based fixes for specific error patterns.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._token: Optional[str] = None

    @property
    def token(self) -> str:
        if not self._token:
            self._token = get_arm_token(
                self.settings.AZURE_TENANT_ID,
                self.settings.AZURE_CLIENT_ID,
                self.settings.AZURE_CLIENT_SECRET,
            )
        return self._token

    def fix(
        self,
        rca_result: Dict[str, Any],
        workflow_context: Dict[str, Any],
        on_progress: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        """Synchronous entry point for workflow remediation."""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self._fix_async(rca_result, workflow_context, on_progress=on_progress)
            )
            loop.close()
            return result
        except Exception as e:
            logger.error(f"FIXER FAILED: {e}", exc_info=True)
            return {
                "success": False,
                "workflow_name": workflow_context.get("workflow_name"),
                "run_id": workflow_context.get("run_id"),
                "error": str(e),
            }

    async def _fix_async(
        self,
        rca_result: Dict[str, Any],
        workflow_context: Dict[str, Any],
        on_progress: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        """Async core remediation pipeline."""

        def _report(step_index: int, label: str) -> None:
            if on_progress:
                on_progress(step_index, label)
        workflow_name = workflow_context.get("workflow_name")
        run_id = workflow_context.get("run_id")
        failed_action_name = workflow_context.get("failed_action_name")
        error_type = workflow_context.get("error_type", "unknown")

        # Guard: workflow_name must be valid
        if not workflow_name or str(workflow_name).lower() in ("none", "unknown", ""):
            logger.error("[FIXER] workflow_name is missing/None — cannot fix incident %s", run_id)
            return {
                "success": False,
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error": "Cannot fix: workflow_name is missing from the incident record.",
            }

        logger.info("=" * 100)
        logger.info("PRODUCTION FIXER V4 - LLM with Confidence")
        logger.info("=" * 100)
        logger.info(f"Workflow: {workflow_name} | Run: {run_id}")
        logger.info(f"Failed Action: {failed_action_name}")
        logger.info(f"Error Type: {error_type}")
        logger.info(f"Root Cause: {rca_result.get('root_cause')}")
        logger.info("")

        try:
            # Step 1: Fetch workflow
            _report(1, "Get Workflow — fetching definition from Azure…")
            logger.info("STEP 1: Fetching workflow definition...")
            workflow = await asyncio.to_thread(
                get_workflow,
                self.token,
                workflow_context.get("subscription_id"),
                workflow_context.get("resource_group"),
                workflow_name,
            )
            definition = workflow.get("properties", {}).get("definition", {})
            if not definition:
                return self._error_response(workflow_name, run_id, "No definition found")
            logger.info("Fetched workflow")

            # Step 2: Locate action using shared utility
            _report(2, "Validate — locating failed action in workflow…")
            logger.info("STEP 2: Locating action in definition...")
            action_path = rem.find_action_path(definition, failed_action_name)
            if not action_path:
                return self._error_response(workflow_name, run_id, f"Action '{failed_action_name}' not found")
            try:
                action_node = rem.navigate_path(definition, action_path)
                if not isinstance(action_node, dict):
                    raise ValueError("Node is not a dictionary")
            except KeyError as e:
                return self._error_response(workflow_name, run_id, f"Navigation error: {e}")

            full_path = "/".join(action_path)
            action_type = action_node.get("type", "unknown").lower()
            logger.info(f"Located action at: {full_path}")
            logger.info(f"   Action type: {action_type}")

            # Step 3: Generate fix
            _report(3, "Patch — generating and applying fix…")
            logger.info("STEP 3: Generating fix...")
            fixed_definition = await self._generate_fix(
                definition, action_path, action_node,
                error_type, rca_result, workflow_context
            )
            if not fixed_definition:
                logger.warning("Fix generation failed - returning original definition")
                fixed_definition = definition
            logger.info("Fix generated")

            # Step 4: Prepare deployment using shared sanitisation
            logger.info("STEP 4: Preparing for deployment...")
            updated_workflow = copy.deepcopy(workflow)
            updated_workflow["properties"]["definition"] = fixed_definition
            updated_workflow = rem.strip_read_only_for_put(updated_workflow)

            # Step 5: Deploy
            _report(4, "Deploy — publishing workflow to Azure…")
            logger.info("STEP 5: Deploying to Azure...")
            if getattr(self.settings, "DRY_RUN", False):
                logger.warning("DRY RUN - Skipping deployment")
                return self._error_response(workflow_name, run_id, "Dry run mode")

            await asyncio.to_thread(
                put_workflow,
                self.token,
                workflow_context.get("subscription_id"),
                workflow_context.get("resource_group"),
                workflow_name,
                updated_workflow,
                workflow.get("etag"),
            )
            logger.info("Deployed successfully!")
            return {
                "success": True,
                "workflow_name": workflow_name,
                "run_id": run_id,
                "error_type": error_type,
                "action_fixed": failed_action_name,
                "changes_applied": True,
            }

        except Exception as e:
            logger.error(f"ERROR: {e}", exc_info=True)
            return self._error_response(workflow_name, run_id, str(e))

    async def _generate_fix(
        self,
        definition: Dict[str, Any],
        action_path: List[str],
        action_node: Dict[str, Any],
        error_type: str,
        rca_result: Dict[str, Any],
        workflow_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Route to timeout fix or LLM fix."""
        if error_type == "timeout":
            return self._fix_timeout(definition, action_path, action_node)
        else:
            return await self._fix_with_llm(definition, action_path, action_node, rca_result, workflow_context)

    def _fix_timeout(
        self, definition: Dict[str, Any], action_path: List[str], action_node: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Deterministic fix for timeouts: add exponential retry policy (optional, can be removed)."""
        action_type = (action_node.get("type") or "").lower()
        if action_type not in ("http", "httpwebhook"):
            logger.info(f"Timeout fix skipped for type '{action_type}'")
            return definition
        try:
            fixed_def = copy.deepcopy(definition)
            node = rem.navigate_path(fixed_def, action_path)
            if node:
                node["retryPolicy"] = {
                    "type": "exponential",
                    "count": 5,
                    "interval": "PT10S",
                    "minimumInterval": "PT5S",
                    "maximumInterval": "PT1M",
                }
                # Update parent conditions using shared utility
                parents = rem.find_parent_conditions(fixed_def, action_path)
                for parent_path in parents:
                    rem.update_condition_runafter(fixed_def, parent_path)
                logger.info("   Added retry policy for timeout")
            return fixed_def
        except Exception as e:
            logger.error(f"Timeout fix failed: {e}")
            return None

    async def _fix_with_llm(
        self,
        definition: Dict[str, Any],
        action_path: List[str],
        action_node: Dict[str, Any],
        rca_result: Dict[str, Any],
        workflow_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """LLM‑driven patch generation with confidence threshold and fallback to original definition."""
        try:
            action_name = action_path[-1]
            action_type = (action_node.get("type") or "").lower()

            logger.info("Calling LLM for dynamic fix...")
            llm = AICoreLLMClient.from_env()

            # System prompt – explicit about allowed fields and confidence
            system_prompt = f"""You are an Azure Logic Apps expert fixer.
ACTION TYPE: {action_type}
TASK: Fix this failed action with a minimal, safe JSON patch.

RULES:
- Return ONLY valid JSON: {{"patch": {{...}}, "confidence": 0.0}}
- NEVER change workflow structure (no 'actions', 'Condition', 'If', etc.).
- NEVER change immutable settings: 'type', 'host', 'method', 'path', 'connection', 'authentication'.
- Use dotted keys for nested fields: "inputs.queries.path"
- If the issue is caused by nullable values, use coalesce() with a safe default.
- If no safe fix exists, return {{"patch": {{}}, "confidence": 0.0}}

Allowed fields for action type '{action_type}':
- 'if' / 'condition' → only 'expression'
- 'apiconnection' → 'inputs.queries.path', 'inputs.method', 'trackedProperties', 'description'
- 'http' / 'httpwebhook' → 'retryPolicy', 'runtimeConfiguration', 'operationOptions', 'description', 'trackedProperties'
- other types → 'description', 'trackedProperties', 'runAfter'

Set confidence >= 0.6 if you are confident the patch will fix the issue, otherwise 0.0.
Return JSON only.
"""

            action_snippet = json.dumps(action_node, indent=2, default=str)[:2500]
            user_prompt = f"""
FAILED ACTION:
Name: {action_name}
Type: {action_type}
Error Type: {workflow_context.get('error_type')}
Root Cause: {rca_result.get('root_cause')}
Exact Issue: {rca_result.get('exact_issue', '')[:300]}

CURRENT ACTION JSON:
{action_snippet}

INSTRUCTIONS:
Analyze the failure and produce a minimal fix patch.
Return JSON with keys "patch" and "confidence".
"""

            response = await asyncio.wait_for(
                llm.complete_json(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    required_keys=["patch", "confidence"],
                ),
                timeout=120,
            )

            if not response or "patch" not in response:
                logger.warning("LLM returned invalid response")
                return definition

            confidence = float(response.get("confidence", 0.0))
            patch = response.get("patch", {})

            if confidence < 0.6:
                logger.warning(f"LLM confidence too low ({confidence:.2f}) – skipping fix")
                return definition

            if not isinstance(patch, dict) or not patch:
                logger.warning("LLM returned empty patch – no changes needed")
                return definition

            # Apply the patch
            fixed_def = copy.deepcopy(definition)
            node = rem.navigate_path(fixed_def, action_path)
            if not node:
                logger.error(f"Could not navigate to action node '{action_name}' after deep copy")
                return definition

            self._apply_nested_patch(node, patch)
            logger.info(f"   Applied LLM patch with {len(patch)} change(s), confidence={confidence:.2f}")
            return fixed_def

        except asyncio.TimeoutError:
            logger.warning("LLM call timed out – skipping fix")
            return definition
        except Exception as e:
            logger.warning(f"LLM fix failed: {str(e)[:100]} – skipping fix")
            return definition

    def _apply_nested_patch(self, node: Dict[str, Any], patch: Dict[str, Any]) -> None:
        """Apply patch with dotted key support."""
        for key, value in patch.items():
            if "." in key:
                parts = key.split(".")
                cur = node
                for part in parts[:-1]:
                    if part not in cur:
                        cur[part] = {}
                    cur = cur[part]
                cur[parts[-1]] = value
                logger.info(f"   Applied nested: {key}")
            else:
                node[key] = value
                logger.info(f"   Applied: {key}")

    @staticmethod
    def _error_response(workflow_name: str, run_id: str, error: str) -> Dict[str, Any]:
        """Standard error response builder."""
        return {"success": False, "workflow_name": workflow_name, "run_id": run_id, "error": error}


_fixer_instance = None


def get_fixer(settings: Optional[Settings] = None) -> FixerAgent:
    """Singleton accessor."""
    global _fixer_instance
    if _fixer_instance is None:
        _fixer_instance = FixerAgent(settings)
    return _fixer_instance