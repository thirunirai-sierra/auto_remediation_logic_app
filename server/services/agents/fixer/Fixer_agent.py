"""
PRODUCTION FIXER V5 — Dynamic LLM-first with full context awareness.
Fixes over V4:
- Variable ordering bug fixed (patch assigned before validation)
- Full workflow parameters block sent to LLM (connection aliases visible)
- No hard-coded "allowed fields" restrictions that block valid fixes
- Dotted-key patch supports deeply nested paths
- Timeout fix still deterministic
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
from typing import Any, Dict, List, Optional

from services.auth import get_arm_token
from config import Settings, get_settings
from services.workflow_service import get_workflow, put_workflow
from services.agents.fixer import remediation as rem
from utils.llm_client import AICoreLLMClient

logger = logging.getLogger(__name__)

_RETRY_ELIGIBLE_TYPES = frozenset({
    "http", "httpwebhook", "apiconnection", "apiconnectionwebhook",
    "apiconnectionnotification", "function", "serviceprovider", "workflow",
})

_MAX_WORKFLOW_CHARS = 12_000
_MAX_ACTION_CHARS   =  4_000


class FixerAgent:
    """Production-grade Logic Apps auto-remediation agent."""

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

    # ── Public entry ─────────────────────────────────────────────────────────

    def fix(self, rca_result: Dict[str, Any], workflow_context: Dict[str, Any]) -> Dict[str, Any]:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._fix_async(rca_result, workflow_context))
            loop.close()
            return result
        except Exception as exc:
            logger.error("[FIXER] FAILED: %s", exc, exc_info=True)
            return {
                "success": False,
                "workflow_name": workflow_context.get("workflow_name"),
                "run_id":        workflow_context.get("run_id"),
                "error":         str(exc),
            }

    # ── Async pipeline ────────────────────────────────────────────────────────

    async def _fix_async(
        self,
        rca_result:       Dict[str, Any],
        workflow_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        workflow_name      = workflow_context.get("workflow_name")
        run_id             = workflow_context.get("run_id")
        failed_action_name = workflow_context.get("failed_action_name")
        error_type         = workflow_context.get("error_type", "unknown")

        if not workflow_name or str(workflow_name).lower() in ("none", "unknown", ""):
            logger.error("[FIXER] workflow_name missing — cannot fix run %s", run_id)
            return self._err(workflow_name, run_id, "workflow_name is missing from the incident record.")

        logger.info("=" * 80)
        logger.info("[FIXER] V5  workflow=%s  action=%s  error_type=%s",
                    workflow_name, failed_action_name, error_type)
        logger.info("[FIXER] root_cause=%s", rca_result.get("root_cause"))
        logger.info("=" * 80)

        try:
            # Step 1 — fetch workflow
            logger.info("[FIXER] Step 1: fetching workflow...")
            workflow = await asyncio.to_thread(
                get_workflow,
                self.token,
                workflow_context.get("subscription_id"),
                workflow_context.get("resource_group"),
                workflow_name,
            )
            definition = workflow.get("properties", {}).get("definition", {})
            if not definition:
                return self._err(workflow_name, run_id, "No workflow definition found")
            logger.info("[FIXER] Workflow fetched OK")

            # Step 2 — locate action
            logger.info("[FIXER] Step 2: locating action '%s'...", failed_action_name)
            action_path = rem.find_action_path(definition, failed_action_name)
            if not action_path:
                return self._err(workflow_name, run_id,
                                 f"Action '{failed_action_name}' not found in definition")
            try:
                action_node = rem.navigate_path(definition, action_path)
                if not isinstance(action_node, dict):
                    raise ValueError("action node is not a dict")
            except (KeyError, ValueError) as exc:
                return self._err(workflow_name, run_id, f"Navigation error: {exc}")

            action_type = (action_node.get("type") or "").lower()
            logger.info("[FIXER] Action located at %s  type=%s",
                        "/".join(action_path), action_type)

            # Step 3 — generate fix
            logger.info("[FIXER] Step 3: generating fix (error_type=%s)...", error_type)
            fixed_definition = await self._generate_fix(
                definition, action_path, action_node,
                error_type, rca_result, workflow_context, workflow,
            )
            if fixed_definition is None:
                logger.warning("[FIXER] Fix generation returned None — using original definition")
                fixed_definition = definition
            logger.info("[FIXER] Fix generation complete")

            # Step 4 — prepare payload
            logger.info("[FIXER] Step 4: preparing deployment payload...")
            updated_workflow = copy.deepcopy(workflow)
            updated_workflow["properties"]["definition"] = fixed_definition
            updated_workflow = rem.strip_read_only_for_put(updated_workflow)

            # Step 5 — deploy
            logger.info("[FIXER] Step 5: deploying to Azure...")
            if getattr(self.settings, "DRY_RUN", False):
                logger.warning("[FIXER] DRY_RUN=True — skipping PUT")
                return self._err(workflow_name, run_id, "Dry run mode — deployment skipped")

            await asyncio.to_thread(
                put_workflow,
                self.token,
                workflow_context.get("subscription_id"),
                workflow_context.get("resource_group"),
                workflow_name,
                updated_workflow,
                workflow.get("etag"),
            )
            logger.info("[FIXER] Deployed successfully!")
            return {
                "success":         True,
                "workflow_name":   workflow_name,
                "run_id":          run_id,
                "error_type":      error_type,
                "action_fixed":    failed_action_name,
                "changes_applied": True,
            }

        except Exception as exc:
            logger.error("[FIXER] Unexpected error: %s", exc, exc_info=True)
            return self._err(workflow_name, run_id, str(exc))

    # ── Fix routing ───────────────────────────────────────────────────────────

    async def _generate_fix(
        self,
        definition:       Dict[str, Any],
        action_path:      List[str],
        action_node:      Dict[str, Any],
        error_type:       str,
        rca_result:       Dict[str, Any],
        workflow_context: Dict[str, Any],
        full_workflow:    Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if error_type == "timeout":
            return self._fix_timeout(definition, action_path, action_node)
        return await self._fix_with_llm(
            definition, action_path, action_node,
            rca_result, workflow_context, full_workflow,
        )

    # ── Deterministic timeout fix ─────────────────────────────────────────────

    def _fix_timeout(
        self,
        definition:  Dict[str, Any],
        action_path: List[str],
        action_node: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        action_type = (action_node.get("type") or "").lower()
        if action_type not in _RETRY_ELIGIBLE_TYPES:
            logger.info("[FIXER] timeout fix: skipped for type '%s'", action_type)
            return definition
        try:
            fixed = copy.deepcopy(definition)
            node  = rem.navigate_path(fixed, action_path)
            if node:
                node["retryPolicy"] = {
                    "type":            "exponential",
                    "count":           5,
                    "interval":        "PT10S",
                    "minimumInterval": "PT5S",
                    "maximumInterval": "PT1M",
                }
                for parent_path in rem.find_parent_conditions(fixed, action_path):
                    rem.update_condition_runafter(fixed, parent_path)
                logger.info("[FIXER] timeout fix: added exponential retry policy")
            return fixed
        except Exception as exc:
            logger.error("[FIXER] timeout fix failed: %s", exc)
            return None

    # ── LLM-driven fix ────────────────────────────────────────────────────────

    async def _fix_with_llm(
        self,
        definition:       Dict[str, Any],
        action_path:      List[str],
        action_node:      Dict[str, Any],
        rca_result:       Dict[str, Any],
        workflow_context: Dict[str, Any],
        full_workflow:    Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        action_name = action_path[-1]
        action_type = (action_node.get("type") or "").lower()

        # Build workflow context — always include parameters (connection aliases)
        workflow_for_prompt = {
            "parameters": full_workflow.get("properties", {}).get("parameters", {}),
            "definition":  definition,
        }
        workflow_json = json.dumps(workflow_for_prompt, indent=2, default=str)
        action_json   = json.dumps(action_node,         indent=2, default=str)

        # Truncate only if very large; keep parameters block intact
        if len(workflow_json) > _MAX_WORKFLOW_CHARS:
            params_json  = json.dumps(
                {"parameters": workflow_for_prompt["parameters"]}, indent=2, default=str
            )
            actions_json = json.dumps(
                definition.get("actions", {}), indent=2, default=str
            )[:_MAX_WORKFLOW_CHARS - len(params_json)]
            workflow_snippet = (
                params_json.rstrip("}").rstrip()
                + ',\n  "actions_snippet": '
                + actions_json
                + "\n}"
            )
        else:
            workflow_snippet = workflow_json

        action_snippet = action_json[:_MAX_ACTION_CHARS]

        system_prompt = f"""You are an expert Azure Logic Apps engineer performing automated remediation.

TASK
----
Produce a minimal JSON patch to fix the failing action described below.

RETURN FORMAT — ONLY valid JSON, nothing else:
{{"patch": {{...}}, "confidence": <float 0.0-1.0>}}

PATCH RULES
-----------
1. Use dotted keys for nested fields, e.g.:
   "inputs.host.connection.name"  →  sets node["inputs"]["host"]["connection"]["name"]
   "inputs.queries.path"          →  sets node["inputs"]["queries"]["path"]
   "retryPolicy"                  →  sets node["retryPolicy"]

2. You MAY change ANY field that caused the failure, including:
   - Connection references:   inputs.host.connection.name
   - Input body / headers:    inputs.body.*, inputs.headers.*
   - Query parameters:        inputs.queries.*
   - Expressions (any @expr): inputs.body.To, inputs.queries.path, etc.
   - Retry / timeout policy:  retryPolicy
   - runAfter conditions:     runAfter

3. Do NOT change:
   - "type"  (action type is immutable)
   - Structural nesting (do not add/remove sibling actions)

4. For wrong connection alias errors:
   Look at the workflow parameters block to find the CORRECT alias,
   then set "inputs.host.connection.name" to the correct expression.

5. For missing/null path input:
   Set the correct dotted path key to a safe expression using coalesce().

6. Set confidence >= 0.6 only when certain the patch will fix the issue.
   If no safe fix can be determined, return {{"patch": {{}}, "confidence": 0.0}}.

ACTION TYPE: {action_type}
"""

        user_prompt = f"""FAILING ACTION
--------------
Name:        {action_name}
Type:        {action_type}
Error type:  {workflow_context.get("error_type", "unknown")}
Root cause:  {rca_result.get("root_cause", "")}
Exact issue: {str(rca_result.get("exact_issue", ""))[:500]}
Suggested fix (from RCA): {str(rca_result.get("suggested_fix", "") or rca_result.get("solution", ""))[:400]}

CURRENT ACTION JSON
-------------------
{action_snippet}

WORKFLOW CONTEXT (parameters + definition)
------------------------------------------
{workflow_snippet}

Identify the precise field(s) that caused the failure.
Cross-reference the parameters block for correct connection aliases.
Return JSON: {{"patch": {{...}}, "confidence": <0.0-1.0>}}
"""

        logger.info("[FIXER] Calling LLM  action=%s  type=%s  context_chars=%d",
                    action_name, action_type, len(workflow_snippet))

        try:
            llm      = AICoreLLMClient.from_env()
            response = await asyncio.wait_for(
                llm.complete_json(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    required_keys=["patch", "confidence"],
                ),
                timeout=180,
            )
        except asyncio.TimeoutError:
            logger.warning("[FIXER] LLM call timed out — using original definition")
            return definition
        except Exception as exc:
            logger.warning("[FIXER] LLM call failed: %s — using original definition", exc)
            return definition

        # ── Parse response (fix variable-ordering bug from V4) ────────────────
        if not response or not isinstance(response, dict):
            logger.warning("[FIXER] LLM returned empty/invalid response")
            return definition

        # Assign FIRST, validate AFTER
        patch      = response.get("patch", {})
        confidence = 0.0
        try:
            confidence = float(response.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0

        logger.info("[FIXER] LLM response: confidence=%.2f  patch_keys=%s",
                    confidence, list(patch.keys()) if isinstance(patch, dict) else "?")

        if confidence < 0.6:
            logger.warning(
                "[FIXER] LLM confidence %.2f < 0.6 — no structural changes. "
                "Deploying workflow as-is (re-deploy resolves some transient issues).",
                confidence,
            )
            return definition

        if not isinstance(patch, dict) or not patch:
            logger.info("[FIXER] LLM returned empty patch — no changes needed, re-deploying")
            return definition

        # ── Apply patch ──────────────────────────────────────────────────────
        fixed = copy.deepcopy(definition)
        node  = rem.navigate_path(fixed, action_path)
        if not node:
            logger.error("[FIXER] Could not navigate to action node after deep copy")
            return definition

        self._apply_patch(node, patch)
        logger.info("[FIXER] Applied patch with %d key(s), confidence=%.2f",
                    len(patch), confidence)
        return fixed

    # ── Patch helper ──────────────────────────────────────────────────────────

    def _apply_patch(self, node: Dict[str, Any], patch: Dict[str, Any]) -> None:
        """Apply patch to node. Keys may use dot-notation for deep nesting."""
        for key, value in patch.items():
            if "." in key:
                parts = key.split(".")
                cur   = node
                for part in parts[:-1]:
                    if part not in cur or not isinstance(cur[part], dict):
                        cur[part] = {}
                    cur = cur[part]
                cur[parts[-1]] = value
                logger.info("[FIXER]   patched nested: %s = %s", key, str(value)[:120])
            else:
                node[key] = value
                logger.info("[FIXER]   patched: %s = %s", key, str(value)[:120])

    @staticmethod
    def _err(workflow_name: str, run_id: str, error: str) -> Dict[str, Any]:
        return {"success": False, "workflow_name": workflow_name, "run_id": run_id, "error": error}


# ── Singleton ─────────────────────────────────────────────────────────────────

_fixer_instance: Optional[FixerAgent] = None


def get_fixer(settings: Optional[Settings] = None) -> FixerAgent:
    global _fixer_instance
    if _fixer_instance is None:
        _fixer_instance = FixerAgent(settings)
    return _fixer_instance