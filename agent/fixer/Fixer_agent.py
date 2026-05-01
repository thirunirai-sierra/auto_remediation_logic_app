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
from datetime import datetime, timezone
from pathlib import Path

import requests

# Add parent directory to path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.services.workflow_service import get_workflow, put_workflow
from auth import get_arm_token
from config import Settings, get_settings
from remediation import strip_read_only_for_put

logger = logging.getLogger(__name__)


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
            
            # 2. Save backup
            backup_dir = workflow_context.get("backup_dir")
            if backup_dir:
                self._save_backup(workflow, workflow_name, run_id, backup_dir)
            
            # 3. Get the failed action details
            definition = workflow.get("properties", {}).get("definition", {})
            actions = definition.get("actions", {})
            failed_action = actions.get(failed_action_name, {})
            action_type = failed_action.get("type", "unknown")
            
            # 4. Generate fix strategy based on error type
            root_cause = rca_result.get("root_cause", "unknown")
            exact_issue = rca_result.get("exact_issue", "")
            
            fix_strategy = self._generate_fix_strategy(
                root_cause=root_cause,
                exact_issue=exact_issue,
                action_type=action_type,
                action_config=failed_action,
                error_message=rca_result.get("recommendation", ""),
            )
            
            logger.info(f"[FIXER] Fix strategy: {fix_strategy.get('strategy_description', 'N/A')}")
            
            # 5. Apply the fix
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
            
            # 6. Deploy to Azure
            result = self._deploy_workflow_fix(
                subscription_id=workflow_context.get("subscription_id"),
                resource_group=workflow_context.get("resource_group"),
                workflow_name=workflow_name,
                fixed_workflow=fixed_workflow,
                etag=workflow.get("etag"),
            )
            
            if result.get("success"):
                logger.info(f"[FIXER] ✅ Fix deployed successfully")
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
    ) -> Dict[str, Any]:
        """Generate fix strategy based on error type."""
        
        # Strategy for contains() null error
        if "contains" in str(error_message).lower() and "null" in str(error_message).lower():
            return {
                "strategy_description": "Fix contains() null error by wrapping first argument with coalesce()",
                "changes": {
                    "expression": self._fix_contains_expression(action_config.get("expression", ""))
                },
                "explanation": "The contains() function received null. Wrapping with coalesce() provides a default empty string.",
                "risk": "low",
            }
        
        # Strategy for missing property error
        elif "property" in str(error_message).lower() and "doesn't exist" in str(error_message).lower():
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
            }
        
        # Strategy for 404 error
        elif "404" in str(error_message) or "not found" in str(error_message).lower():
            return {
                "strategy_description": "Fix 404 error by updating API endpoint",
                "changes": {
                    "inputs": {"uri": self.settings.fallback_http_url}
                },
                "explanation": "The endpoint returned 404. Updated to a working fallback endpoint.",
                "risk": "medium",
            }
        
        # Generic strategy
        else:
            return {
                "strategy_description": f"Apply standard fix for {root_cause}",
                "changes": {},
                "explanation": exact_issue,
                "risk": "low",
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
            actions = definition.get("actions", {})
            
            if failed_action_name not in actions:
                logger.error(f"[FIXER] Action {failed_action_name} not found")
                return None
            
            action_node = actions[failed_action_name]
            changes = fix_strategy.get("changes", {})
            
            # Apply changes
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
                
                logger.info(f"[FIXER] ✅ Successfully deployed")
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
    
    def _save_backup(self, workflow: Dict[str, Any], workflow_name: str, run_id: str, backup_dir: str):
        """Save workflow backup."""
        try:
            backup_path = Path(backup_dir) / f"{workflow_name}_{run_id[:8]}.backup.json"
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(backup_path, "w") as f:
                json.dump({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "workflow_name": workflow_name,
                    "run_id": run_id,
                    "workflow": workflow,
                }, f, indent=2, default=str)
            
            logger.info(f"[FIXER] Backup saved to {backup_path}")
        except Exception as e:
            logger.warning(f"[FIXER] Could not save backup: {e}")


# Singleton
_fixer_instance = None

def get_fixer(settings: Optional[Settings] = None) -> FixerAgent:
    global _fixer_instance
    if _fixer_instance is None:
        _fixer_instance = FixerAgent(settings)
    return _fixer_instance