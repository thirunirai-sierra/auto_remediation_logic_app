"""FastAPI app for Fixer Agent endpoints."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from agent.fixer import (
    apply_fix_from_rca,
    apply_fixed_definition,
    call_llm_for_fix,
    extract_failed_action_name,
    fetch_workflow_definition,
    get_rca_analysis,
)

app = FastAPI(
    title="Logic App Fixer Agent API",
    description="API to fetch workflow definitions and fix Logic App errors",
    version="1.0.0",
)


class RootCauseInput(BaseModel):
    resource_workflowName_s: str
    code_s: Optional[str] = None
    error_message_s: str
    error_location: Optional[str] = None
    root_cause: Optional[str] = None
    solution: Optional[str] = None
    status_s: Optional[str] = None
    Level: Optional[str] = None


class FixWorkflowResponse(BaseModel):
    workflow_name: str
    workflow_definition: Dict[str, Any]
    failed_action: str
    error_message: str
    rca_analysis: Dict[str, Any]
    root_cause: str
    recommendation_steps: list[str]
    fixed_definition: Dict[str, Any]


class ApplyFixInput(BaseModel):
    workflow_name: str
    fixed_definition: Dict[str, Any]


class ApplyFixResponse(BaseModel):
    workflow_name: str
    status: str
    message: str
    updated_workflow: Dict[str, Any]


@app.get("/")
def root() -> Dict[str, str]:
    return {"status": "ok", "message": "Fixer Agent API is running. Go to /docs for Swagger UI."}


@app.post("/fix-workflow", response_model=FixWorkflowResponse)
def fix_workflow_endpoint(input_data: RootCauseInput) -> FixWorkflowResponse:
    try:
        workflow_resource = fetch_workflow_definition(input_data.resource_workflowName_s)
        definition = workflow_resource.get("properties", {}).get("definition", {})

        # Prefer explicit RCA location from payload, then fallback to message parsing.
        failed_action = (
            (input_data.error_location or "").strip()
            or extract_failed_action_name(input_data.error_message_s)
        )
        if not failed_action:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Could not resolve failed action. Provide `error_location` in RCA payload "
                    "or include an actionable error_message_s."
                ),
            )

        # If RCA is already provided by upstream root-cause agent, use it directly.
        if input_data.root_cause:
            rca_result = {
                "error_code": input_data.code_s or "unknown",
                "error_location": failed_action,
                "root_cause": input_data.root_cause,
                "solution": input_data.solution or "",
            }
        else:
            rca_result = get_rca_analysis(
                error_code=input_data.code_s or "",
                error_message=input_data.error_message_s,
                failed_action_name=failed_action,
                workflow_name=input_data.resource_workflowName_s,
            )

        # Primary: rule-based fixer from RCA
        rule_fix = apply_fix_from_rca(definition, failed_action, rca_result)

        recommendation_steps: list[str]
        fixed_definition: Dict[str, Any]

        if rule_fix.get("applied"):
            recommendation_steps = [
                f"Rule-based fix applied: {rule_fix.get('fix_name')}",
                "Review the updated failed action configuration.",
                "Apply and validate in a test run.",
            ]
            fixed_definition = rule_fix["fixed_definition"]
        else:
            # Fallback only when rule-based fix is not applicable.
            llm_fix = call_llm_for_fix(
                error_message=input_data.error_message_s,
                failed_action_name=failed_action,
                workflow_definition=definition,
                rca_result=rca_result,
            )
            recommendation_steps = [str(x) for x in llm_fix.get("recommendation_steps", [])]
            fixed_definition = llm_fix["fixed_definition"]

        return FixWorkflowResponse(
            workflow_name=input_data.resource_workflowName_s,
            workflow_definition=definition,
            failed_action=failed_action,
            error_message=input_data.error_message_s,
            rca_analysis=rca_result,
            root_cause=str(rca_result.get("root_cause") or "unknown"),
            recommendation_steps=recommendation_steps,
            fixed_definition=fixed_definition,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/apply-fix", response_model=ApplyFixResponse)
def apply_fix_endpoint(input_data: ApplyFixInput) -> ApplyFixResponse:
    try:
        updated_workflow = apply_fixed_definition(
            workflow_name=input_data.workflow_name,
            fixed_definition=input_data.fixed_definition,
        )
        return ApplyFixResponse(
            workflow_name=input_data.workflow_name,
            status="success",
            message=f"Successfully applied fix to workflow '{input_data.workflow_name}'",
            updated_workflow=updated_workflow,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
