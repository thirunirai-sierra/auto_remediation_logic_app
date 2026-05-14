# server/services/agents/rca/engine.py
"""
Root Cause Analysis (RCA) engine powered by LLM and knowledge base enhancement.

This module provides asynchronous RCA generation for workflow failures by:
- Analyzing error context using an LLM
- Falling back to rule-based detection when AI fails
- Enhancing remediation guidance using semantic knowledge search
- Returning structured RCA metadata for observability pipelines

Features:
    - Async LLM execution with timeout protection
    - Rule-based fallback analysis
    - Knowledge base semantic enhancement
    - Structured JSON RCA responses
    - Confidence scoring
    - Workflow observability enrichment
"""
import asyncio
import logging
from typing import Any, Dict, Optional

from utils.llm_client import AICoreLLMClient
from services.agents.knowledge.knowledge_base import KnowledgeAgent
from config import Settings

logger = logging.getLogger(__name__)


async def generate_rca(
    failed_action: Dict[str, Any],
    error_context: Dict[str, Any],
    error_type: str,
    settings: Settings,
) -> Dict[str, Any]:
    """
    Generate root cause analysis (RCA) for a failed workflow action.

    The RCA process includes:
        1. LLM-based diagnostic analysis
        2. Rule-based fallback analysis if LLM fails
        3. Knowledge base enhancement using semantic search
        4. Structured remediation recommendations

    Args:
        failed_action (Dict[str, Any]):
            Metadata describing the failed workflow action.

        error_context (Dict[str, Any]):
            Detailed execution context including:
                - error_message
                - error_code
                - action_type
                - failed_action_name
                - action_inputs
                - workflow_name
                - status
                - level

        error_type (str):
            Normalized error classification category.

        settings (Settings):
            Application configuration instance.

    Returns:
        Dict[str, Any]:
            Structured RCA result containing:
                - root_cause (str)
                - exact_issue (str)
                - solution (str)
                - suggested_fix (str)
                - confidence (float)
                - workflow_name (str)
                - error_message_s (str)
                - code_s (str)
                - status_s (str)
                - Level (str)
                - knowledge_sources (optional)

    Notes:
        - LLM execution is protected by a 30-second timeout.
        - Rule-based analysis is automatically used if AI analysis fails.
        - Knowledge base enhancement is optional and non-blocking.
        - The returned dictionary is always guaranteed to contain
          a non-null suggested_fix field.
    """
    logger.info("=" * 80)
    logger.info("RCA: Starting analysis for error_type=%s", error_type)
    logger.info("=" * 80)

    system_prompt = (
        "You are an Azure Logic Apps error analyst. Analyze the error and provide diagnosis. "
        "Return ONLY valid JSON with keys: root_cause, exact_issue, solution, suggested_fix, confidence (0.0-1.0). "
        "'suggested_fix' must be a short actionable sentence (max 100 chars). "
        "Do not include markdown code blocks. Start with { and end with }."
    )

    user_prompt = f"""
Error type: {error_type}
Error message: {error_context.get('error_message', '')}
Error code: {error_context.get('error_code', '')}
Failed action type: {error_context.get('action_type', 'unknown')}
Failed action name: {error_context.get('failed_action_name', 'unknown')}
Action inputs preview: {str(error_context.get('action_inputs', ''))[:300]}
"""

    llm_client = AICoreLLMClient.from_env()
    llm_result = None

    try:
        logger.info("RCA: Calling LLM for analysis...")
        llm_result = await asyncio.wait_for(
            llm_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                required_keys=["root_cause", "exact_issue", "solution", "suggested_fix", "confidence"]
            ),
            timeout=30.0
        )
        if not llm_result or not isinstance(llm_result, dict):
            raise ValueError("LLM returned invalid result")
        logger.info("RCA: LLM analysis succeeded")
        logger.info("   root_cause: %s", llm_result.get("root_cause", "unknown"))
        logger.info("   suggested_fix: %s", llm_result.get("suggested_fix", "none"))
        logger.info("   confidence: %.2f", float(llm_result.get("confidence", 0.0)))
        logger.info("RCA full result: %s", llm_result)
    except asyncio.TimeoutError:
        logger.error("RCA: LLM call timed out after 30 seconds – using fallback")
        llm_result = None
    except Exception as e:
        logger.warning("RCA: LLM failed: %s – using fallback", str(e)[:150])
        llm_result = None

    # Fallback if LLM failed
    if llm_result is None:
        from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score
        error_msg = error_context.get("error_message", "")
        error_code = error_context.get("error_code", "")
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_context)
        confidence = confidence_score(root_cause, error_code, error_msg)
        # Generate suggested_fix based on root cause
        if root_cause == "not_found":
            suggested_fix = "Update the endpoint URL to a valid address."
        elif root_cause == "auth_or_authorization_error":
            suggested_fix = "Refresh authentication or check permissions."
        elif root_cause == "timeout":
            suggested_fix = "Increase timeout and add a retry policy."
        elif root_cause in ("payload_or_schema_error", "null_reference_error"):
            suggested_fix = "Add null/type checks and defaults to the expression."
        elif root_cause == "throttling":
            suggested_fix = "Implement exponential backoff retry."
        elif root_cause == "dns_resolution_error":
            suggested_fix = "Fix the hostname or DNS configuration."
        elif root_cause == "connection_refused":
            suggested_fix = "Check firewall and port accessibility."
        else:
            suggested_fix = "Review the action inputs and outputs manually."
        llm_result = {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "solution": f"Rule-based analysis: {exact_issue}",
            "suggested_fix": suggested_fix,
            "confidence": confidence,
        }
        logger.info("RCA: Using fallback - root_cause=%s, suggested_fix=%s, confidence=%.2f",
                    root_cause, suggested_fix, confidence)

    # Add extra context fields
    llm_result["workflow_name"] = error_context.get("workflow_name", "unknown")
    llm_result["error_message_s"] = error_context.get("error_message", "")
    llm_result["code_s"] = error_context.get("error_code", "")
    llm_result["status_s"] = error_context.get("status", "unknown")
    llm_result["Level"] = error_context.get("level", "Error")

    # Ensure suggested_fix is never None
    if llm_result.get("suggested_fix") is None:
        llm_result["suggested_fix"] = "No suggested fix available."

    # Knowledge base enhancement (optional, runs in thread)
    try:
        logger.info("RCA: Searching knowledge base...")
        knowledge = KnowledgeAgent(settings)
        query = f"{error_type} {error_context.get('error_message', '')} {error_context.get('error_code', '')}"
        similar_chunks = await asyncio.to_thread(knowledge.search, query, 3)
        if similar_chunks:
            logger.info("RCA: Found %d knowledge base chunks", len(similar_chunks))
            kb_text = "\n".join([f"[{i}] {chunk['text'][:200]}" for i, chunk in enumerate(similar_chunks, 1)])
            enhancement_prompt = f"""
Based on the following knowledge base content, improve the solution and suggested_fix:

{kb_text}

Current solution: {llm_result.get('solution', '')}
Current suggested_fix: {llm_result.get('suggested_fix', '')}

Return JSON with keys 'solution' and 'suggested_fix'.
"""
            try:
                enhanced = await asyncio.to_thread(
                    llm_client.complete_json,
                    system_prompt="You are a technical writer. Improve the solution and suggested fix using knowledge base. Return JSON with keys 'solution' and 'suggested_fix'.",
                    user_prompt=enhancement_prompt,
                )
                if enhanced and isinstance(enhanced, dict):
                    if "solution" in enhanced:
                        llm_result["solution"] = enhanced["solution"]
                    if "suggested_fix" in enhanced:
                        llm_result["suggested_fix"] = enhanced["suggested_fix"]
                        logger.info("RCA: Knowledge base enhancement applied to suggested_fix")
            except Exception as e:
                logger.warning("RCA: Knowledge enhancement failed: %s", str(e)[:100])
            llm_result["knowledge_sources"] = [chunk["meta"] for chunk in similar_chunks]
        else:
            logger.info("RCA: No knowledge base matches found")
    except Exception as e:
        logger.warning("RCA: Knowledge base search failed: %s", str(e)[:100])

    logger.info("=" * 80)
    logger.info("RCA: Complete - root_cause=%s, suggested_fix=%s, confidence=%.2f",
                llm_result.get("root_cause"), llm_result.get("suggested_fix"), float(llm_result.get("confidence", 0.0)))
    logger.info("=" * 80)
    return llm_result