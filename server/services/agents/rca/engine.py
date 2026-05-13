# server/services/agents/rca/engine.py
"""
RCA engine: root cause analysis using LLM with knowledge base enhancement.
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
    Generate root cause analysis (RCA) for a failed Logic App action.

    This function uses an LLM client to produce a JSON-based RCA, including 
    root cause, exact issue, solution, and confidence score. Optionally, it 
    enhances the solution using a knowledge base search.

    Args:
        failed_action (Dict[str, Any]): The failed action object from the workflow.
        error_context (Dict[str, Any]): Dictionary containing error details, e.g.,
            - error_message
            - error_code
            - action_type
            - failed_action_name
            - action_inputs
            - workflow_name
            - status
            - level
        error_type (str): High-level classification of the error, e.g., "404", "timeout".
        settings (Settings): Configuration object providing environment settings.

    Returns:
        Dict[str, Any]: RCA result containing:
            - root_cause (str): The diagnosed root cause.
            - exact_issue (str): Description of the exact issue.
            - solution (str): Recommended solution text.
            - confidence (float): Confidence score (0.0-1.0).
            - workflow_name (str): Name of the workflow.
            - error_message_s (str): Original error message.
            - code_s (str): Error code.
            - status_s (str): Action status.
            - Level (str): Severity level.
            - knowledge_sources (Optional[List[Dict]]): Metadata of KB chunks used for enhancement.
    """
    logger.info("=" * 80)
    logger.info("RCA: Starting analysis for error_type=%s", error_type)
    logger.info("=" * 80)

    system_prompt = (
        "You are an Azure Logic Apps error analyst. Analyze the error and provide diagnosis. "
        "Return ONLY valid JSON with keys: root_cause, exact_issue, solution, confidence (0.0-1.0). "
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
        llm_result = await llm_client.complete_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            required_keys=["root_cause", "exact_issue", "solution", "confidence"]
        )
        if not llm_result or not isinstance(llm_result, dict):
            raise ValueError("LLM returned invalid result")
        logger.info("RCA: LLM analysis succeeded")
        logger.info("   root_cause: %s", llm_result.get("root_cause", "unknown"))
        logger.info("   confidence: %.2f", float(llm_result.get("confidence", 0.0)))
        logger.info("RCA full result: %s", llm_result)

    except Exception as e:
        logger.warning("RCA: LLM failed: %s – using fallback", str(e)[:150])
        from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score
        error_msg = error_context.get("error_message", "")
        error_code = error_context.get("error_code", "")
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_context)
        confidence = confidence_score(root_cause, error_code, error_msg)
        llm_result = {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "solution": f"Rule-based analysis: {exact_issue}",
            "confidence": confidence,
        }
        logger.info("RCA: Using fallback - root_cause=%s, confidence=%.2f", root_cause, confidence)

    # Add extra context fields to the result
    llm_result["workflow_name"] = error_context.get("workflow_name", "unknown")
    llm_result["error_message_s"] = error_context.get("error_message", "")
    llm_result["code_s"] = error_context.get("error_code", "")
    llm_result["status_s"] = error_context.get("status", "unknown")
    llm_result["Level"] = error_context.get("level", "Error")

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
Based on the following knowledge base content, improve the solution quality:

{kb_text}

Current solution: {llm_result.get('solution', '')}

Return JSON with key 'solution' containing improved solution text.
"""
            try:
                enhanced = await llm_client.complete_json(
                    system_prompt="You are a technical writer. Improve the solution using knowledge base. Return JSON with key 'solution'.",
                    user_prompt=enhancement_prompt,
                )
                if enhanced and isinstance(enhanced, dict) and "solution" in enhanced:
                    llm_result["solution"] = enhanced["solution"]
                    logger.info("RCA: Knowledge base enhancement applied")
            except Exception as e:
                logger.warning("RCA: Knowledge enhancement failed: %s", str(e)[:100])
            llm_result["knowledge_sources"] = [chunk["meta"] for chunk in similar_chunks]
        else:
            logger.info("RCA: No knowledge base matches found")
    except Exception as e:
        logger.warning("RCA: Knowledge base search failed: %s", str(e)[:100])

    logger.info("=" * 80)
    logger.info("RCA: Complete - root_cause=%s, confidence=%.2f", llm_result.get("root_cause"), float(llm_result.get("confidence", 0.0)))
    logger.info("=" * 80)
    return llm_result