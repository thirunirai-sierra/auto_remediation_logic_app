"""
Root Cause Analysis (RCA) engine powered by LLM and knowledge base enhancement.
With comprehensive logging of diagnosis and suggested fix.
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
    Generate root cause analysis with comprehensive logging.
    Logs: Input -> LLM Call -> LLM Response -> Fallback -> KB Enhancement -> Final Output.
    """
    logger.info("=" * 80)
    logger.info("RCA AGENT - Root Cause Analysis")
    logger.info("=" * 80)
    
    # ============================================================
    # STEP 0: INPUT LOGGING
    # ============================================================
    logger.info("RCA INPUT:")
    logger.info(f"   Error Type: {error_type}")
    logger.info(f"   Error Message: {error_context.get('error_message', '')[:300]}...")
    logger.info(f"   Error Code: {error_context.get('error_code', '')}")
    logger.info(f"   Failed Action: {error_context.get('failed_action_name', '')}")
    logger.info(f"   Action Type: {error_context.get('action_type', '')}")
    logger.info("")

    system_prompt = (
        "You are an Azure Logic Apps error analyst expert.\n"
        "Analyze the error and provide a structured diagnosis.\n"
        "Return ONLY valid JSON with these keys:\n"
        "- root_cause: short label (max 50 chars)\n"
        "- exact_issue: what went wrong (1-2 sentences)\n"
        "- solution: how to solve it (detailed, max 200 chars)\n"
        "- suggested_fix: quick actionable step (1 sentence, max 100 chars)\n"
        "- confidence: 0.0-1.0 confidence score\n\n"
        "Start with { and end with }. No markdown, no code blocks."
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

    # ============================================================
    # STEP 1: LLM ANALYSIS
    # ============================================================
    logger.info("STEP 1: LLM Analysis")
    logger.info("-" * 80)
    logger.info("Sending request to SAP AI Core LLM...")
    logger.info(f"   System Prompt: {system_prompt[:100]}...")
    logger.info(f"   User Prompt: {user_prompt[:200]}...")
    logger.info("")

    try:
        llm_result = await asyncio.wait_for(
            llm_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                required_keys=["root_cause", "exact_issue", "solution", "suggested_fix", "confidence"]
            ),
            timeout=100.0
        )

        if llm_result and isinstance(llm_result, dict):
            logger.info("LLM Call Succeeded")
            logger.info("RCA LLM RESPONSE:")
            logger.info(f"   DIAGNOSED ROOT CAUSE: {llm_result.get('root_cause', 'N/A')}")
            logger.info(f"   EXACT ISSUE: {llm_result.get('exact_issue', 'N/A')[:200]}...")
            logger.info(f"   SUGGESTED FIX: {llm_result.get('suggested_fix', 'N/A')}")
            logger.info(f"   CONFIDENCE: {float(llm_result.get('confidence', 0.0))}")
            logger.info(f"   SOLUTION: {llm_result.get('solution', 'N/A')[:200]}...")
            logger.info("")
        else:
            logger.warning("LLM returned invalid format")
            llm_result = None

    except asyncio.TimeoutError:
        logger.error("LLM call timed out (45 seconds)")
        llm_result = None
    except Exception as e:
        logger.error(f"LLM call failed: {str(e)[:200]}")
        llm_result = None

    # ============================================================
    # STEP 2: FALLBACK (Rule-Based Analysis)
    # ============================================================
    if llm_result is None:
        logger.info("STEP 2: No LLM result – returning generic analysis")
        llm_result = {
            "root_cause": "unknown",
            "exact_issue": "Could not determine root cause automatically.",
            "solution": "Review the action inputs and outputs manually. Enable debug logging.",
            "suggested_fix": "Manual inspection required.",
            "confidence": 0.0,
        }

        from utils.error_detector import infer_root_cause, extract_exact_issue, confidence_score

        error_msg = error_context.get("error_message", "")
        error_code = error_context.get("error_code", "")
        root_cause = infer_root_cause(error_code, error_msg)
        exact_issue = extract_exact_issue(error_msg, root_cause, error_context)
        confidence = confidence_score(root_cause, error_code, error_msg)

        logger.info(f"   Inferred Root Cause: {root_cause}")
        logger.info(f"   Exact Issue: {exact_issue}")
        logger.info(f"   Confidence Score: {confidence}")
        logger.info("")

        # Map root causes to solutions
        solution_map = {
            "null_reference_error": "Use coalesce() to provide default values for null references. Guard expressions with empty() checks.",
            "payload_or_schema_error": "Align request payload with API contract. Ensure all required fields are present and valid.",
            "timeout": "Increase timeout duration and add a fixed retry policy (3-4 retries with PT30S interval).",
            "auth_or_authorization_error": "Refresh authentication token or connection secret. Verify service principal has RBAC permissions.",
            "dns_resolution_error": "Fix the hostname or update DNS resolution path. Check private endpoint configuration.",
            "connection_refused": "Verify service is running and accessible. Check firewall rules and network security groups.",
            "not_found": "Validate resource ID, API path, and version. Ensure resource exists in correct subscription/resource group.",
            "throttling": "Implement exponential backoff retry policy. Honor Retry-After headers from API.",
            "unknown": "Inspect action inputs/outputs. Enable debug telemetry for detailed diagnostics.",
        }

        # Map to suggested fixes
        fix_map = {
            "null_reference_error": "Add coalesce() guard to null-prone expressions.",
            "payload_or_schema_error": "Add missing required fields to request.",
            "timeout": "Add retry policy with 3 attempts and PT30S interval.",
            "auth_or_authorization_error": "Refresh token and verify RBAC permissions.",
            "dns_resolution_error": "Fix hostname in endpoint URL.",
            "connection_refused": "Verify firewall/NSG rules allow connection.",
            "not_found": "Validate resource ID and API endpoint.",
            "throttling": "Implement exponential backoff (max 6 retries).",
            "unknown": "Manual review required.",
        }

        solution = solution_map.get(root_cause, solution_map["unknown"])
        suggested_fix = fix_map.get(root_cause, fix_map["unknown"])

        llm_result = {
            "root_cause": root_cause,
            "exact_issue": exact_issue,
            "solution": solution,
            "suggested_fix": suggested_fix,
            "confidence": confidence,
        }

        logger.info("Fallback Analysis Complete")
        logger.info("FALLBACK RCA RESULT:")
        logger.info(f"   DIAGNOSED ROOT CAUSE: {root_cause}")
        logger.info(f"   EXACT ISSUE: {exact_issue[:200]}...")
        logger.info(f"   SUGGESTED FIX: {suggested_fix}")
        logger.info(f"   CONFIDENCE: {confidence} (rule-based)")
        logger.info("")

    # Add extra context fields
    llm_result["workflow_name"] = error_context.get("workflow_name", "unknown")
    llm_result["error_message_s"] = error_context.get("error_message", "")
    llm_result["code_s"] = error_context.get("error_code", "")
    llm_result["status_s"] = error_context.get("status", "unknown")
    llm_result["Level"] = error_context.get("level", "Error")

    # Ensure suggested_fix is never None
    if llm_result.get("suggested_fix") is None:
        llm_result["suggested_fix"] = "No suggested fix available. Manual review required."

    # ============================================================
    # STEP 3: KNOWLEDGE BASE ENHANCEMENT (Optional)
    # ============================================================
    logger.info("STEP 3: Knowledge Base Enhancement")
    logger.info("-" * 80)

    try:
        logger.info("   Searching HANA knowledge base for similar issues...")
        knowledge = KnowledgeAgent(settings)
        query = f"{error_type} {error_context.get('error_message', '')} {error_context.get('error_code', '')}"
        
        similar_chunks = await asyncio.to_thread(knowledge.search, query, 3)

        if similar_chunks:
            logger.info(f"Found {len(similar_chunks)} similar issues in knowledge base")
            for i, chunk in enumerate(similar_chunks, 1):
                similarity = chunk.get('similarity', 0)
                logger.info(f"   Chunk {i}: similarity={similarity:.2f}")
                # Safely get meta title
                meta = chunk.get('meta', {})
                title = meta.get('title', 'Unknown') if isinstance(meta, dict) else 'Unknown'
                logger.info(f"      Title: {title[:100]}")
            logger.info("")

            # Store knowledge sources in result
            llm_result["knowledge_sources"] = [
                {
                    "similarity": chunk.get('similarity', 0),
                    "title": chunk.get('meta', {}).get('title', 'Unknown') if isinstance(chunk.get('meta'), dict) else 'Unknown',
                    "url": chunk.get('meta', {}).get('url', '') if isinstance(chunk.get('meta'), dict) else '',
                }
                for chunk in similar_chunks
            ]
        else:
            logger.info("No similar issues found in knowledge base")
            logger.info("")

    except Exception as e:
        logger.warning(f"Knowledge base search failed: {str(e)[:150]}")
        logger.info("")

    # ============================================================
    # FINAL OUTPUT
    # ============================================================
    logger.info("=" * 80)
    logger.info("RCA FINAL OUTPUT:")
    logger.info("-" * 80)
    logger.info(f"DIAGNOSED ROOT CAUSE: {llm_result.get('root_cause', 'unknown')}")
    logger.info(f"EXACT ISSUE: {llm_result.get('exact_issue', 'unknown')[:250]}")
    logger.info(f"SOLUTION: {llm_result.get('solution', 'unknown')[:250]}")
    logger.info(f"SUGGESTED FIX: {llm_result.get('suggested_fix', 'none')}")
    logger.info(f"CONFIDENCE SCORE: {float(llm_result.get('confidence', 0.0))}")
    logger.info("=" * 80)
    
    return llm_result