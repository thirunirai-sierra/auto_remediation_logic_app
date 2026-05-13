# server/utils/llm_client.py
"""
SAP AI Core LLM Client.

Provides:
- Authentication for SAP AI Core
- Chat completion via LangChain ChatOpenAI proxy
- Robust JSON extraction from model output
- Retry mechanism with fallback non-JSON response
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Sequence

import requests
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI

logger = logging.getLogger(__name__)


class AICoreLLMClient:
    """
    Client for interacting with SAP AI Core LLM deployments.

    Features:
        - OAuth-based authentication
        - LangChain ChatOpenAI proxy integration
        - JSON response extraction with fallback parsing
        - Multi-attempt retry logic
        - Graceful degradation to simplified responses

    Threading:
        Uses asyncio.to_thread to prevent blocking event loop.
    """

    def __init__(
        self,
        *,
        auth_url: str,
        client_id: str,
        client_secret: str,
        base_url: str,
        resource_group: str,
        chat_deployment_id: str,
        timeout_seconds: int = 120,
    ) -> None:
        self.auth_url = auth_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self.resource_group = resource_group
        self.chat_deployment_id = chat_deployment_id
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "AICoreLLMClient":
        required = {
            "AICORE_AUTH_URL": os.getenv("AICORE_AUTH_URL"),
            "AICORE_CLIENT_ID": os.getenv("AICORE_CLIENT_ID"),
            "AICORE_CLIENT_SECRET": os.getenv("AICORE_CLIENT_SECRET"),
            "AICORE_BASE_URL": os.getenv("AICORE_BASE_URL"),
            "AICORE_RESOURCE_GROUP": os.getenv("AICORE_RESOURCE_GROUP"),
            "AICORE_CHAT_DEPLOYMENT_ID": os.getenv("AICORE_CHAT_DEPLOYMENT_ID"),
        }
        missing = [k for k, v in required.items() if not (v or "").strip()]
        if missing:
            raise ValueError(f"Missing AI Core env vars: {', '.join(missing)}")
        return cls(
            auth_url=required["AICORE_AUTH_URL"] or "",
            client_id=required["AICORE_CLIENT_ID"] or "",
            client_secret=required["AICORE_CLIENT_SECRET"] or "",
            base_url=required["AICORE_BASE_URL"] or "",
            resource_group=required["AICORE_RESOURCE_GROUP"] or "",
            chat_deployment_id=required["AICORE_CHAT_DEPLOYMENT_ID"] or "",
        )

    def _token(self) -> str:
        """
    Retrieves OAuth access token from SAP AI Core.

    Returns:
        str: Bearer access token.

    Raises:
        ValueError: If token is missing in response.
        requests.HTTPError: If authentication request fails.
    """
        resp = requests.post(
            self.auth_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise ValueError("AI Core auth did not return access_token")
        return str(token)

    def _get_proxy_llm(self, temperature: float = 0.0) -> ChatOpenAI:
        """
    Creates a LangChain ChatOpenAI proxy instance.

    Args:
        temperature: Sampling temperature for LLM output.

    Returns:
        ChatOpenAI: Configured LLM instance.
    """
        return ChatOpenAI(deployment_id=self.chat_deployment_id, temperature=temperature)

    def _extract_json_from_text(self, text: str) -> Optional[Dict[str, Any]]:
        """
    Extracts JSON object from raw LLM response text.

    Attempts:
        1. Direct JSON parsing
        2. Regex extraction of {...}
        3. Cleanup of markdown code blocks

    Args:
        text: Raw LLM output string.

    Returns:
        Parsed JSON dict if successful, otherwise None.
    """
        if not text:
            return None
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        cleaned = re.sub(r'```(?:json)?\s*\n?', '', text)
        match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        return None

    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        required_keys: Optional[Sequence[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
    Executes LLM request and ensures structured JSON output.

    Process:
        1. Try up to 3 full LLM attempts
        2. Parse and validate JSON response
        3. Retry if parsing fails or keys are missing
        4. Fallback to simplified non-JSON response

    Args:
        system_prompt: System-level instructions for the model.
        user_prompt: User query/input prompt.
        required_keys: Optional list of required JSON keys.

    Returns:
        Dict containing parsed JSON response or fallback structure.
        Returns None if all attempts fail.
    """
        enhanced_system = (
            system_prompt.strip()
            + "\n\n"
            + "You must respond with ONLY a valid JSON object. Do not include any other text, "
            "markdown, explanations, or code fences. Start with '{' and end with '}'.\n\n"
            "Example response format:\n"
            '{"root_cause": "string", "exact_issue": "string", "solution": "string", "confidence": 0.95}'
        )

        for attempt in range(3):
            try:
                # Run the synchronous LangChain call in a thread to avoid blocking
                llm = self._get_proxy_llm(temperature=0.0)
                response = await asyncio.to_thread(
                    llm.invoke,
                    [
                        {"role": "system", "content": enhanced_system},
                        {"role": "user", "content": user_prompt},
                    ]
                )
                content = str(getattr(response, "content", "") or "").strip()
                # Log raw response at INFO level for debugging
                logger.info("LLM raw response (attempt %d, len=%d): %s", attempt+1, len(content), content[:500])

                parsed = self._extract_json_from_text(content)
                if parsed is None:
                    logger.warning("No JSON found in response, retrying")
                    continue

                if required_keys and not all(k in parsed for k in required_keys):
                    missing = set(required_keys) - set(parsed.keys())
                    logger.warning("Missing required keys: %s – retrying", missing)
                    continue

                logger.info("JSON parsing succeeded (attempt %d)", attempt+1)
                return parsed

            except Exception as e:
                logger.warning("Attempt %d failed: %s", attempt+1, str(e)[:200])

        # Fallback simplified (last resort) – also run in thread
        logger.warning("Stage 1 failed, falling back to simplified prompt")
        simple_prompt = f"Answer in one short sentence: {user_prompt}"
        for attempt in range(2):
            try:
                llm = self._get_proxy_llm(temperature=0.0)
                response = await asyncio.to_thread(
                    llm.invoke,
                    [
                        {"role": "system", "content": "You are a helpful assistant. Answer concisely."},
                        {"role": "user", "content": simple_prompt},
                    ]
                )
                content = str(getattr(response, "content", "") or "").strip()
                logger.info("Simplified response: %s", content[:200])
                fallback = {
                    "root_cause": content[:200],
                    "exact_issue": content[:200],
                    "solution": content[:200],
                    "confidence": 0.5,
                }
                if required_keys and not all(k in fallback for k in required_keys):
                    continue
                return fallback
            except Exception as e:
                logger.warning("Simplified attempt %d failed: %s", attempt+1, str(e)[:200])

        logger.error("All attempts failed")
        return None