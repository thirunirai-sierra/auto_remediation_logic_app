# server/utils/llm_client.py
"""
SAP AI Core LLM Client – correct URL without double /v2.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Sequence

import httpx

logger = logging.getLogger(__name__)


class SAPAICoreLLMClient:
    """
    Asynchronous client for SAP AI Core's OpenAI‑compatible chat completion API.

    Handles authentication (OAuth2 client credentials), constructs the correct
    URL with api‑version, and provides methods for JSON‑structured or plain‑text
    responses.
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
        model_name: str = "gpt-5",
        timeout_seconds: int = 100,
        api_version: str = "2023-05-15",
    ) -> None:
        """
        Initialise the SAP AI Core LLM client.

        Args:
            auth_url: OAuth2 token endpoint (e.g., SAP AI Core authentication URL).
            client_id: OAuth2 client ID.
            client_secret: OAuth2 client secret.
            base_url: Base URL of the SAP AI Core API (without trailing /v2).
            resource_group: AI resource group (used in headers).
            chat_deployment_id: Deployment ID for the chat model.
            model_name: Model name passed in the payload (default "gpt-5").
            timeout_seconds: Request timeout in seconds.
            api_version: API version query parameter (default "2023-05-15").
        """
        self.auth_url = auth_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        # Remove any trailing /v2 from base_url
        base_url = base_url.rstrip("/")
        if base_url.endswith("/v2"):
            base_url = base_url[:-3]
        self.base_url = base_url
        self.resource_group = resource_group
        self.chat_deployment_id = chat_deployment_id
        self.model_name = model_name
        self.timeout_seconds = timeout_seconds
        self.api_version = api_version
        self._token: Optional[str] = None

    @classmethod
    def from_env(cls) -> "SAPAICoreLLMClient":
        """
        Create a client instance using environment variables.

        Required env vars:
            AICORE_AUTH_URL, AICORE_CLIENT_ID, AICORE_CLIENT_SECRET,
            AICORE_BASE_URL, AICORE_RESOURCE_GROUP, AICORE_CHAT_DEPLOYMENT_ID

        Optional env vars:
            AICORE_MODEL_NAME (default "gpt-5")
            AICORE_API_VERSION (default "2023-05-15")

        Returns:
            SAPAICoreLLMClient: Configured client instance.

        Raises:
            ValueError: If any required environment variable is missing.
        """
        required = {
            "AICORE_AUTH_URL": os.getenv("AICORE_AUTH_URL"),
            "AICORE_CLIENT_ID": os.getenv("AICORE_CLIENT_ID"),
            "AICORE_CLIENT_SECRET": os.getenv("AICORE_CLIENT_SECRET"),
            "AICORE_BASE_URL": os.getenv("AICORE_BASE_URL"),
            "AICORE_RESOURCE_GROUP": os.getenv("AICORE_RESOURCE_GROUP"),
            "AICORE_CHAT_DEPLOYMENT_ID": os.getenv("AICORE_CHAT_DEPLOYMENT_ID"),
        }
        optional = {
            "AICORE_MODEL_NAME": os.getenv("AICORE_MODEL_NAME", "gpt-5"),
            "AICORE_API_VERSION": os.getenv("AICORE_API_VERSION", "2023-05-15"),
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
            model_name=optional["AICORE_MODEL_NAME"],
            api_version=optional["AICORE_API_VERSION"],
        )

    async def _get_token(self) -> str:
        """
        Obtain an OAuth2 bearer token using client credentials.

        Returns:
            str: Access token.

        Raises:
            HTTPError: If the token request fails.
        """
        if self._token:
            return self._token
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.auth_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=30,
            )
            resp.raise_for_status()
            self._token = resp.json()["access_token"]
            logger.info("SAP AI Core authentication successful")
            return self._token

    async def _chat_completion(self, messages: list) -> str:
        """
        Send a chat completion request and return the assistant's content.

        Args:
            messages: List of message dicts with "role" and "content".

        Returns:
            str: The assistant's response content (or empty string if none).

        Raises:
            httpx.HTTPStatusError: If the API returns an error status.
        """
        token = await self._get_token()
        url = (
            f"{self.base_url}/v2/inference/deployments/{self.chat_deployment_id}/chat/completions"
            f"?api-version={self.api_version}"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "AI-Resource-Group": self.resource_group,
            "Content-Type": "application/json",
        }
        payload = {
            "messages": messages,
            "max_completion_tokens": 4000,
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "").strip()
            return ""

    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        required_keys: Optional[Sequence[str]] = None,
        temperature: float = 0.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Request a JSON response from the LLM and parse it.

        Args:
            system_prompt: System instruction (e.g., "You are an expert...").
            user_prompt: The user query / context.
            required_keys: Optional list of keys that must be present in the parsed JSON.
            temperature: Ignored (kept for compatibility with existing callers).

        Returns:
            Parsed JSON dictionary, or None if parsing fails after 5 attempts.
        """
        if not user_prompt or len(user_prompt.strip()) < 10:
            logger.warning("User prompt too short")
            return None

        required_keys = required_keys or []
        enhanced_system = system_prompt.strip() + "\n\nReturn ONLY valid JSON. No markdown, no explanations."

        for attempt in range(5):
            try:
                content = await self._chat_completion(
                    [
                        {"role": "system", "content": enhanced_system},
                        {"role": "user", "content": user_prompt},
                    ]
                )
                if not content:
                    continue
                parsed = self._extract_json(content)
                if parsed is None:
                    continue
                if required_keys and set(required_keys) - set(parsed.keys()):
                    continue
                logger.info("JSON parsing succeeded")
                return parsed
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                await asyncio.sleep(1)
        logger.error("All attempts failed")
        return None

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Extract a JSON object from a string that may contain markdown or extra text.

        Tries:
            1. Direct json.loads()
            2. Remove markdown code fences and try again.
            3. Brace counting to find the outermost {...} block.

        Args:
            text: Raw LLM output.

        Returns:
            Parsed dictionary or None.
        """
        text = text.strip()
        try:
            return json.loads(text)
        except:
            pass
        cleaned = re.sub(r'```(?:json)?\s*', '', text, flags=re.IGNORECASE)
        cleaned = re.sub(r'```\s*$', '', cleaned)
        try:
            return json.loads(cleaned)
        except:
            pass
        start = cleaned.find('{')
        if start == -1:
            return None
        brace = 0
        in_str = False
        esc = False
        for i in range(start, len(cleaned)):
            ch = cleaned[i]
            if esc:
                esc = False
                continue
            if ch == '\\':
                esc = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if not in_str:
                if ch == '{':
                    brace += 1
                elif ch == '}':
                    brace -= 1
                    if brace == 0:
                        try:
                            return json.loads(cleaned[start:i+1])
                        except:
                            return None
        return None

    async def complete_text(self, *, system_prompt: str, user_prompt: str) -> str:
        """
        Request a plain text response from the LLM.

        Args:
            system_prompt: System instruction.
            user_prompt: User query.

        Returns:
            Assistant's response as a string (empty if error).
        """
        try:
            return await self._chat_completion([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ])
        except Exception as e:
            logger.error(f"Text call failed: {e}")
            return ""


# Backward compatibility alias
AICoreLLMClient = SAPAICoreLLMClient