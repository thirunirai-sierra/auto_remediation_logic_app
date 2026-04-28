from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

from logic_app_remediator.rca.models.rca_model import JSONDict

logger = logging.getLogger(__name__)


class AICoreLLMClient:
    def __init__(
        self,
        *,
        auth_url: str,
        client_id: str,
        client_secret: str,
        base_url: str,
        resource_group: str,
        chat_deployment_id: str,
        timeout_seconds: int = 60,
    ) -> None:
        self.auth_url = auth_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self.resource_group = resource_group
        self.chat_deployment_id = chat_deployment_id
        self.timeout_seconds = timeout_seconds
        self._validate_configuration()

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
        try:
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
            logger.debug("AI Core token fetch success")
            return str(token)
        except Exception as ex:
            logger.debug("AI Core token fetch failure: %s", ex)
            raise

    def _chat_url(self) -> str:
        return (
            f"{self.base_url}/resource-groups/{self.resource_group}"
            f"/deployments/{self.chat_deployment_id}/chat/completions"
        )

    def _alternate_chat_url(self) -> str:
        return f"{self.base_url}/inference/deployments/{self.chat_deployment_id}/chat/completions"

    def _validate_configuration(self) -> None:
        parsed = urlparse(self.base_url)
        if not parsed.scheme or not parsed.netloc:
            logger.warning("AI Core base_url looks invalid: %s", self.base_url)
        auth_host = urlparse(self.auth_url).netloc
        base_host = parsed.netloc
        if auth_host and base_host:
            logger.debug("AI Core config check: auth_host=%s base_host=%s", auth_host, base_host)
        if not self.resource_group.strip():
            logger.warning("AI Core resource group is empty")
        if not self.chat_deployment_id.strip():
            logger.warning("AI Core chat deployment id is empty")

    @staticmethod
    def _safe_headers(token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token[:8]}...{token[-4:]}" if token else "Bearer <empty>",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _payload(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        return {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 800,
        }

    def _call_chat(self, *, token: str, url: str, payload: Dict[str, Any]) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        logger.debug("AI Core chat URL: %s", url)
        logger.debug("AI Core chat headers (safe): %s", self._safe_headers(token))
        logger.debug("AI Core chat payload: %s", payload)
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout_seconds,
        )
        logger.debug("AI Core chat response status: %s", resp.status_code)
        return resp

    def verify_deployment(self) -> Tuple[bool, str]:
        """
        Best-effort verification by probing known chat URL shapes with a tiny payload.
        Returns (ok, message).
        """
        try:
            token = self._token()
        except Exception as ex:
            return (False, f"token_fetch_failed: {ex}")

        payload = self._payload("Return {} only.", "{}")
        candidates = [self._chat_url(), self._alternate_chat_url()]
        errors = []
        for url in candidates:
            try:
                resp = self._call_chat(token=token, url=url, payload=payload)
                if resp.status_code in (200, 201):
                    return (True, f"ok:{url}")
                if resp.status_code == 404:
                    errors.append(f"404:{url}")
                else:
                    errors.append(f"{resp.status_code}:{url}")
            except Exception as ex:
                errors.append(f"err:{url}:{ex}")
        return (False, "; ".join(errors))

    def test_connection(self) -> Optional[JSONDict]:
        """
        Standalone call for manual troubleshooting.
        """
        return self.complete_json(
            system_prompt="You are a test assistant. Return JSON only.",
            user_prompt='{"ping":"pong"}',
        )

    def complete_json(self, *, system_prompt: str, user_prompt: str) -> Optional[JSONDict]:
        required_keys = [
            "error_location",
            "action_type",
            "error_code",
            "root_cause",
            "exact_issue",
            "recommendation",
            "solution",
            "confidence",
        ]
        for attempt in range(3):
            try:
                token = self._token()
                payload = self._payload(system_prompt, user_prompt)
                primary_url = self._chat_url()
                resp = self._call_chat(token=token, url=primary_url, payload=payload)
                if resp.status_code == 404:
                    logger.error(
                        "Invalid deployment or endpoint (404). deployment_id=%s resource_group=%s url=%s",
                        self.chat_deployment_id,
                        self.resource_group,
                        primary_url,
                    )
                    alt_url = self._alternate_chat_url()
                    logger.warning("Trying alternate AI Core chat path: %s", alt_url)
                    resp = self._call_chat(token=token, url=alt_url, payload=payload)
                    if resp.status_code == 404:
                        logger.error(
                            "Invalid deployment or endpoint on alternate path (404). deployment_id=%s resource_group=%s",
                            self.chat_deployment_id,
                            self.resource_group,
                        )
                resp.raise_for_status()
                data: Dict[str, Any] = resp.json()
                content = (
                    (((data.get("choices") or [{}])[0]).get("message") or {}).get("content")
                    or ""
                ).strip()
                logger.debug("LLM raw response: %s", content)
                if not content:
                    raise ValueError("Empty LLM content")
                start = content.find("{")
                end = content.rfind("}")
                if start == -1 or end == -1 or end < start:
                    raise ValueError("LLM response did not contain valid JSON object text")
                parsed = json.loads(content[start : end + 1])
                if not isinstance(parsed, dict):
                    raise ValueError("LLM response JSON root is not an object")
                if not all(k in parsed for k in required_keys):
                    logger.warning("LLM response missing required keys")
                return parsed
            except Exception as ex:
                logger.warning("LLM parse failed (attempt %s): %s", attempt + 1, ex)
        return None


def test_aicore_connection_from_env() -> Optional[JSONDict]:
    """
    Simple standalone LLM connectivity test.
    Usage:
      python -c "from logic_app_remediator.rca.models.llm_client import test_aicore_connection_from_env as t; print(t())"
    """
    client = AICoreLLMClient.from_env()
    ok, msg = client.verify_deployment()
    logger.info("AI Core deployment verification: ok=%s detail=%s", ok, msg)
    return client.test_connection()
