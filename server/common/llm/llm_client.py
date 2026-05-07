from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests

from common.llm.model import get_llm
from agent.observer.models.rca_model import JSONDict

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

    def _get_proxy_llm(self, temperature: float = 0.0) -> Any:
        return get_llm(temperature=temperature, deployment_id=self.chat_deployment_id)

    def _chat_url(self) -> str:
        return f"{self.base_url}/inference/deployments/{self.chat_deployment_id}/chat/completions"

    def _validate_configuration(self) -> None:
        parsed = urlparse(self.base_url)
        if not parsed.scheme or not parsed.netloc:
            logger.warning("AI Core base_url looks invalid: %s", self.base_url)

    @staticmethod
    def _safe_headers(token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token[:8]}...{token[-4:]}" if token else "Bearer <empty>",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _payload(system_prompt: str, user_prompt: str, max_tokens: int = 800) -> Dict[str, Any]:
        return {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
        }

    def _call_chat(self, *, token: str, url: str, payload: Dict[str, Any]) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if self.resource_group:
            headers["AI-Resource-Group"] = self.resource_group
        max_attempts = 4
        for attempt in range(max_attempts):
            logger.debug("AI Core final chat URL: %s", url)
            logger.debug("AI Core chat headers (safe): %s", self._safe_headers(token))
            logger.debug("AI Core chat payload: %s", payload)
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.timeout_seconds,
            )
            if resp.status_code == 429 and attempt < max_attempts - 1:
                time.sleep(2**attempt)
                continue
            return resp
        return resp

    def verify_deployment(self) -> Tuple[bool, str]:
        try:
            token = self._token()
        except Exception as ex:
            return (False, f"token_fetch_failed: {ex}")
        payload = self._payload("Return {} only.", "{}")
        url = self._chat_url()
        try:
            resp = self._call_chat(token=token, url=url, payload=payload)
            if resp.status_code in (200, 201):
                return (True, f"ok:{url}")
            return (False, f"{resp.status_code}:{url}")
        except Exception as ex:
            return (False, f"err:{url}:{ex}")

    def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        required_keys: Optional[Sequence[str]] = None,
    ) -> Optional[JSONDict]:
        for attempt in range(3):
            try:
                llm = self._get_proxy_llm()
                response = llm.invoke(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                )
                content = str(getattr(response, "content", "") or "").strip()
                start = content.find("{")
                end = content.rfind("}")
                if start == -1 or end == -1 or end < start:
                    raise ValueError("LLM response did not contain valid JSON object text")
                parsed = json.loads(content[start : end + 1])
                if not isinstance(parsed, dict):
                    raise ValueError("LLM response JSON root is not an object")
                if required_keys and not all(k in parsed for k in required_keys):
                    logger.warning("LLM response missing required keys")
                return parsed
            except Exception as ex:
                logger.warning("ChatOpenAI parse failed (attempt %s): %s", attempt + 1, ex)
            try:
                token = self._token()
                payload = self._payload(system_prompt, user_prompt)
                chat_url = self._chat_url()
                resp = self._call_chat(token=token, url=chat_url, payload=payload)
                resp.raise_for_status()
                data: Dict[str, Any] = resp.json()
                message_obj = (((data.get("choices") or [{}])[0]).get("message") or {})
                content_raw = message_obj.get("content") or ""
                if isinstance(content_raw, list):
                    text_parts = [
                        str(item.get("text", ""))
                        for item in content_raw
                        if isinstance(item, dict)
                    ]
                    content = "".join(text_parts).strip()
                else:
                    content = str(content_raw).strip()
                start = content.find("{")
                end = content.rfind("}")
                if start == -1 or end == -1 or end < start:
                    raise ValueError("LLM response did not contain valid JSON object text")
                parsed = json.loads(content[start : end + 1])
                if not isinstance(parsed, dict):
                    raise ValueError("LLM response JSON root is not an object")
                if required_keys and not all(k in parsed for k in required_keys):
                    logger.warning("LLM response missing required keys")
                return parsed
            except Exception as ex:
                logger.warning("LLM parse failed (attempt %s): %s", attempt + 1, ex)
        return None


def test_aicore_connection_from_env() -> Optional[JSONDict]:
    client = AICoreLLMClient.from_env()
    ok, msg = client.verify_deployment()
    logger.info("AI Core deployment verification: ok=%s detail=%s", ok, msg)
    return client.complete_json(
        system_prompt="You are a test assistant. Return JSON only.",
        user_prompt='{"ping":"pong"}',
    )
