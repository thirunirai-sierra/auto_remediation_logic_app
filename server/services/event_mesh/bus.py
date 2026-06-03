"""
Event Mesh bus: in-process asyncio queues (local) + optional HTTP publish to SAP EM.
Consumers invoke agent REST APIs: POST /api/agents/{agent}
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable, Dict, List, Optional

import httpx

from config import get_settings
from services.event_mesh.messages import PipelineEnvelope
from services.event_mesh.queues import AGENT_NAMES, get_agent_for_queue, get_queue_for_agent

logger = logging.getLogger(__name__)

_bus: Optional["EventMeshBus"] = None


class EventMeshBus:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._queues: Dict[str, asyncio.Queue] = {
            agent: asyncio.Queue() for agent in AGENT_NAMES
        }
        self._worker_tasks: List[asyncio.Task] = []
        self._running = False

    def _api_base(self) -> str:
        return (getattr(self._settings, "PIPELINE_API_BASE", None) or "http://127.0.0.1:8000").rstrip("/")

    async def publish(self, agent: str, envelope: PipelineEnvelope) -> Dict[str, Any]:
        """Publish envelope to the agent's queue (local bus + optional SAP EM)."""
        if agent not in AGENT_NAMES:
            raise ValueError(f"Unknown agent: {agent}")

        queue_name = get_queue_for_agent(agent)
        envelope.current_agent = agent
        payload = envelope.to_event_payload()

        # Optional: forward to SAP Event Mesh REST publish endpoint
        em_url = getattr(self._settings, "EVENT_MESH_PUBLISH_URL", None)
        if em_url and getattr(self._settings, "EVENT_MESH_PUBLISH_ENABLED", False):
            await self._publish_external(em_url, queue_name, payload)

        await self._queues[agent].put(payload)
        logger.info(
            "[EM-BUS] published correlation=%s agent=%s queue=%s",
            envelope.correlation_id,
            agent,
            queue_name,
        )
        return {
            "published": True,
            "agent": agent,
            "queue": queue_name,
            "correlation_id": envelope.correlation_id,
        }

    async def _publish_external(self, base_url: str, queue_name: str, payload: Dict[str, Any]) -> None:
        """POST to SAP Event Mesh (configure EVENT_MESH_PUBLISH_URL per your tenant)."""
        url = base_url.rstrip("/")
        if "{queue}" in url:
            url = url.format(queue=queue_name)
        else:
            url = f"{url}/{queue_name}"
        headers = {}
        token = getattr(self._settings, "EVENT_MESH_TOKEN", None)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code >= 400:
                    logger.warning("[EM-BUS] external publish %s: %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            logger.warning("[EM-BUS] external publish failed: %s", exc)

    async def deliver_from_webhook(
        self, queue_name: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """SAP Event Mesh webhook delivery → route to agent queue."""
        agent = get_agent_for_queue(queue_name)
        if not agent:
            return {"accepted": False, "reason": f"unknown queue: {queue_name}"}
        await self._queues[agent].put(payload)
        logger.info("[EM-BUS] webhook → local queue agent=%s", agent)
        return {"accepted": True, "agent": agent, "queue": queue_name}

    async def start_workers(self, process_fn: Callable[[str, PipelineEnvelope], Awaitable[None]]) -> None:
        if self._running:
            return
        self._running = True
        for agent in AGENT_NAMES:
            task = asyncio.create_task(self._worker_loop(agent, process_fn), name=f"em-worker-{agent}")
            self._worker_tasks.append(task)
        logger.info("[EM-BUS] started %d queue workers", len(AGENT_NAMES))

    async def stop_workers(self) -> None:
        for t in self._worker_tasks:
            t.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        self._running = False

    async def _worker_loop(
        self,
        agent: str,
        process_fn: Callable[[str, PipelineEnvelope], Awaitable[None]],
    ) -> None:
        q = self._queues[agent]
        while True:
            try:
                raw = await q.get()
                envelope = PipelineEnvelope.from_event_payload(raw)
                logger.info(
                    "[EM-WORKER-%s] correlation=%s run=%s",
                    agent,
                    envelope.correlation_id,
                    envelope.run_id,
                )
                await process_fn(agent, envelope)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("[EM-WORKER-%s] error: %s", agent, exc)

    async def invoke_agent_api(self, agent: str, envelope: PipelineEnvelope) -> Dict[str, Any]:
        """Call POST /api/agents/{agent}/pipeline with envelope (Event Mesh + API pattern)."""
        url = f"{self._api_base()}/api/agents/{agent}/pipeline"
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(url, json=envelope.to_event_payload())
            resp.raise_for_status()
            return resp.json()

    def queue_depths(self) -> Dict[str, int]:
        return {agent: self._queues[agent].qsize() for agent in AGENT_NAMES}


def get_bus() -> EventMeshBus:
    global _bus
    if _bus is None:
        _bus = EventMeshBus()
    return _bus
