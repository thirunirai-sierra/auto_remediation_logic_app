# services/event_mesh/queues.py
"""
Four Event Mesh queues — one per agent in the remediation pipeline.
Verifier removed from pipeline.
"""

from typing import Dict, List, Optional
from config import get_settings

AGENT_NAMES: List[str] = ["observer", "classifier", "rca", "fixer"]

NEXT_AGENT: Dict[str, Optional[str]] = {
    "observer":   "classifier",
    "classifier": "rca",
    "rca":        "fixer",
    "fixer":      None,
}


def _default_queues() -> Dict[str, str]:
    prefix = "default/event/1/api/agents"
    return {
        "observer":   f"{prefix}/observer",
        "classifier": f"{prefix}/classifier",
        "rca":        f"{prefix}/rca",
        "fixer":      f"{prefix}/fixer",
    }


def get_queue_map() -> Dict[str, str]:
    s = get_settings()
    defaults = _default_queues()
    return {
        "observer":   getattr(s, "EVENT_MESH_QUEUE_OBSERVER",   None) or defaults["observer"],
        "classifier": getattr(s, "EVENT_MESH_QUEUE_CLASSIFIER", None) or defaults["classifier"],
        "rca":        getattr(s, "EVENT_MESH_QUEUE_RCA",        None) or defaults["rca"],
        "fixer":      getattr(s, "EVENT_MESH_QUEUE_FIXER",      None) or defaults["fixer"],
    }


def get_queue_for_agent(agent: str) -> str:
    """Return the full queue name for a given agent. Never returns None."""
    qmap = get_queue_map()
    if agent not in qmap:
        # Fallback: construct a sensible default rather than returning None
        return f"default/event/1/api/agents/{agent}"
    return qmap[agent]


def get_agent_for_queue(queue_name: str) -> Optional[str]:
    """Map a queue name back to an agent name."""
    qmap = get_queue_map()
    for agent, q in qmap.items():
        if q == queue_name:
            return agent
    for agent in AGENT_NAMES:
        if queue_name.endswith(f"/{agent}"):
            return agent
    low = queue_name.lower()
    for agent in AGENT_NAMES:
        if agent in low:
            return agent
    return None


def get_next_agent(agent: str) -> Optional[str]:
    return NEXT_AGENT.get(agent)


def queue_definitions() -> List[dict]:
    """Metadata for GET /api/event-mesh/queues."""
    s = get_settings()
    qmap = get_queue_map()
    base = (getattr(s, "PIPELINE_API_BASE", None) or "http://127.0.0.1:8000").rstrip("/")
    defs = []
    for i, agent in enumerate(AGENT_NAMES):
        q = qmap[agent]
        next_ag = NEXT_AGENT.get(agent)
        defs.append({
            "agent":            agent,
            "queue":            q,
            "order":            i + 1,
            "next_agent":       next_ag,
            "next_queue":       qmap.get(next_ag) if next_ag else None,
            "api_endpoint":     f"{base}/api/agents/{agent}",
            "webhook_consumer": f"{base}/api/event-mesh/consume/{agent}",
        })
    return defs