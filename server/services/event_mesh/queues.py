# services/event_mesh/queues.py
"""
Five Event Mesh queues — one per agent in the remediation pipeline.
Queue names match those created in SAP Event Mesh.
"""

from typing import Dict, List, Optional

from config import get_settings

# Pipeline order (orchestrator publishes to observer only)
AGENT_NAMES: List[str] = ["observer", "classifier", "rca", "fixer", "verifier"]

NEXT_AGENT: Dict[str, Optional[str]] = {
    "observer": "classifier",
    "classifier": "rca",
    "rca": "fixer",
    "fixer": "verifier",
    "verifier": None,
}


def _default_queues() -> Dict[str, str]:
    """
    Default queue names that match SAP Event Mesh configuration.
    Modify these if your queues have a different prefix.
    """
    prefix = "default/event/1/api/agents"
    return {
        "observer":   f"{prefix}/observer",
        "classifier": f"{prefix}/classifier",
        "rca":        f"{prefix}/rca",
        "fixer":      f"{prefix}/fixer",
        "verifier":   f"{prefix}/verifier",
    }


def get_queue_map() -> Dict[str, str]:
    """
    Resolve queue topic names from settings or defaults.
    Environment variables can override each queue individually.
    """
    s = get_settings()
    defaults = _default_queues()
    return {
        "observer":   getattr(s, "EVENT_MESH_QUEUE_OBSERVER", None) or defaults["observer"],
        "classifier": getattr(s, "EVENT_MESH_QUEUE_CLASSIFIER", None) or defaults["classifier"],
        "rca":        getattr(s, "EVENT_MESH_QUEUE_RCA", None) or defaults["rca"],
        "fixer":      getattr(s, "EVENT_MESH_QUEUE_FIXER", None) or defaults["fixer"],
        "verifier":   getattr(s, "EVENT_MESH_QUEUE_VERIFIER", None) or defaults["verifier"],
    }


def get_queue_for_agent(agent: str) -> str:
    """Return the full queue name for a given agent."""
    qmap = get_queue_map()
    if agent not in qmap:
        raise ValueError(f"Unknown agent: {agent}")
    return qmap[agent]


def get_agent_for_queue(queue_name: str) -> Optional[str]:
    """Map a queue name (e.g., from x-em-queue header) back to an agent."""
    qmap = get_queue_map()
    # Exact match first
    for agent, q in qmap.items():
        if q == queue_name:
            return agent
    # Fallback: check if the queue name ends with the agent name
    for agent in AGENT_NAMES:
        if queue_name.endswith(f"/{agent}"):
            return agent
    # Last resort: search for agent name anywhere in the queue name
    low = queue_name.lower()
    for agent in AGENT_NAMES:
        if agent in low:
            return agent
    return None


def get_next_agent(agent: str) -> Optional[str]:
    """Return the next agent in the pipeline, or None if this is the last."""
    return NEXT_AGENT.get(agent)


def queue_definitions() -> List[dict]:
    """Metadata for GET /api/event-mesh/queues."""
    s = get_settings()
    qmap = get_queue_map()
    base = (getattr(s, "PIPELINE_API_BASE", None) or "http://127.0.0.1:8000").rstrip("/")
    defs = []
    for i, agent in enumerate(AGENT_NAMES):
        q = qmap[agent]
        defs.append({
            "agent": agent,
            "queue": q,
            "order": i + 1,
            "next_agent": NEXT_AGENT.get(agent),
            "next_queue": qmap.get(NEXT_AGENT[agent]) if NEXT_AGENT.get(agent) else None,
            "api_endpoint": f"{base}/api/agents/{agent}",
            "webhook_consumer": f"{base}/api/event-mesh/consume/{agent}",
        })
    return defs