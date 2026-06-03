"""
In-memory fix pipeline progress for observability UI polling.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

FIX_STAGES = ["Submit", "Get Workflow", "Validate", "Patch", "Deploy"]

_lock = threading.Lock()
_progress: Dict[str, Dict[str, Any]] = {}


def set_progress(
    incident_id: str,
    *,
    status: str = "FIX_IN_PROGRESS",
    step_index: int,
    current_step: str,
    steps_done: Optional[List[str]] = None,
    fix_summary: Optional[str] = None,
) -> None:
    done = steps_done if steps_done is not None else FIX_STAGES[: max(0, step_index)]
    payload = {
        "status": status,
        "step_index": step_index,
        "total_steps": len(FIX_STAGES),
        "current_step": current_step,
        "steps_done": done,
        "fix_summary": fix_summary,
    }
    with _lock:
        _progress[incident_id] = payload


def get_progress(incident_id: str) -> Optional[Dict[str, Any]]:
    with _lock:
        return _progress.get(incident_id)


def clear_progress(incident_id: str) -> None:
    with _lock:
        _progress.pop(incident_id, None)
