"""
Pipeline message envelope passed on Event Mesh queues between agents.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class PipelineEnvelope(BaseModel):
    """Single unit of work flowing across the five agent queues."""

    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    workflow_name: str
    run_id: str  # Azure Logic App run ID (not ORBLOGICAPPS-*)
    subscription_id: str
    resource_group: str
    incident_id: Optional[str] = None  # HANA INCIDENT_ID when known (manual apply-fix)

    current_agent: Optional[str] = None
    source: str = "orchestrator"

    observer: Dict[str, Any] = Field(default_factory=dict)
    classifier: Dict[str, Any] = Field(default_factory=dict)
    rca: Dict[str, Any] = Field(default_factory=dict)
    fixer: Dict[str, Any] = Field(default_factory=dict)
    verifier: Dict[str, Any] = Field(default_factory=dict)

    status: str = "pending"
    error: Optional[str] = None
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    class Config:
        extra = "allow"

    def to_event_payload(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_event_payload(cls, data: Dict[str, Any]) -> "PipelineEnvelope":
        if "correlation_id" not in data and "run_id" in data:
            data = dict(data)
            data.setdefault("correlation_id", str(uuid4()))
        return cls.model_validate(data)
