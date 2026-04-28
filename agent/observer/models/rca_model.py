from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, Sequence

Action = Dict[str, Any]
JSONDict = Dict[str, Any]


@dataclass(frozen=True)
class RCAResult:
    error_location: str
    action_type: str
    error_code: str
    root_cause: str
    exact_issue: str
    recommendation: str
    solution: str
    confidence: float

    def to_dict(self) -> JSONDict:
        return {
            "error_location": self.error_location,
            "action_type": self.action_type,
            "error_code": self.error_code,
            "root_cause": self.root_cause,
            "exact_issue": self.exact_issue,
            "recommendation": self.recommendation,
            "solution": self.solution,
            "confidence": self.confidence,
        }


class LLMClient(Protocol):
    def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        required_keys: Optional[Sequence[str]] = None,
    ) -> Optional[JSONDict]:
        ...
