"""Fixer agent package.

Lazy-load Fixer_agent to avoid circular imports:
remediation -> agent.fixer.utils -> agent.fixer (package) must not import remediation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = ["FixerAgent", "get_fixer"]

if TYPE_CHECKING:
    from agent.fixer.Fixer_agent import FixerAgent as FixerAgent
    from agent.fixer.Fixer_agent import get_fixer as get_fixer


def __getattr__(name: str):
    if name == "FixerAgent":
        from agent.fixer.Fixer_agent import FixerAgent as _FixerAgent

        globals()["FixerAgent"] = _FixerAgent
        return _FixerAgent
    if name == "get_fixer":
        from agent.fixer.Fixer_agent import get_fixer as _get_fixer

        globals()["get_fixer"] = _get_fixer
        return _get_fixer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
