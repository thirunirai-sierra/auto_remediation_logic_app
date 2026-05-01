"""Knowledge package: RAG helpers + optional KnowledgeAgent."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent.knowledge.knowledge_base import KnowledgeAgent

__all__ = ["KnowledgeAgent"]


def __getattr__(name: str):
    if name == "KnowledgeAgent":
        from agent.knowledge.knowledge_base import KnowledgeAgent as _KnowledgeAgent

        return _KnowledgeAgent
    raise AttributeError(name)
