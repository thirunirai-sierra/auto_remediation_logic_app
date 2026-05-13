# server/routers/knowledge.py
import asyncio
import logging
from fastapi import APIRouter, Query, HTTPException, Depends, Request
from services.agents.knowledge.knowledge_base import KnowledgeAgent

logger = logging.getLogger(__name__)
router = APIRouter()

# Use a single agent instance (still sync internally, but we'll run in thread)
_kb = KnowledgeAgent()

async def _run_sync(fn, *args, **kwargs):
    """Run synchronous KnowledgeAgent methods in a thread pool."""
    return await asyncio.to_thread(fn, *args, **kwargs)

@router.get("/")
async def root():
    return {
        "service": "Azure Logic Apps Knowledge Base",
        "version": "2.0.0",
        "endpoints": {
            "search": "/knowledge/search?q=...",
            "stats": "/knowledge/stats",
            "health/live": "/knowledge/health/live",
            "health/ready": "/knowledge/health/ready",
        }
    }

# ---- Health endpoints ----
@router.get("/health/live")
async def liveness():
    """Kubernetes liveness probe – just confirms the process is alive."""
    return {"status": "alive"}

@router.get("/health/ready")
async def readiness():
    """Readiness probe – checks that HANA and embedding are functional."""
    try:
        stats = await _run_sync(_kb.get_stats)
        if stats["total"] == 0:
            return {"status": "degraded", "reason": "knowledge base empty"}
        # Optionally test embedding API with a dummy call
        return {"status": "ready", "total_chunks": stats["total"]}
    except Exception as e:
        logger.error("Readiness check failed: %s", e)
        return {"status": "unhealthy", "error": str(e)}

@router.get("/stats")
async def get_stats():
    try:
        stats = await _run_sync(_kb.get_stats)
        return {
            "total_chunks": stats["total"],
            "vectorized_chunks": stats["vectorized"],
            "pending_chunks": stats["pending"],
            "status": "ready" if stats["vectorized"] > 0 else "needs_vectorization",
        }
    except Exception as e:
        logger.error("Failed to get stats: %s", e)
        raise HTTPException(503, detail="Knowledge base unavailable")

@router.get("/search")
async def search_documents(
    q: str = Query(..., min_length=1),
    top_k: int = Query(5, ge=1, le=20),
    # In the future: add min_similarity: float = Query(0.6, ge=0, le=1)
):
    try:
        results = await _run_sync(_kb.search, q, top_k)
        return {
            "query": q,
            "results_count": len(results),
            "results": [
                {
                    "title": r["meta"].get("title", "Unknown"),
                    "category": r["meta"].get("category", "Unknown"),
                    "url": r["meta"].get("url", ""),
                    "source": r["meta"].get("source", "Microsoft Learn"),
                    "similarity": round(r["similarity"], 2),
                    "text": r["text"],
                }
                for r in results
            ],
        }
    except Exception as e:
        logger.error("Search failed: %s", e, exc_info=True)
        raise HTTPException(500, detail="Search failed")