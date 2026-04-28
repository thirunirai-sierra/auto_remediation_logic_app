#main_server.py
"""FastAPI server for HANA Knowledge Base - Search Logic Apps documentation."""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from agents.Knowledge_agent import KnowledgeAgent

# Initialize Knowledge Agent ONCE (global)
kb = KnowledgeAgent()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    print("\n" + "=" * 60)
    print("🚀 HANA Knowledge Base API Server")
    print("=" * 60)
   
    try:
        stats = kb.get_stats()
        print(f"✅ Connected to HANA")
        print(f"   Total chunks: {stats['total']}")
        print(f"   Vectorized: {stats['vectorized']}")
        print(f"   Pending: {stats['pending']}")
    except Exception as e:
        print(f"⚠️ HANA connection issue: {e}")
   
    print("\n📍 Swagger UI: http://127.0.0.1:8000/docs")
    print("📍 Health Check: http://127.0.0.1:8000/health")
    print("\n" + "=" * 60 + "\n")
   
    yield
   
    print("\n👋 Server shutting down...\n")


app = FastAPI(
    title="Azure Logic Apps Knowledge Base",
    description="Semantic search over Microsoft Logic Apps documentation using HANA vector database",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Azure Logic Apps Knowledge Base",
        "version": "1.0.0",
        "endpoints": {
            "search": "/search?q=your query&top_k=5",
            "stats": "/stats",
            "health": "/health",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        stats = kb.get_stats()
        return {
            "status": "healthy",
            "total_chunks": stats["total"],
            "vectorized": stats["vectorized"]
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }


@app.get("/stats")
async def get_stats():
    """Get knowledge base statistics."""
    try:
        stats = kb.get_stats()
        return {
            "total_chunks": stats["total"],
            "vectorized_chunks": stats["vectorized"],
            "pending_chunks": stats["pending"],
            "status": "ready" if stats["vectorized"] > 0 else "needs_vectorization"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
async def search_documents(
    q: str = Query(..., description="Search query", min_length=1),
    top_k: int = Query(5, description="Number of results", ge=1, le=20)
):
    """Search for similar documentation using vector similarity."""
    try:
        # Use the global kb instance
        results = kb.search(q, top_k)
       
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
                    "text": r["text"]
                }
                for r in results
            ]
        }
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        reload=False
    )
 