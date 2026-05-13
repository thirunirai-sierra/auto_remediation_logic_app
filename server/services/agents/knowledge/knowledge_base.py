# server/services/agents/knowledge/knowledge_base.py
"""
Knowledge Agent that uses HANA vector store for semantic search.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

from db.hana_client import HanaClient
from services.agents.knowledge.embedder import Embedder
from services.agents.knowledge.scraper import get_knowledge_chunks_async
from config import Settings, get_settings

logger = logging.getLogger(__name__)


class KnowledgeAgent:
    """
    Knowledge Agent for storing and retrieving documentation using HANA vector search.
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the knowledge agent.

        Args:
            settings: Application settings, if None will use get_settings().
        """
        self.settings = settings or get_settings()
        self.embedder = Embedder(self.settings)

    def ingest(self, clear: bool = False, skip_slow_urls: bool = True, batch_size: int = 3) -> Dict[str, int]:
        """
        Ingest documentation from Microsoft Learn into HANA.

        Args:
            clear: If True, drop and recreate the knowledge table.
            skip_slow_urls: Skip URLs known to be slow to scrape.
            batch_size: Number of parallel scrapes.

        Returns:
            Statistics dict with total, vectorized, pending counts.
        """
        logger.info("Knowledge Agent: Ingesting documentation...")
        chunks = asyncio.run(
            get_knowledge_chunks_async(
                skip_slow_urls=skip_slow_urls,
                batch_size=batch_size,
                timeout_per_url=45,
            )
        )
        if not chunks:
            logger.info("No chunks collected")
            return {"total": 0, "vectorized": 0, "pending": 0}

        with HanaClient() as db:
            if clear:
                db.create_knowledge_table(drop_first=True)
            else:
                db.create_knowledge_table(drop_first=False)
            inserted = db.insert_knowledge_chunks(chunks)
            logger.info("Inserted %d chunks", inserted)
            stats = db.get_knowledge_stats()
        return stats

    def vectorize(self, batch_size: int = 20) -> Dict[str, int]:
        """
        Generate embeddings for unvectorized chunks.

        Args:
            batch_size: Number of chunks to process per batch.

        Returns:
            Dict with vectorized count and failed count.
        """
        logger.info("Knowledge Agent: Generating embeddings...")
        with HanaClient() as db:
            stats = db.get_knowledge_stats()
            if stats["pending"] == 0:
                logger.info("All chunks already vectorized")
                return {"vectorized": 0, "failed": 0}

            vectorized = 0
            failed = 0
            while True:
                rows = db.get_unvectorized_chunks(batch_size)
                if not rows:
                    break
                texts = [row[1] for row in rows]
                chunk_ids = [row[0] for row in rows]
                try:
                    vectors = self.embedder.embed_batch(texts)
                    db.update_embeddings(list(zip(chunk_ids, vectors)))
                    vectorized += len(rows)
                    logger.info("Vectorized %d/%d", vectorized, stats["pending"])
                except Exception as e:
                    logger.error("Batch vectorization failed: %s", e)
                    failed += len(rows)
            return {"vectorized": vectorized, "failed": failed}

    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Search the knowledge base for chunks similar to the query.

        Args:
            query: Search query string.
            top_k: Number of results to return.

        Returns:
            List of dicts with keys: text, meta, similarity.
        """
        query_vector = self.embedder.embed(query)
        with HanaClient() as db:
            return db.search_similar(query_vector, top_k)

    def get_stats(self) -> Dict[str, int]:
        """Return statistics of the knowledge base."""
        with HanaClient() as db:
            return db.get_knowledge_stats()

    def add_url(self, url: str, category: str = None, skip_vectorize: bool = False) -> Dict[str, Any]:
        """
        Add a single URL to the knowledge base.

        Args:
            url: Microsoft Learn URL.
            category: Optional category string.
            skip_vectorize: If True, do not generate embedding immediately.

        Returns:
            Dict with success flag and message.
        """
        logger.info("Processing URL: %s", url)
        with HanaClient() as db:
            existing = db.get_existing_urls()
            if url in existing:
                return {"success": True, "message": "URL already exists"}

        async def scrape():
            from services.agents.knowledge.scraper import scrape_page
            import httpx
            async with httpx.AsyncClient(
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True,
            ) as client:
                return await scrape_page(client, url, timeout=60)

        result = asyncio.run(scrape())
        if not result:
            return {"success": False, "message": "Failed to scrape URL"}

        from services.agents.knowledge.scraper import chunk_text
        chunks = chunk_text(result["text"])
        if not chunks:
            return {"success": False, "message": "No meaningful chunks extracted"}

        chunk_entries = [
            {
                "text": chunk,
                "meta": {
                    "title": result["title"],
                    "url": url,
                    "category": category or result["category"],
                    "source": "Microsoft Learn",
                    "product": "Azure Logic Apps",
                },
            }
            for chunk in chunks
        ]

        with HanaClient() as db:
            if url in db.get_existing_urls():
                return {"success": True, "message": "URL added by another process"}
            inserted = db.insert_knowledge_chunks(chunk_entries)
            if not skip_vectorize and inserted > 0:
                rows = db.get_unvectorized_chunks(limit=inserted)
                if rows:
                    vectors = self.embedder.embed_batch([row[1] for row in rows])
                    db.update_embeddings([(rows[i][0], vectors[i]) for i in range(len(rows))])
            return {"success": True, "message": f"Added {url} with {len(chunks)} chunks"}