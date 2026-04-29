"""Knowledge Agent - Institutional Memory for Azure Logic Apps."""

import asyncio
import logging
import time
from typing import Any, Dict, List

import httpx

from agent.knowledge.client import HanaClient
from agent.knowledge.embedder import Embedder
from agent.knowledge.scraper import (
    chunk_text,
    get_knowledge_chunks_async,
    scrape_page,
)

logger = logging.getLogger(__name__)


class KnowledgeAgent:
    """
    Knowledge Agent - Institutional Memory.

    Responsibilities:
    - Store and retrieve documentation chunks
    - Vectorize content for semantic search
    - Provide similar content based on error queries
    """

    def __init__(self):
        self.embedder = Embedder()

    def ingest(self, clear: bool = False, skip_slow_urls: bool = True, batch_size: int = 3) -> Dict[str, int]:
        logger.info("\n Knowledge Agent: Ingesting documentation...")
        start_time = time.time()

        chunks = asyncio.run(
            get_knowledge_chunks_async(
                skip_slow_urls=skip_slow_urls,
                batch_size=batch_size,
                timeout_per_url=45,
            )
        )

        logger.info(f"  Collected {len(chunks)} chunks from Microsoft Learn")

        if not chunks:
            logger.info("  No chunks collected")
            return {"total": 0, "vectorized": 0, "pending": 0}

        with HanaClient() as db:
            db.create_table(drop_first=clear)
            inserted = db.insert_chunks(chunks)
            logger.info(f"  Stored {inserted} chunks in HANA")
            elapsed = time.time() - start_time
            logger.info(f"  Ingestion completed in {elapsed:.1f}s")
            return db.get_stats()

    def vectorize(self, batch_size: int = 20) -> Dict[str, int]:
        logger.info("\n Knowledge Agent: Generating embeddings...")
        start_time = time.time()

        with HanaClient() as db:
            stats = db.get_stats()
            logger.info(f"  Total: {stats['total']} | Pending: {stats['pending']}")

            if stats["pending"] == 0:
                logger.info("  All chunks already vectorized")
                return stats

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
                    logger.info(f"   Vectorized {vectorized}/{stats['pending']}")
                except Exception as e:
                    failed += len(rows)
                    logger.info(f"   Failed batch: {e}")

            elapsed = time.time() - start_time
            logger.info(f"  Completed: {vectorized} vectorized, {failed} failed in {elapsed:.1f}s")
            return {"vectorized": vectorized, "failed": failed}

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        query_vector = self.embedder.embed(query)
        with HanaClient() as db:
            return db.search_similar(query_vector, top_k)

    def get_stats(self) -> Dict[str, int]:
        with HanaClient() as db:
            return db.get_stats()

    def add_url(self, url: str, category: str = None, skip_vectorize: bool = False) -> Dict[str, Any]:
        logger.info(f"\n Processing URL: {url}")

        with HanaClient() as db:
            existing_urls = db.get_existing_urls()
            if url in existing_urls:
                return {"success": True, "message": " URL already exists. Skipped."}

        async def scrape():
            async with httpx.AsyncClient(
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                follow_redirects=True,
            ) as client:
                return await scrape_page(client, url, timeout=60)

        result = asyncio.run(scrape())
        if not result:
            return {"success": False, "message": " Failed to scrape URL or no error content found"}

        chunks = chunk_text(result["text"])
        if not chunks:
            return {"success": False, "message": " No meaningful chunks extracted"}

        logger.info(f"    Created {len(chunks)} chunks")
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
            existing_urls = db.get_existing_urls()
            if url in existing_urls:
                return {"success": True, "message": " URL was added by another process. Skipped."}
            inserted = db.insert_chunks(chunk_entries)
            logger.info(f" Inserted {inserted} chunks into HANA")

            if not skip_vectorize and inserted > 0:
                logger.info("  Generating embeddings...")
                unvectorized = db.get_unvectorized_chunks(limit=inserted)
                if unvectorized:
                    chunk_ids = [row[0] for row in unvectorized]
                    texts = [row[1] for row in unvectorized]
                    vectors = self.embedder.embed_batch(texts)
                    db.update_embeddings(list(zip(chunk_ids, vectors)))
                    logger.info(f" Vectorized {len(vectors)} chunks")

        return {"success": True, "message": f" Added {url} with {len(chunks)} chunks"}
