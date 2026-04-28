"""Knowledge Agent - Institutional Memory for Azure Logic Apps."""

import asyncio
import time
import httpx
from typing import List, Dict, Optional, Any

from agents.Knowledge_agent.client import HanaClient
from agents.Knowledge_agent.scraper import (
    get_knowledge_chunks_async, 
    scrape_page, 
    chunk_text,
    extract_category
)
from agents.Knowledge_agent.embedder import Embedder

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
        """
        Scrape Microsoft docs, chunk, and store in HANA.
        
        Args:
            clear: If True, drop and recreate table before ingestion.
            skip_slow_urls: Skip known problematic URLs.
            batch_size: Number of concurrent scrape operations.
        
        Returns:
            Dictionary with ingestion statistics.
        """
        print("\n📥 Knowledge Agent: Ingesting documentation...")
        start_time = time.time()
        
        chunks = asyncio.run(get_knowledge_chunks_async(
            skip_slow_urls=skip_slow_urls,
            batch_size=batch_size,
            timeout_per_url=45
        ))
        
        print(f"  Collected {len(chunks)} chunks from Microsoft Learn")
        
        if not chunks:
            print("  No chunks collected")
            return {"total": 0, "vectorized": 0, "pending": 0}
        
        with HanaClient() as db:
            db.create_table(drop_first=clear)
            inserted = db.insert_chunks(chunks)
            print(f"  Stored {inserted} chunks in HANA")
            
            elapsed = time.time() - start_time
            print(f"  Ingestion completed in {elapsed:.1f}s")
            
            return db.get_stats()
    
    def vectorize(self, batch_size: int = 20) -> Dict[str, int]:
        """
        Generate embeddings for unvectorized chunks.
        
        Args:
            batch_size: Number of chunks to process per batch.
        
        Returns:
            Dictionary with vectorization statistics.
        """
        print("\n🧠 Knowledge Agent: Generating embeddings...")
        start_time = time.time()
        
        with HanaClient() as db:
            stats = db.get_stats()
            print(f"  Total: {stats['total']} | Pending: {stats['pending']}")
            
            if stats['pending'] == 0:
                print("  All chunks already vectorized")
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
                    print(f"  ✓ Vectorized {vectorized}/{stats['pending']}")
                except Exception as e:
                    failed += len(rows)
                    print(f"  ✗ Failed batch: {e}")
            
            elapsed = time.time() - start_time
            print(f"  Completed: {vectorized} vectorized, {failed} failed in {elapsed:.1f}s")
            
            return {"vectorized": vectorized, "failed": failed}
    
    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Search for similar content using vector similarity.
        
        Args:
            query: User question or error description.
            top_k: Number of results to return.
        
        Returns:
            List of similar chunks with metadata and similarity scores.
        """
        query_vector = self.embedder.embed(query)
        
        with HanaClient() as db:
            return db.search_similar(query_vector, top_k)
    
    def get_stats(self) -> Dict[str, int]:
        """Get knowledge base statistics."""
        with HanaClient() as db:
            return db.get_stats()
    
    def add_url(self, url: str, category: str = None, skip_vectorize: bool = False) -> Dict[str, Any]:
        """
        Add a single URL to knowledge base.
        Automatically skips if URL already exists.
        
        Args:
            url: Microsoft Learn URL to add
            category: Optional category (auto-detected if not provided)
            skip_vectorize: If True, skip embedding generation
        
        Returns:
            Dictionary with status and message
        """
        print(f"\n🌐 Processing URL: {url}")
        
        # Check if URL already exists
        with HanaClient() as db:
            existing_urls = db.get_existing_urls()
            if url in existing_urls:
                return {"success": True, "message": f"⏭️ URL already exists. Skipped."}
        
        # Scrape the URL
        async def scrape():
            async with httpx.AsyncClient(
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                follow_redirects=True
            ) as client:
                return await scrape_page(client, url, timeout=60)
        
        result = asyncio.run(scrape())
        
        if not result:
            return {"success": False, "message": "❌ Failed to scrape URL or no error content found"}
        
        # Chunk the content
        chunks = chunk_text(result["text"])
        
        if not chunks:
            return {"success": False, "message": "❌ No meaningful chunks extracted"}
        
        print(f"   📄 Created {len(chunks)} chunks")
        
        # Prepare chunks for insertion
        chunk_entries = []
        for chunk in chunks:
            chunk_entries.append({
                "text": chunk,
                "meta": {
                    "title": result["title"],
                    "url": url,
                    "category": category or result["category"],
                    "source": "Microsoft Learn",
                    "product": "Azure Logic Apps",
                }
            })
        
        # Insert into HANA
        with HanaClient() as db:
            # Double-check again
            existing_urls = db.get_existing_urls()
            if url in existing_urls:
                return {"success": True, "message": f"⏭️ URL was added by another process. Skipped."}
            
            inserted = db.insert_chunks(chunk_entries)
            print(f"   💾 Inserted {inserted} chunks into HANA")
            
            # Vectorize if requested
            if not skip_vectorize and inserted > 0:
                print(f"   🧠 Generating embeddings...")
                unvectorized = db.get_unvectorized_chunks(limit=inserted)
                if unvectorized:
                    chunk_ids = [row[0] for row in unvectorized]
                    texts = [row[1] for row in unvectorized]
                    vectors = self.embedder.embed_batch(texts)
                    db.update_embeddings(list(zip(chunk_ids, vectors)))
                    print(f"   ✅ Vectorized {len(vectors)} chunks")
        
        return {"success": True, "message": f"✅ Added {url} with {len(chunks)} chunks"}