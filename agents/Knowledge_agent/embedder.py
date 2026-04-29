"""SAP AI Core embedding client for vectorization."""

import asyncio
from typing import List
from gen_ai_hub.proxy.native.openai import OpenAI
from config import get_settings


class Embedder:
    """Generate embeddings using SAP AI Core."""
    
    def __init__(self):
        self.config = get_settings()
        self.client = OpenAI()
    
    def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text (sync)."""
        response = self.client.embeddings.create(
            deployment_id=self.config.embedding_deployment_id,
            input=[text[:8000]]
        )
        return response.data[0].embedding
    
    async def embed_async(self, text: str) -> List[float]:
        """Generate embedding for a single text (async)."""
        # Run sync embedding in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.embed, text)
    
    async def embed_batch_async(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts in parallel."""
        tasks = [self.embed_async(text) for text in texts]
        return await asyncio.gather(*tasks)
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts (sync fallback)."""
        # Use async version with new event loop
        try:
            loop = asyncio.get_running_loop()
            # Already in async context
            return asyncio.run_coroutine_threadsafe(
                self.embed_batch_async(texts), loop
            ).result()
        except RuntimeError:
            # No running loop, create one
            return asyncio.run(self.embed_batch_async(texts))