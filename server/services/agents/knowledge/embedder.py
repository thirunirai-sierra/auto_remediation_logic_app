# server/services/agents/knowledge/embedder.py
"""SAP AI Core embedding client for vectorization."""
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from gen_ai_hub.proxy.native.openai import OpenAI
from config import Settings, get_settings

logger = logging.getLogger(__name__)


class Embedder:
    """Generate embeddings using SAP AI Core (thread‑safe)."""

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialise the Embedder with optional configuration.

        Args:
            settings (Optional[Settings]): Configuration object. If None, uses default settings.

        Attributes:
            client (Optional[OpenAI]): OpenAI client instance for embedding generation.
            config (Settings): Settings/configuration object.
        """
        self.config = settings or get_settings()
        try:
            self.client = OpenAI()
            logger.info("Embedder: OpenAI client initialised")
        except Exception as e:
            logger.error("Embedder: Failed to initialise OpenAI client: %s", e)
            self.client = None

    def embed(self, text: str) -> List[float]:
        """
        Generate an embedding for a single text string.

        Args:
            text (str): Input text to vectorize. Only the first 8000 characters are used.

        Returns:
            List[float]: The embedding vector for the input text.

        Raises:
            RuntimeError: If the Embedder client was not successfully initialised.
        """
        if not self.client:
            raise RuntimeError("Embedder client not initialised")
        response = self.client.embeddings.create(
            deployment_id=self.config.EMBEDDING_DEPLOYMENT_ID,
            input=[text[:8000]],
        )
        return response.data[0].embedding

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts using a thread pool.

        This is thread-safe and can be called from both async and sync contexts.

        Args:
            texts (List[str]): List of text strings to embed.

        Returns:
            List[List[float]]: List of embedding vectors corresponding to each input text.
        """
        if not texts:
            return []
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(self.embed, texts))
        return results
    _embedder_instance = None


def get_embedder(settings: Optional[Settings] = None) -> Embedder:
    """
    Get a singleton Embedder instance.

    Args:
        settings (Optional[Settings]): Configuration object. If None, uses default settings.
            The first call determines the settings; later calls ignore the argument.

    Returns:
        Embedder: The singleton Embedder instance.
    """
    global _embedder_instance
    if _embedder_instance is None:
        _embedder_instance = Embedder(settings)
    return _embedder_instance