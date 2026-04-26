"""
AI Manager - Handles AI API interactions (lightweight version, no embeddings)
"""

import logging
import os

import aiohttp

from utils.get_logger import get_logger


class AIManager:
    """
    AIManager handles AI operations including embeddings and prompt-related tasks.

    Uses Voyage AI (voyage-3.5-lite):
    - 512 dimensional embeddings (optimal for FAISS)
    - Very fast & highly ranked on MTEB
    - Optimized for semantic search

    https://docs.voyageai.com/docs/api-key-and-installation
    """

    def __init__(self, logger: logging.Logger | None = None, verbose: bool = False):
        self.logger = logger or get_logger(__name__)
        self.verbose = verbose

        # Optional backend URL for additional AI operations
        self.server_url = os.getenv("AI_API_URL", "")
        if self.server_url and not self.server_url.startswith(("http://", "https://")):
            self.server_url = f"http://{self.server_url}"

    async def __aenter__(self):
        return self

    async def create(self):
        """Initialize async resources (placeholder)."""
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Clean shutdown (placeholder)."""

    # ----------------------------------------------------------------------
    # Health check
    # ----------------------------------------------------------------------

    async def ready(self) -> bool:
        """
        Check if AI services are available.
        For Voyage, this just verifies the API key + optional backend.
        """
        api_key = os.getenv("VOYAGE_API_KEY")
        if not api_key:
            if self.verbose:
                self.logger.warning("AIManager: VOYAGE_API_KEY not configured")
            return False

        if self.server_url:
            try:
                async with aiohttp.ClientSession().get(f"{self.server_url}/health") as response:
                    if response.status != 200 and self.verbose:
                        self.logger.warning(
                            f"AIManager: Backend health check failed ({response.status})"
                        )
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"AI backend not reachable: {str(e)}")

        return True

    def close(self):
        """Clean up resources (placeholder)."""
