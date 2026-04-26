"""
AI Firebase Functions Handlers
Handles AI classification of search queries.
"""

import logging
import time

from utils.redis_cache import RedisCache

# Note: classifier import commented out - not needed for embedding pipeline
# from media_manager.mediacircle.ai.prompts.classifier import ClassificationResponse, classify

CacheExpiration = -1
AICache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="ai",
    verbose=False,
    isClassMethod=True,
)
# Configure logging
logger = logging.getLogger(__name__)


class AIHandler:
    """Class containing AI-focused Firebase Functions."""

    def __init__(self):
        """Initialize AI Handler"""
        logger.info("AIHandler initialized")

    @RedisCache.use_cache(AICache, prefix="classify_search_query")
    async def classify_search_query(self, query: str):
        """
        Classify a search query using AI to determine intent and routing.

        Args:
            req.data: {
                "query": str  # The search query to classify
            }

        Returns:
            dict: {
                "results": [
                    {
                        "type": str,        # Classification type (e.g., "Actor", "Movie", "Genre", "Keyword")
                        "value": str,       # The classified value/entity
                        "tmdb_id": int|None,  # TMDB ID if applicable
                        "scope": str        # Search scope (e.g., "All", "Movies", "TV", "Music")
                    }
                ],
                "error": str|None,          # Error message if classification failed
                "execution_time": float     # Time taken to classify
            }
        """
        try:
            t0 = time.perf_counter()
            logger.info(f"SEARCH: CLASSIFIER => Classifying query: '{query}'")
            # classification_response: ClassificationResponse = await classify(query)
            logger.info(f"SEARCH: CLASSIFIER => Response in {time.perf_counter() - t0} seconds")
            # return classification_response
            raise NotImplementedError("Classifier not implemented yet")
        except Exception as e:
            logger.error(f"SEARCH: CLASSIFIER => Error classifying query: '{query}' : {e}")
            # return ClassificationResponse(query=query, results=[], error=str(e), execution_time=0.0)
            raise


ai_handler = AIHandler()
