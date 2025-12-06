"""
NewsAI Core Service - Base service for NewsAI (Event Registry) API operations
Handles core API communication and article processing.
"""

from datetime import datetime

from api.newsai.auth import Auth
from api.newsai.event_models import MCEventItem
from api.newsai.models import MCNewsItem, NewsSource
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger

logger = get_logger(__name__)


class NewsAIService(Auth, BaseAPIClient):
    """
    Core NewsAI service for Event Registry API operations.
    Provides foundation for search and other operations.

    Rate Limits (from Event Registry documentation):
    - Free tier: 2,000 requests/day, 10 requests/second
    - Paid tiers: Higher limits based on plan
    """

    BASE_URL = "https://eventregistry.org"

    def __init__(self):
        """Initialize NewsAI service. API keys are accessed from secrets at runtime."""
        super().__init__()

    def _process_event_item(self, event_data: dict) -> MCEventItem:
        """
        Process and normalize an event item from Event Registry.

        Args:
            event_data: Raw event data from Event Registry

        Returns:
            MCEventItem with standardized fields
        """
        try:
            # Create event with Pydantic model - let the model handle field mapping
            event = MCEventItem.model_validate(event_data)
            return event
        except Exception as e:
            logger.error(f"Error processing event item: {e}")
            # Return minimal valid event
            return MCEventItem(  # type: ignore[call-arg]
                uri=event_data.get("uri", "error"),
                title=event_data.get("title", "Error processing event"),
                event_date=event_data.get("eventDate", datetime.now().strftime("%Y-%m-%d")),
                total_article_count=event_data.get("totalArticleCount", 0),
            )

    def _process_article_item(self, article_data: dict) -> MCNewsItem:
        """
        Process and normalize an article item from Event Registry.

        Args:
            article_data: Raw article data from Event Registry

        Returns:
            MCNewsItem with standardized fields
        """
        try:
            # Extract source information
            source_data = article_data.get("source", {})
            # Handle empty string case - if name is empty or None, use default
            source_name = source_data.get("title") or source_data.get("uri") or "Unknown Source"
            if isinstance(source_name, str) and source_name.strip() == "":
                source_name = "Unknown Source"

            source_id = source_data.get("uri")
            source = NewsSource(id=source_id, name=source_name)

            # Process published date - Event Registry provides date, time, and dateTime
            published_at = article_data.get("dateTime") or article_data.get("date")
            if published_at:
                try:
                    # Parse ISO format date if needed
                    if "T" in published_at:
                        pub_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        published_at = pub_date.isoformat()
                except Exception:
                    # Keep original if parsing fails
                    pass

            # Extract image URL
            url_to_image = article_data.get("image")

            # Extract description/body
            description = article_data.get("body") or article_data.get("title")
            if description and len(description) > 500:
                # Truncate long descriptions
                description = description[:497] + "..."

            # Process authors - Event Registry returns a list, but we need a string
            authors = article_data.get("authors")
            author_str = None
            if authors and isinstance(authors, list) and len(authors) > 0:
                # Join multiple authors with commas
                author_str = ", ".join(str(a) for a in authors if a)
            elif isinstance(authors, str):
                author_str = authors

            # Create article with Pydantic model
            article = MCNewsItem(  # type: ignore[call-arg]
                uri=article_data.get("uri"),
                title=article_data.get("title", ""),
                description=description,
                content=article_data.get("body"),
                url=article_data.get("url", ""),
                url_to_image=url_to_image,
                published_at=published_at,
                author=author_str,
                news_source=source,
                lang=article_data.get("lang"),
                is_duplicate=article_data.get("isDuplicate"),
                date=article_data.get("date"),
                time=article_data.get("time"),
                date_time=article_data.get("dateTime"),
                sim=article_data.get("sim"),
                sentiment=article_data.get("sentiment"),
                wgt=article_data.get("wgt"),
                relevance=article_data.get("relevance"),
            )

            return article

        except Exception as e:
            # Log detailed error information
            import traceback

            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "article_keys": list(article_data.keys()) if article_data else [],
                "article_title": article_data.get("title", "N/A") if article_data else "N/A",
                "article_url": article_data.get("url", "N/A") if article_data else "N/A",
            }
            logger.error(f"Error processing article item: {e}", extra=error_details)

            # Return minimal valid article with safe defaults
            return MCNewsItem(  # type: ignore[call-arg]
                title=article_data.get("title") or "Error processing article",
                description="Error processing article data",
                url=article_data.get("url") or "https://error.invalid",
                news_source=NewsSource(name="Unknown Source"),
            )
