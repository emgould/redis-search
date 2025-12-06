"""
Tests for News search service (search.py only).
Tests cover all methods defined in search.py:
- get_trending_news
- search_news
- get_news_sources
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from api.news.search import NewsSearchService

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filepath: str) -> dict:
    """Load a JSON fixture file."""
    with open(FIXTURES_DIR / filepath) as f:
        return json.load(f)


class TestNewsSearchService:
    """Tests for NewsSearchService."""

    @pytest.fixture
    def service(self, mock_newsapi_key):
        """Create a search service instance."""
        from unittest.mock import PropertyMock

        with patch("api.news.auth.Auth.news_api_key", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_newsapi_key
            service = NewsSearchService()
            # Trigger property access to initialize newsapi
            _ = service.newsapi
            yield service

    @pytest.fixture
    def mock_trending_response(self):
        """Mock NewsAPI trending response."""
        return load_fixture("make_requests/trending_news_us.json")

    @pytest.fixture
    def mock_search_response(self):
        """Mock NewsAPI search response."""
        return load_fixture("make_requests/search_news_ai.json")

    @pytest.fixture
    def mock_sources_response(self):
        """Mock NewsAPI sources response."""
        return load_fixture("make_requests/news_sources_en.json")

    def test_service_initialization(self, service):
        """Test service initialization."""
        assert service.newsapi is not None

    @pytest.mark.asyncio
    async def test_get_trending_news_basic(self, service, mock_trending_response):
        """Test basic trending news retrieval."""
        with patch.object(
            service.newsapi, "get_top_headlines", return_value=mock_trending_response
        ):
            result = await service.get_trending_news(country="us", page_size=10)

        assert result.status == "ok"
        assert result.country == "us"
        assert len(result.results) > 0
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_get_trending_news_with_category(self, service, mock_trending_response):
        """Test trending news with category filter."""
        with patch.object(
            service.newsapi, "get_top_headlines", return_value=mock_trending_response
        ):
            result = await service.get_trending_news(
                country="us", category="technology", page_size=10
            )

        assert result.status == "ok"
        assert result.category == "technology"
        assert result.country == "us"

    @pytest.mark.asyncio
    async def test_get_trending_news_with_query(self, service, mock_trending_response):
        """Test trending news with query filter."""
        with patch.object(
            service.newsapi, "get_top_headlines", return_value=mock_trending_response
        ):
            result = await service.get_trending_news(country="us", query="technology", page_size=10)

        assert result.status == "ok"
        assert result.query == "technology"

    @pytest.mark.asyncio
    async def test_get_trending_news_with_pagination(self, service, mock_trending_response):
        """Test trending news with pagination."""
        with patch.object(
            service.newsapi, "get_top_headlines", return_value=mock_trending_response
        ):
            result = await service.get_trending_news(country="us", page_size=10, page=2)

        assert result.status == "ok"
        assert result.page == 2

    @pytest.mark.asyncio
    async def test_get_trending_news_page_size_limit(self, service, mock_trending_response):
        """Test that page_size is limited to 100."""
        from unittest.mock import MagicMock

        mock_method = MagicMock(return_value=mock_trending_response)
        with patch.object(service.newsapi, "get_top_headlines", mock_method):
            result = await service.get_trending_news(country="us", page_size=200)

        assert result.status == "ok"
        # Verify that the API was called with max 100
        mock_method.assert_called_once()
        call_kwargs = mock_method.call_args[1]
        assert call_kwargs["page_size"] == 100

    @pytest.mark.asyncio
    async def test_get_trending_news_api_error(self, service):
        """Test trending news with API error."""
        from newsapi.newsapi_exception import NewsAPIException

        with patch.object(
            service.newsapi, "get_top_headlines", side_effect=NewsAPIException("API Error")
        ):
            result = await service.get_trending_news(country="us")

        assert result.status == "error"
        assert len(result.results) == 0
        assert result.total_results == 0

    @pytest.mark.asyncio
    async def test_search_news_basic(self, service, mock_search_response):
        """Test basic news search."""
        with patch.object(service.newsapi, "get_everything", return_value=mock_search_response):
            result = await service.search_news(query="artificial intelligence", page_size=10)

        assert result.status == "ok"
        assert result.query == "artificial intelligence"
        assert len(result.results) > 0
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_search_news_with_language(self, service, mock_search_response):
        """Test news search with language filter."""
        with patch.object(service.newsapi, "get_everything", return_value=mock_search_response):
            result = await service.search_news(query="technology", language="en", page_size=10)

        assert result.status == "ok"
        assert result.language == "en"

    @pytest.mark.asyncio
    async def test_search_news_with_sort_by(self, service, mock_search_response):
        """Test news search with sort_by parameter."""
        with patch.object(service.newsapi, "get_everything", return_value=mock_search_response):
            result = await service.search_news(
                query="technology", sort_by="popularity", page_size=10
            )

        assert result.status == "ok"
        assert result.sort_by == "popularity"

    @pytest.mark.asyncio
    async def test_search_news_with_dates(self, service, mock_search_response):
        """Test news search with date filters."""
        with patch.object(service.newsapi, "get_everything", return_value=mock_search_response):
            result = await service.search_news(
                query="technology",
                from_date="2024-01-01",
                to_date="2024-01-31",
                page_size=10,
            )

        assert result.status == "ok"
        assert result.from_date == "2024-01-01"
        assert result.to_date == "2024-01-31"

    @pytest.mark.asyncio
    async def test_search_news_page_size_limit(self, service, mock_search_response):
        """Test that page_size is limited to 100."""
        from unittest.mock import MagicMock

        mock_method = MagicMock(return_value=mock_search_response)
        with patch.object(service.newsapi, "get_everything", mock_method):
            result = await service.search_news(query="test", page_size=200)

        assert result.status == "ok"
        # Verify that the API was called with max 100
        mock_method.assert_called_once()
        call_kwargs = mock_method.call_args[1]
        assert call_kwargs["page_size"] == 100

    @pytest.mark.asyncio
    async def test_search_news_api_error(self, service):
        """Test news search with API error."""
        from newsapi.newsapi_exception import NewsAPIException

        with patch.object(
            service.newsapi, "get_everything", side_effect=NewsAPIException("API Error")
        ):
            result = await service.search_news(query="test")

        assert result.status == "error"
        assert len(result.results) == 0
        assert result.total_results == 0

    @pytest.mark.asyncio
    async def test_get_news_sources_basic(self, service, mock_sources_response):
        """Test basic news sources retrieval."""
        with patch.object(service.newsapi, "get_sources", return_value=mock_sources_response):
            result = await service.get_news_sources()

        assert result.status == "ok"
        assert len(result.results) > 0
        assert result.total_sources > 0
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_get_news_sources_with_category(self, service, mock_sources_response):
        """Test news sources with category filter."""
        with patch.object(service.newsapi, "get_sources", return_value=mock_sources_response):
            result = await service.get_news_sources(category="technology")

        assert result.status == "ok"
        assert result.category == "technology"

    @pytest.mark.asyncio
    async def test_get_news_sources_with_language(self, service, mock_sources_response):
        """Test news sources with language filter."""
        with patch.object(service.newsapi, "get_sources", return_value=mock_sources_response):
            result = await service.get_news_sources(language="en")

        assert result.status == "ok"
        assert result.language == "en"

    @pytest.mark.asyncio
    async def test_get_news_sources_with_country(self, service, mock_sources_response):
        """Test news sources with country filter."""
        with patch.object(service.newsapi, "get_sources", return_value=mock_sources_response):
            result = await service.get_news_sources(country="us")

        assert result.status == "ok"
        assert result.country == "us"

    @pytest.mark.asyncio
    async def test_get_news_sources_with_all_filters(self, service, mock_sources_response):
        """Test news sources with all filters."""
        with patch.object(service.newsapi, "get_sources", return_value=mock_sources_response):
            result = await service.get_news_sources(
                category="technology", language="en", country="us"
            )

        assert result.status == "ok"
        assert result.category == "technology"
        assert result.language == "en"
        assert result.country == "us"

    @pytest.mark.asyncio
    async def test_get_news_sources_api_error(self, service):
        """Test news sources with API error."""
        from newsapi.newsapi_exception import NewsAPIException

        with patch.object(
            service.newsapi, "get_sources", side_effect=NewsAPIException("API Error")
        ):
            result = await service.get_news_sources()

        assert result.status == "error"
        assert len(result.results) == 0
        assert result.total_sources == 0
        assert result.total_results == 0
