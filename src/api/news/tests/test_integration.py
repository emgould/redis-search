"""
Integration tests for News service.
These tests hit the actual NewsAPI endpoints (no mocks).

Requirements:
- Internet connection required
- NEWS_API_KEY environment variable required

Run with: pytest api/news/tests/test_integration.py -v
"""

import json
from unittest.mock import MagicMock

import pytest
from contracts.models import MCSources, MCSubType, MCType
from firebase_functions import https_fn

from api.news.handlers import NewsHandler
from api.news.models import MCNewsItem
from api.news.wrappers import news_wrapper, search_person_async
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def news_handler():
    """Create NewsHandler instance."""
    return NewsHandler()


@pytest.fixture
def mock_request():
    """Create a mock Firebase Functions Request object."""

    def _create_mock_request(args: dict[str, str | None] | None = None):
        mock_req = MagicMock(spec=https_fn.Request)
        # Make args support .get() method like a dict
        args_dict = args or {}
        mock_req.args = MagicMock()
        mock_req.args.get = lambda key, default=None: args_dict.get(key, default)
        return mock_req

    return _create_mock_request


class TestHandlers:
    """Integration tests for all News handlers."""

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_basic(self, news_handler, mock_request):
        """Test get_trending_news handler with basic parameters."""
        req = mock_request({"country": "us", "page_size": "10"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        assert data["country"] == "us"
        # Write snapshot for visual inspection
        write_snapshot(data, "get_trending_news_handler_basic_result.json")

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_with_category(self, news_handler, mock_request):
        """Test get_trending_news handler with category parameter."""
        req = mock_request({"country": "us", "category": "technology", "page_size": "5"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["category"] == "technology"
        # Write snapshot for visual inspection
        write_snapshot(data, "get_trending_news_handler_with_category_result.json")

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_with_query(self, news_handler, mock_request):
        """Test get_trending_news handler with query parameter."""
        req = mock_request({"country": "us", "query": "technology", "page_size": "5"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        # Write snapshot for visual inspection
        write_snapshot(data, "get_trending_news_handler_with_query_result.json")

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_with_pagination(self, news_handler, mock_request):
        """Test get_trending_news handler with pagination."""
        req = mock_request({"country": "us", "page_size": "5", "page": "2"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["page"] == 2

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_invalid_page_size(self, news_handler, mock_request):
        """Test get_trending_news handler with invalid page_size."""
        req = mock_request({"country": "us", "page_size": "101"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_get_trending_news_handler_invalid_country(self, news_handler, mock_request):
        """Test get_trending_news handler with invalid country code."""
        req = mock_request({"country": "invalid", "page_size": "10"})
        response = await news_handler.get_trending_news(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_search_news_handler_basic(self, news_handler, mock_request):
        """Test search_news handler with basic parameters."""
        req = mock_request({"query": "artificial intelligence", "page_size": "10"})
        response = await news_handler.search_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        assert data["query"] == "artificial intelligence"
        # Write snapshot for visual inspection
        write_snapshot(data, "search_news_handler_basic_result.json")

    @pytest.mark.asyncio
    async def test_search_news_handler_with_language(self, news_handler, mock_request):
        """Test search_news handler with language parameter."""
        req = mock_request({"query": "technology", "language": "en", "page_size": "5"})
        response = await news_handler.search_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["language"] == "en"

    @pytest.mark.asyncio
    async def test_search_news_handler_with_sort_by(self, news_handler, mock_request):
        """Test search_news handler with sort_by parameter."""
        req = mock_request({"query": "technology", "sort_by": "popularity", "page_size": "5"})
        response = await news_handler.search_news(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["sort_by"] == "popularity"

    @pytest.mark.asyncio
    async def test_search_news_handler_no_query(self, news_handler, mock_request):
        """Test search_news handler without query parameter."""
        req = mock_request({"page_size": "10"})
        response = await news_handler.search_news(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_search_news_handler_invalid_sort_by(self, news_handler, mock_request):
        """Test search_news handler with invalid sort_by."""
        req = mock_request({"query": "test", "sort_by": "invalid", "page_size": "10"})
        response = await news_handler.search_news(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_get_news_sources_handler_basic(self, news_handler, mock_request):
        """Test get_news_sources handler with basic parameters."""
        req = mock_request({})
        response = await news_handler.get_news_sources(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0

    @pytest.mark.asyncio
    async def test_get_news_sources_handler_with_category(self, news_handler, mock_request):
        """Test get_news_sources handler with category parameter."""
        req = mock_request({"category": "technology"})
        response = await news_handler.get_news_sources(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["category"] == "technology"
        # Write snapshot for visual inspection
        write_snapshot(data, "get_news_sources_handler_with_category_result.json")

    @pytest.mark.asyncio
    async def test_get_news_sources_handler_with_language(self, news_handler, mock_request):
        """Test get_news_sources handler with language parameter."""
        req = mock_request({"language": "en"})
        response = await news_handler.get_news_sources(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["language"] == "en"

    @pytest.mark.asyncio
    async def test_get_news_sources_handler_with_country(self, news_handler, mock_request):
        """Test get_news_sources handler with country parameter."""
        req = mock_request({"country": "us"})
        response = await news_handler.get_news_sources(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert data["country"] == "us"


class TestWrappers:
    """Integration tests for all News wrappers."""

    @pytest.mark.asyncio
    async def test_get_trending_news_basic(self):
        """Test news_wrapper.get_trending_news with basic parameters."""
        result = await news_wrapper.get_trending_news(country="us", page_size=10)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.country == "us"
        assert result.error is None
        # Write snapshot for visual inspection
        write_snapshot(result.model_dump(), "get_trending_news_basic_result.json")

    @pytest.mark.asyncio
    async def test_get_trending_news_with_category(self):
        """Test news_wrapper.get_trending_news with category parameter."""
        result = await news_wrapper.get_trending_news(
            country="us", category="technology", page_size=5
        )

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.category == "technology"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_get_trending_news_with_query(self):
        """Test news_wrapper.get_trending_news with query parameter."""
        result = await news_wrapper.get_trending_news(country="us", query="technology", page_size=5)

        assert result.status_code == 200
        assert result is not None
        assert result.query == "technology"
        assert result.error is None
        # Note: API may return 0 results for some queries, which is valid

    @pytest.mark.asyncio
    async def test_get_trending_news_with_pagination(self):
        """Test news_wrapper.get_trending_news with pagination."""
        result = await news_wrapper.get_trending_news(country="us", page_size=5, page=2)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.page == 2
        assert result.error is None

    @pytest.mark.asyncio
    async def test_search_news_basic(self):
        """Test news_wrapper.search_news with basic parameters."""
        result = await news_wrapper.search_news(query="artificial intelligence", page_size=10)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.query == "artificial intelligence"
        assert result.error is None
        # Write snapshot for visual inspection
        write_snapshot(result.model_dump(), "search_news_basic_result.json")

    @pytest.mark.asyncio
    async def test_search_news_with_language(self):
        """Test news_wrapper.search_news with language parameter."""
        result = await news_wrapper.search_news(query="technology", language="en", page_size=5)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.language == "en"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_search_news_with_sort_by(self):
        """Test news_wrapper.search_news with sort_by parameter."""
        result = await news_wrapper.search_news(
            query="technology", sort_by="popularity", page_size=5
        )

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.sort_by == "popularity"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_get_news_sources_basic(self):
        """Test news_wrapper.get_news_sources with basic parameters."""
        result = await news_wrapper.get_news_sources()

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.error is None
        # Write snapshot for visual inspection
        write_snapshot(result.model_dump(), "get_news_sources_basic_result.json")

    @pytest.mark.asyncio
    async def test_get_news_sources_with_category(self):
        """Test news_wrapper.get_news_sources with category parameter."""
        result = await news_wrapper.get_news_sources(category="technology")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.category == "technology"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_get_news_sources_with_language(self):
        """Test news_wrapper.get_news_sources with language parameter."""
        result = await news_wrapper.get_news_sources(language="en")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.language == "en"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_get_news_sources_with_country(self):
        """Test news_wrapper.get_news_sources with country parameter."""
        result = await news_wrapper.get_news_sources(country="us")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.country == "us"
        assert result.error is None


class TestWrapperRequiredFields:
    """Integration tests for required fields in wrapper responses."""

    @pytest.mark.asyncio
    async def test_trending_news_has_required_fields(self):
        """Test that trending news results have mc_id, mc_type, source, and source_id."""
        result = await news_wrapper.get_trending_news(country="us", page_size=10)

        assert result.status_code == 200
        assert len(result.results) > 0

        for article in result.results:
            # Verify required fields are present and not None/empty
            assert article.mc_id, f"mc_id is missing or empty for article: {article.title}"
            assert article.mc_type, f"mc_type is missing or empty for article: {article.title}"
            assert article.source, f"source is missing or empty for article: {article.title}"
            assert article.source_id, f"source_id is missing or empty for article: {article.title}"

            # Verify correct values
            assert article.mc_type == "news_article"
            assert article.source.value == "newsapi"

    @pytest.mark.asyncio
    async def test_search_news_has_required_fields(self):
        """Test that search news results have mc_id, mc_type, source, and source_id."""
        result = await news_wrapper.search_news(query="technology", page_size=10)

        assert result.status_code == 200
        assert len(result.results) > 0

        for article in result.results:
            # Verify required fields are present and not None/empty
            assert article.mc_id, f"mc_id is missing or empty for article: {article.title}"
            assert article.mc_type, f"mc_type is missing or empty for article: {article.title}"
            assert article.source, f"source is missing or empty for article: {article.title}"
            assert article.source_id, f"source_id is missing or empty for article: {article.title}"

            # Verify correct values
            assert article.mc_type == "news_article"
            assert article.source.value == "newsapi"

    @pytest.mark.asyncio
    async def test_news_sources_has_required_fields(self):
        """Test that news sources results have mc_id, mc_type, source, and source_id."""
        result = await news_wrapper.get_news_sources()

        assert result.status_code == 200
        assert len(result.results) > 0

        for source_item in result.results:
            # Verify required fields are present and not None/empty
            assert source_item.mc_id, f"mc_id is missing or empty for source: {source_item.name}"
            assert source_item.mc_type, (
                f"mc_type is missing or empty for source: {source_item.name}"
            )
            assert source_item.source, f"source is missing or empty for source: {source_item.name}"
            assert source_item.source_id, (
                f"source_id is missing or empty for source: {source_item.name}"
            )

            # Verify correct values
            assert source_item.mc_type == "news_article"
            assert source_item.source.value == "newsapi"

    @pytest.mark.asyncio
    async def test_search_person_async(self):
        """Test search_person_async wrapper function."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request for a known news author
        # Using a common author name that should return results
        person_request = MCPersonSearchRequest(
            source_id="123",  # Not used for NewsAPI (uses name)
            source=MCSources.NEWSAPI,
            mc_type=MCType.PERSON,
            mc_id="author_david_leonhardt",
            mc_subtype=MCSubType.AUTHOR,
            name="David Leonhardt",  # Well-known NYT columnist
        )

        # Call the wrapper function
        result = await search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # For NewsAPI, details may be None (no author model)
        # But works should contain articles

        # Validate works array contains articles
        # Note: May be empty if no articles found, but structure should be valid
        if len(result.works) > 0:
            for work in result.works:
                work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
                item_validated = MCNewsItem.model_validate(work_dict)
                assert item_validated.mc_type == MCType.NEWS_ARTICLE
                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for article: {item_validated.title}"
                )
                assert item_validated.mc_type == MCType.NEWS_ARTICLE, (
                    f"mc_type is wrong for article: {item_validated.title}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for article: {item_validated.title}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for article: {item_validated.title}"
                )
                # Verify author field matches (case-insensitive partial match)
                if item_validated.author:
                    assert (
                        "leonhardt" in item_validated.author.lower()
                        or "david" in item_validated.author.lower()
                    )

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_news.json")

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source(self):
        """Test search_person_async with non-NewsAPI source - wrapper accepts any source."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with non-NewsAPI source
        # News wrapper doesn't filter by source - it searches by name regardless
        person_request = MCPersonSearchRequest(
            source_id="123",
            source=MCSources.TMDB,  # Not NewsAPI, but wrapper accepts it
            mc_type=MCType.PERSON,
            mc_id="author_123",
            mc_subtype=MCSubType.AUTHOR,
            name="Test Author",
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate that wrapper accepts any source and searches by name
        # Will return 200 (with results) or 404 (no articles found), not 400
        assert result.status_code in [200, 404]
        assert result.input == person_request
        assert result.details is None
        assert result.related == []
        # Works may be empty if no articles match the author name

    @pytest.mark.asyncio
    async def test_search_person_async_missing_name(self):
        """Test search_person_async with missing author name."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request without name
        person_request = MCPersonSearchRequest(
            source_id="123",
            source=MCSources.NEWSAPI,
            mc_type=MCType.PERSON,
            mc_id="author_123",
            mc_subtype=MCSubType.AUTHOR,
            name="",  # Empty name
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "name" in result.error.lower()
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_author_not_found(self):
        """Test search_person_async with non-existent author."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with non-existent author name
        person_request = MCPersonSearchRequest(
            source_id="123",
            source=MCSources.NEWSAPI,
            mc_type=MCType.PERSON,
            mc_id="author_nonexistent",
            mc_subtype=MCSubType.AUTHOR,
            name="XyZqWrTpLmN123456789",  # Very unlikely to exist
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # For NewsAPI, if no articles found, works will be empty but status may still be 200
        # Or it could return 404/500 if API error occurs
        assert result.status_code in [200, 404, 500]
        # Works may be empty if no articles found
        assert result.related == []
