"""
Unit tests for News service wrappers.
Tests Firebase Functions compatible async wrappers using ApiWrapperResponse pattern.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from api.news.models import MCNewsItem, NewsSearchResponse, NewsSource, TrendingNewsResponse
from api.news.wrappers import news_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_trending_response():
    """Mock trending news response data."""
    return {
        "status": "ok",
        "totalResults": 2,
        "articles": [
            {
                "source": {"id": "bbc-news", "name": "BBC News"},
                "author": "Test Author 1",
                "title": "Breaking News 1",
                "description": "Description 1",
                "url": "https://example.com/article1",
                "urlToImage": "https://example.com/image1.jpg",
                "publishedAt": "2024-01-15T10:30:00Z",
                "content": "Content 1...",
            },
        ],
    }


@pytest.mark.asyncio
async def test_get_trending_news(mock_newsapi_key, mock_trending_response):
    """Test news_wrapper.get_trending_news wrapper."""
    with patch(
        "api.news.search.NewsSearchService.get_trending_news", new_callable=AsyncMock
    ) as mock_method:
        # Setup mock
        source = NewsSource(name="BBC News")
        article = MCNewsItem(title="Test", url="https://example.com", news_source=source)
        mock_response = TrendingNewsResponse(
            results=[article], total_results=1, country="us", status="ok"
        )
        mock_method.return_value = mock_response

        # Call wrapper
        result = await news_wrapper.get_trending_news(country="us")

        # Verify
        assert isinstance(result, TrendingNewsResponse)
        assert result.status_code == 200
        assert result.total_results == 1
        assert result.country == "us"
        assert len(result.results) == 1
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify MCBaseItem fields are present in results
        assert len(result.results) > 0
        article = result.results[0]
        assert article.mc_id is not None
        assert article.mc_type == "news_article"

        # Write snapshot (exclude dynamic fields)
        result_dict = result.model_dump()
        result_copy = result_dict.copy()
        result_copy.pop("fetched_at", None)
        # Remove mc_id from results as it's hash-based and changes
        if "results" in result_copy:
            for article_item in result_copy["results"]:
                article_item.pop("mc_id", None)
        write_snapshot(
            json.dumps(result_copy, indent=2, sort_keys=True),
            "get_trending_news_result.json",
        )


@pytest.mark.asyncio
async def test_search_news(mock_newsapi_key):
    """Test news_wrapper.search_news wrapper."""
    with patch(
        "api.news.search.NewsSearchService.search_news", new_callable=AsyncMock
    ) as mock_method:
        # Setup mock
        source = NewsSource(name="TechCrunch")
        article = MCNewsItem(title="AI News", url="https://example.com", news_source=source)
        mock_response = NewsSearchResponse(
            results=[article], total_results=1, query="AI", status="ok"
        )
        mock_method.return_value = mock_response

        # Call wrapper
        result = await news_wrapper.search_news(query="AI")

        # Verify
        assert isinstance(result, NewsSearchResponse)
        assert result.status_code == 200
        assert result.query == "AI"
        assert result.total_results == 1
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify MCBaseItem fields are present in results
        assert len(result.results) > 0
        article = result.results[0]
        assert article.mc_id is not None
        assert article.mc_type == "news_article"

        # Write snapshot (exclude dynamic fields)
        result_dict = result.model_dump()
        result_copy = result_dict.copy()
        result_copy.pop("fetched_at", None)
        # Remove mc_id from results as it's hash-based and changes
        if "results" in result_copy:
            for article_item in result_copy["results"]:
                article_item.pop("mc_id", None)
        write_snapshot(json.dumps(result_copy, indent=2, sort_keys=True), "search_news_result.json")


@pytest.mark.asyncio
async def test_get_news_sources(mock_newsapi_key):
    """Test news_wrapper.get_news_sources wrapper."""
    with patch(
        "api.news.search.NewsSearchService.get_news_sources", new_callable=AsyncMock
    ) as mock_method:
        # Setup mock
        from api.news.models import NewsSourceDetails, NewsSourcesResponse

        source = NewsSourceDetails(id="bbc-news", name="BBC News")
        mock_response = NewsSourcesResponse(
            results=[source], total_results=1, total_sources=1, status="ok"
        )
        mock_method.return_value = mock_response

        # Call wrapper
        result = await news_wrapper.get_news_sources()

        # Verify
        assert isinstance(result, NewsSourcesResponse)
        assert result.status_code == 200
        assert result.total_sources == 1
        assert len(result.results) == 1
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Write snapshot
        result_dict = result.model_dump()
        # Remove mc_id from results as it's dynamic
        if "results" in result_dict:
            for item in result_dict["results"]:
                item.pop("mc_id", None)
        write_snapshot(
            json.dumps(result_dict, indent=2, sort_keys=True), "get_news_sources_result.json"
        )


@pytest.mark.asyncio
async def test_wrapper_returns_mcsearchresponse(mock_newsapi_key):
    """Test that wrappers return MCSearchResponse derivative."""
    with patch(
        "api.news.search.NewsSearchService.get_trending_news", new_callable=AsyncMock
    ) as mock_method:
        source = NewsSource(name="Test")
        article = MCNewsItem(title="Test", url="https://example.com", news_source=source)
        mock_response = TrendingNewsResponse(
            results=[article], total_results=1, country="us", status="ok"
        )
        mock_method.return_value = mock_response

        result = await news_wrapper.get_trending_news()

        # Verify it's a TrendingNewsResponse (MCSearchResponse derivative)
        assert isinstance(result, TrendingNewsResponse)
        assert result.status_code == 200
        assert result.data_type is not None
        assert result.data_source is not None


@pytest.mark.asyncio
async def test_wrapper_error_handling(mock_newsapi_key):
    """Test that wrappers return error responses properly."""
    with patch(
        "api.news.search.NewsSearchService.get_trending_news", new_callable=AsyncMock
    ) as mock_method:
        # Mock service to return error response
        mock_response = TrendingNewsResponse(
            results=[],
            total_results=0,
            country="us",
            status="error",
            error="API key invalid",
        )
        mock_method.return_value = mock_response

        result = await news_wrapper.get_trending_news(country="us")

        # Verify error response
        assert isinstance(result, TrendingNewsResponse)
        assert result.status_code == 500
        assert result.error == "Failed to fetch trending news"
        assert result.data_type is not None


@pytest.mark.asyncio
async def test_wrapper_exception_handling(mock_newsapi_key):
    """Test that wrappers handle exceptions properly."""
    with patch(
        "api.news.search.NewsSearchService.get_trending_news", new_callable=AsyncMock
    ) as mock_method:
        # Mock service to raise exception
        mock_method.side_effect = Exception("Test exception")

        result = await news_wrapper.get_trending_news(country="us")

        # Verify error response
        assert isinstance(result, TrendingNewsResponse)
        assert result.status_code == 500
        assert result.error == "Test exception"
        # Verify it's a TrendingNewsResponse structure
        assert result.results == []
        assert result.total_results == 0
        assert result.data_type is not None


class TestWrapperRequiredFields:
    """Tests for required fields in wrapper responses."""

    @pytest.mark.asyncio
    async def test_trending_news_has_required_fields(self, mock_newsapi_key):
        """Test that trending news results have mc_id, mc_type, source, and source_id."""
        with patch(
            "api.news.search.NewsSearchService.get_trending_news", new_callable=AsyncMock
        ) as mock_method:
            # Setup mock with explicit source_id
            source = NewsSource(name="BBC News")
            articles = [
                MCNewsItem(
                    title="Article 1",
                    url="https://example.com/article1",
                    news_source=source,
                    source_id="article1",
                ),
                MCNewsItem(
                    title="Article 2",
                    url="https://example.com/article2",
                    news_source=source,
                    source_id="article2",
                ),
            ]
            mock_response = TrendingNewsResponse(
                results=articles, total_results=2, country="us", status="ok"
            )
            mock_method.return_value = mock_response

            result = await news_wrapper.get_trending_news(country="us")

            assert len(result.results) == 2
            for article in result.results:
                # Verify required fields are present and not None/empty
                assert article.mc_id, f"mc_id is missing or empty for article: {article.title}"
                assert article.mc_type, f"mc_type is missing or empty for article: {article.title}"
                assert article.source, f"source is missing or empty for article: {article.title}"
                assert article.source_id, (
                    f"source_id is missing or empty for article: {article.title}"
                )

                # Verify correct values
                assert article.mc_type == "news_article"
                assert article.source.value == "newsapi"

    @pytest.mark.asyncio
    async def test_search_news_has_required_fields(self, mock_newsapi_key):
        """Test that search news results have mc_id, mc_type, source, and source_id."""
        with patch(
            "api.news.search.NewsSearchService.search_news", new_callable=AsyncMock
        ) as mock_method:
            # Setup mock with explicit source_id
            source = NewsSource(name="TechCrunch")
            articles = [
                MCNewsItem(
                    title="AI Article 1",
                    url="https://example.com/ai1",
                    news_source=source,
                    source_id="ai1",
                ),
                MCNewsItem(
                    title="AI Article 2",
                    url="https://example.com/ai2",
                    news_source=source,
                    source_id="ai2",
                ),
            ]
            mock_response = NewsSearchResponse(
                results=articles, total_results=2, query="AI", status="ok"
            )
            mock_method.return_value = mock_response

            result = await news_wrapper.search_news(query="AI")

            assert len(result.results) == 2
            for article in result.results:
                # Verify required fields are present and not None/empty
                assert article.mc_id, f"mc_id is missing or empty for article: {article.title}"
                assert article.mc_type, f"mc_type is missing or empty for article: {article.title}"
                assert article.source, f"source is missing or empty for article: {article.title}"
                assert article.source_id, (
                    f"source_id is missing or empty for article: {article.title}"
                )

                # Verify correct values
                assert article.mc_type == "news_article"
                assert article.source.value == "newsapi"

    @pytest.mark.asyncio
    async def test_news_sources_has_required_fields(self, mock_newsapi_key):
        """Test that news sources results have mc_id, mc_type, source, and source_id."""
        with patch(
            "api.news.search.NewsSearchService.get_news_sources", new_callable=AsyncMock
        ) as mock_method:
            # Setup mock with explicit source_id
            from api.news.models import NewsSourceDetails, NewsSourcesResponse

            sources = [
                NewsSourceDetails(id="bbc-news", name="BBC News", source_id="bbc-news"),
                NewsSourceDetails(id="cnn", name="CNN", source_id="cnn"),
            ]
            mock_response = NewsSourcesResponse(
                results=sources, total_results=2, total_sources=2, status="ok"
            )
            mock_method.return_value = mock_response

            result = await news_wrapper.get_news_sources()

            assert len(result.results) == 2
            for source_item in result.results:
                # Verify required fields are present and not None/empty
                assert source_item.mc_id, (
                    f"mc_id is missing or empty for source: {source_item.name}"
                )
                assert source_item.mc_type, (
                    f"mc_type is missing or empty for source: {source_item.name}"
                )
                assert source_item.source, (
                    f"source is missing or empty for source: {source_item.name}"
                )
                assert source_item.source_id, (
                    f"source_id is missing or empty for source: {source_item.name}"
                )

                # Verify correct values
                assert source_item.mc_type == "news_article"
                assert source_item.source.value == "newsapi"
