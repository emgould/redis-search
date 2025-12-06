"""
Unit tests for NewsAI models.
Tests Pydantic model validation and field mapping.
"""

import pytest

from api.newsai.models import (
    MCNewsItem,
    NewsSearchResponse,
    NewsSource,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from contracts.models import MCSources, MCType

pytestmark = pytest.mark.unit


def test_news_source_model():
    """Test NewsSource model creation."""
    source = NewsSource(id="test-source", name="Test Source")
    assert source.id == "test-source"
    assert source.name == "Test Source"


def test_mc_news_item_basic():
    """Test MCNewsItem model with basic fields."""
    source = NewsSource(name="BBC News")
    article = MCNewsItem(
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
    )

    assert article.title == "Test Article"
    assert article.url == "https://example.com/article"
    assert article.news_source.name == "BBC News"
    assert article.mc_type == MCType.NEWS_ARTICLE
    assert article.source == MCSources.NEWSAI
    assert article.mc_id is not None
    assert article.source_id is not None


def test_mc_news_item_with_uri():
    """Test MCNewsItem model with Event Registry URI."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(
        uri="123456-abcd-test",
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
    )

    assert article.uri == "123456-abcd-test"
    assert article.source_id == "123456-abcd-test"
    assert article.mc_id is not None


def test_mc_news_item_date_fields():
    """Test MCNewsItem model with date/time fields."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
        date="2024-01-15",
        time="10:30:00",
    )

    assert article.date == "2024-01-15"
    assert article.time == "10:30:00"
    assert article.published_at == "2024-01-15T10:30:00"


def test_mc_news_item_datetime_field():
    """Test MCNewsItem model with dateTime field."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
        date_time="2024-01-15T10:30:00Z",
    )

    assert article.date_time == "2024-01-15T10:30:00Z"
    assert article.published_at == "2024-01-15T10:30:00Z"


def test_news_source_details():
    """Test NewsSourceDetails model."""
    source = NewsSourceDetails(
        uri="test-source-uri",
        name="Test Source",
        description="A test news source",
        url="https://example.com",
        category="news",
        language="eng",
        country="US",
    )

    assert source.uri == "test-source-uri"
    assert source.name == "Test Source"
    assert source.description == "A test news source"
    assert source.mc_type == MCType.NEWS_ARTICLE
    assert source.source == MCSources.NEWSAI
    assert source.mc_id is not None
    assert source.source_id == "test-source-uri"


def test_trending_news_response():
    """Test TrendingNewsResponse model."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
    )

    response = TrendingNewsResponse(
        results=[article],
        total_results=1,
        country="us",
        query="test",
        category="technology",
        page=1,
        page_size=20,
        status="ok",
    )

    assert len(response.results) == 1
    assert response.total_results == 1
    assert response.country == "us"
    assert response.query == "test"
    assert response.category == "technology"
    assert response.data_type == MCType.NEWS_ARTICLE
    assert response.data_source == "NewsAI Trending"


def test_news_search_response():
    """Test NewsSearchResponse model."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(
        title="Test Article",
        url="https://example.com/article",
        news_source=source,
    )

    response = NewsSearchResponse(
        results=[article],
        total_results=1,
        query="AI technology",
        language="en",
        sort_by="publishedAt",
        page=1,
        page_size=20,
        status="ok",
    )

    assert len(response.results) == 1
    assert response.total_results == 1
    assert response.query == "AI technology"
    assert response.language == "en"
    assert response.sort_by == "publishedAt"
    assert response.data_type == MCType.NEWS_ARTICLE
    assert response.data_source == "NewsAI Search"


def test_news_sources_response():
    """Test NewsSourcesResponse model."""
    source1 = NewsSourceDetails(uri="source1", name="Source 1")
    source2 = NewsSourceDetails(uri="source2", name="Source 2")

    response = NewsSourcesResponse(
        results=[source1, source2],
        total_results=2,
        category="technology",
        language="en",
        country="us",
        status="ok",
    )

    assert len(response.results) == 2
    assert response.total_results == 2
    assert response.total_sources == 2  # Should be synced
    assert response.category == "technology"
    assert response.data_type == MCType.NEWS_ARTICLE
    assert response.data_source == "NewsAI Sources"


def test_news_sources_response_sync():
    """Test NewsSourcesResponse syncs total_sources with total_results."""
    source = NewsSourceDetails(uri="source1", name="Source 1")

    # Test syncing from total_results to total_sources
    response1 = NewsSourcesResponse(
        results=[source],
        total_results=1,
        status="ok",
    )
    assert response1.total_sources == 1

    # Test syncing from total_sources to total_results
    response2 = NewsSourcesResponse(
        results=[source],
        total_sources=1,
        status="ok",
    )
    assert response2.total_results == 1

