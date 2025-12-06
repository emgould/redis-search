"""
Unit tests for News models.
Tests Pydantic model validation and serialization.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from api.news.models import (
    MCNewsItem,
    NewsSearchResponse,
    NewsSource,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)

pytestmark = pytest.mark.unit


def test_news_source_model():
    """Test NewsSource model."""
    source = NewsSource(id="bbc-news", name="BBC News")

    assert source.id == "bbc-news"
    assert source.name == "BBC News"


def test_news_source_model_no_id():
    """Test NewsSource model without ID."""
    source = NewsSource(name="BBC News")

    assert source.id is None
    assert source.name == "BBC News"


def test_news_article_model():
    """Test MCNewsItem model."""
    source = NewsSource(id="bbc-news", name="BBC News")
    article = MCNewsItem(
        title="Test Article",
        description="Test description",
        content="Test content",
        url="https://example.com/article",
        url_to_image="https://example.com/image.jpg",
        published_at="2024-01-15T10:30:00Z",
        author="Test Author",
        news_source=source,  # Property 'source' provides backward compatibility
    )

    assert article.title == "Test Article"
    assert article.description == "Test description"
    assert article.url == "https://example.com/article"
    assert article.news_source.name == "BBC News"  # news_source is the NewsSource object
    assert article.source.value == "newsapi"  # source is the MCSources enum
    assert article.mc_id is not None
    assert article.mc_type == "news_article"


def test_news_article_model_minimal():
    """Test MCNewsItem model with minimal data."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(title="Test", url="https://example.com", news_source=source)

    assert article.title == "Test"
    assert article.url == "https://example.com"
    assert article.description is None
    assert article.mc_id is not None


def test_news_article_model_validation():
    """Test MCNewsItem model validation."""
    # Missing required fields
    with pytest.raises(ValidationError):
        MCNewsItem(title="Test")  # Missing url and source


def test_news_article_mc_id_generation():
    """Test mc_id generation for MCNewsItem."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(title="Test Article", url="https://example.com", news_source=source)

    assert article.mc_id is not None
    assert article.mc_id.startswith("news_")
    assert article.mc_type == "news_article"


def test_news_source_details_model():
    """Test NewsSourceDetails model."""
    source = NewsSourceDetails(
        id="bbc-news",
        name="BBC News",
        description="BBC News description",
        url="https://bbc.com",
        category="general",
        language="en",
        country="gb",
    )

    assert source.id == "bbc-news"
    assert source.name == "BBC News"
    assert source.category == "general"
    assert source.language == "en"
    assert source.country == "gb"


def test_trending_news_response_model():
    """Test TrendingNewsResponse model."""
    source = NewsSource(name="BBC News")
    article = MCNewsItem(title="Test", url="https://example.com", news_source=source)

    response = TrendingNewsResponse(
        results=[article],
        total_results=1,
        country="us",
        query="technology",
        category="technology",
        page=1,
        page_size=20,
        status="ok",
    )

    assert len(response.results) == 1
    assert response.total_results == 1
    assert response.country == "us"
    assert response.query == "technology"
    assert response.category == "technology"
    assert response.status == "ok"
    assert response.fetched_at is not None


def test_trending_news_response_defaults():
    """Test TrendingNewsResponse default values."""
    response = TrendingNewsResponse(results=[], total_results=0)

    assert response.page == 1
    assert response.page_size == 20
    assert response.fetched_at is not None


def test_news_search_response_model():
    """Test NewsSearchResponse model."""
    source = NewsSource(name="TechCrunch")
    article = MCNewsItem(title="AI News", url="https://example.com", news_source=source)

    response = NewsSearchResponse(
        results=[article],
        total_results=1,
        query="AI",
        language="en",
        sort_by="publishedAt",
        from_date="2024-01-01",
        to_date="2024-01-31",
        page=1,
        page_size=20,
        status="ok",
    )

    assert len(response.results) == 1
    assert response.query == "AI"
    assert response.language == "en"
    assert response.from_date == "2024-01-01"
    assert response.to_date == "2024-01-31"


def test_news_sources_response_model():
    """Test NewsSourcesResponse model."""
    source1 = NewsSourceDetails(id="bbc-news", name="BBC News")
    source2 = NewsSourceDetails(id="cnn", name="CNN")

    response = NewsSourcesResponse(
        results=[source1, source2],
        total_results=2,
        total_sources=2,
        category="general",
        language="en",
        country="us",
        status="ok",
    )

    assert len(response.results) == 2
    assert response.total_sources == 2
    assert response.category == "general"


def test_model_serialization():
    """Test model serialization to dict."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(title="Test", url="https://example.com", news_source=source)

    article_dict = article.model_dump()

    assert isinstance(article_dict, dict)
    assert article_dict["title"] == "Test"
    assert article_dict["url"] == "https://example.com"
    assert article_dict["news_source"]["name"] == "Test Source"
    assert article_dict["mc_id"] is not None


def test_model_json_serialization():
    """Test model JSON serialization."""
    source = NewsSource(name="Test Source")
    article = MCNewsItem(title="Test", url="https://example.com", news_source=source)

    json_str = article.model_dump_json()

    assert isinstance(json_str, str)
    assert "Test" in json_str
    assert "https://example.com" in json_str
