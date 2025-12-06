"""
Unit tests for Comscore Models.
Tests Pydantic model validation and serialization.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking, BoxOfficeResponse
from api.subapi.comscore.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestBoxOfficeRanking:
    """Tests for BoxOfficeRanking model."""

    def test_box_office_ranking_validation(self):
        """Test BoxOfficeRanking model validation."""
        mock_box_office_ranking = load_fixture("models/box_office_ranking.json")
        ranking = BoxOfficeRanking.model_validate(mock_box_office_ranking)

        assert ranking.rank == 1
        assert ranking.title_name == "Wicked"
        assert ranking.weekend_estimate == "15500000"
        assert ranking.dom_distributor == "Universal Pictures"
        assert ranking.intl_distributor == "Universal Pictures International"

    def test_box_office_ranking_with_minimal_data(self):
        """Test BoxOfficeRanking with minimal required fields."""
        ranking = BoxOfficeRanking(
            rank=1,
            title_name="Test Movie",
            weekend_estimate="1000000",
        )

        assert ranking.rank == 1
        assert ranking.title_name == "Test Movie"
        assert ranking.weekend_estimate == "1000000"
        assert ranking.dom_distributor is None
        assert ranking.intl_distributor is None

    def test_box_office_ranking_missing_required_field(self):
        """Test that missing required fields raise ValidationError."""
        with pytest.raises(ValidationError):
            BoxOfficeRanking(
                rank=1,
                # Missing title_name and weekend_estimate
            )

    def test_box_office_ranking_serialization(self):
        """Test BoxOfficeRanking serialization to dict."""
        mock_box_office_ranking = load_fixture("models/box_office_ranking.json")
        ranking = BoxOfficeRanking.model_validate(mock_box_office_ranking)
        data = ranking.model_dump()

        assert isinstance(data, dict)
        assert data["rank"] == 1
        assert data["title_name"] == "Wicked"
        assert "weekend_estimate" in data


class TestBoxOfficeData:
    """Tests for BoxOfficeData model."""

    def test_box_office_data_validation(self):
        """Test BoxOfficeData model validation."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        data = BoxOfficeData.model_validate(mock_box_office_data)

        assert len(data.rankings) == 3
        assert data.exhibition_week == "2025-01-03"
        assert data.fetched_at == "2025-01-03T12:00:00"
        assert all(isinstance(r, BoxOfficeRanking) for r in data.rankings)

    def test_box_office_data_empty_rankings(self):
        """Test BoxOfficeData with empty rankings list."""
        data = BoxOfficeData(
            rankings=[],
            exhibition_week="2025-01-03",
            fetched_at="2025-01-03T12:00:00",
        )

        assert len(data.rankings) == 0
        assert data.exhibition_week == "2025-01-03"

    def test_box_office_data_rankings_order(self):
        """Test that rankings maintain their order."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        data = BoxOfficeData.model_validate(mock_box_office_data)

        assert data.rankings[0].rank == 1
        assert data.rankings[0].title_name == "Wicked"
        assert data.rankings[1].rank == 2
        assert data.rankings[1].title_name == "Moana 2"
        assert data.rankings[2].rank == 3
        assert data.rankings[2].title_name == "Nosferatu"

    def test_box_office_data_serialization(self):
        """Test BoxOfficeData serialization to dict."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        data = BoxOfficeData.model_validate(mock_box_office_data)
        serialized = data.model_dump()

        assert isinstance(serialized, dict)
        assert "rankings" in serialized
        assert isinstance(serialized["rankings"], list)
        assert len(serialized["rankings"]) == 3


class TestBoxOfficeResponse:
    """Tests for BoxOfficeResponse model."""

    def test_box_office_response_with_data(self):
        """Test BoxOfficeResponse with successful data."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        box_office_data = BoxOfficeData.model_validate(mock_box_office_data)
        response = BoxOfficeResponse(data=box_office_data, error=None)

        assert response.data is not None
        assert response.error is None
        assert len(response.data.rankings) == 3

    def test_box_office_response_with_error(self):
        """Test BoxOfficeResponse with error."""
        response = BoxOfficeResponse(
            data=None, error={"error": "Failed to fetch", "code": "fetch_failed"}
        )

        assert response.data is None
        assert response.error is not None
        assert response.error["error"] == "Failed to fetch"
        assert response.error["code"] == "fetch_failed"

    def test_box_office_response_both_none(self):
        """Test BoxOfficeResponse with both data and error as None."""
        response = BoxOfficeResponse(data=None, error=None)

        assert response.data is None
        assert response.error is None
