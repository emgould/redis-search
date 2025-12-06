"""Pytest configuration and fixtures for SchedulesDirect tests."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dotenv import load_dotenv

from api.schedulesdirect.auth import SchedulesDirectAuth
from api.schedulesdirect.core import SchedulesDirectService

# Set environment to test mode FIRST, before any imports
os.environ["ENVIRONMENT"] = "test"
os.environ["ENABLE_CACHE_FOR_TESTS"] = "1"

# Load environment variables from api.dev.env for integration tests
# Try multiple paths to find the config file
config_paths = [
    # Path relative to python_functions directory (when running from python_functions/)
    Path(__file__).parent.parent.parent.parent.parent / "config" / "api.dev.env",
    # Path from current working directory
    Path.cwd() / "config" / "api.dev.env",
    # Absolute path fallback
    Path("/Users/ericgould/dev/mediacircle/config/api.dev.env"),
]

config_loaded = False
for config_path in config_paths:
    if config_path.exists():
        load_dotenv(config_path, override=True)
        config_loaded = True
        break

# Map SCHEDULES_DIRECT_USER to SCHEDULES_DIRECT_USERNAME if needed
if os.getenv("SCHEDULES_DIRECT_USER") and not os.getenv("SCHEDULES_DIRECT_USERNAME"):
    os.environ["SCHEDULES_DIRECT_USERNAME"] = os.getenv("SCHEDULES_DIRECT_USER")


@pytest.fixture
def mock_auth():
    """Mock SchedulesDirectAuth with test credentials."""
    auth = SchedulesDirectAuth()
    auth._username = "test_user"
    auth._password = "test_password"
    auth._device_id = "test_device"
    return auth


@pytest.fixture
def schedules_direct_service(mock_auth):
    """Create SchedulesDirectService instance with mocked auth."""
    return SchedulesDirectService(auth=mock_auth)


@pytest.fixture
def mock_token_response():
    """Mock successful token response from SchedulesDirect."""
    return {
        "token": "test_token_12345",
        "expires": "2025-11-26T00:00:00Z",
        "code": 0,
        "message": "OK",
    }


@pytest.fixture
def mock_schedule_response():
    """Mock schedule response from SchedulesDirect.

    Note: airDateTime is in UTC. For primetime (8-11 PM Eastern = 20:00-23:00 EST),
    that's 01:00-04:00 UTC the next day. So for Nov 25 primetime, we need Nov 26 01:00-04:00 UTC.
    """
    return [
        {
            "stationID": "I10759",
            "programs": [
                {
                    "programID": "EP0000012345",
                    # 8 PM EST on Nov 25 = 01:00 UTC on Nov 26
                    "airDateTime": "2025-11-26T01:00:00Z",
                    "duration": 3600,
                    "md5": "abc123",
                    "new": True,
                    "liveTapeDelay": "Live",
                },
                {
                    "programID": "EP0000067890",
                    # 9 PM EST on Nov 25 = 02:00 UTC on Nov 26
                    "airDateTime": "2025-11-26T02:00:00Z",
                    "duration": 1800,
                    "md5": "def456",
                    "new": False,
                    "liveTapeDelay": "Tape",
                },
            ],
        },
        {
            "stationID": "I10760",
            "programs": [
                {
                    "programID": "EP0000098765",
                    # 8 PM EST on Nov 25 = 01:00 UTC on Nov 26
                    "airDateTime": "2025-11-26T01:00:00Z",
                    "duration": 3600,
                    "md5": "ghi789",
                    "new": True,
                },
            ],
        },
    ]


@pytest.fixture
def mock_program_metadata():
    """Mock program metadata response from SchedulesDirect."""
    return [
        {
            "programID": "EP0000012345",
            "titles": [{"title120": "The Test Show"}],
            "descriptions": {
                "description1000": [
                    {
                        "descriptionLanguage": "en",
                        "description": "A test show description.",
                    }
                ]
            },
            "originalAirDate": "2020-01-01",
            "genres": ["Drama"],
            "episodeTitle": "Pilot",
            "seasonNumber": 1,
            "episodeNumber": 1,
            "seriesInfo": {"seriesId": "12345"},
        },
        {
            "programID": "EP0000067890",
            "titles": [{"title120": "Another Show"}],
            "descriptions": {
                "description1000": [
                    {
                        "descriptionLanguage": "en",
                        "description": "Another show description.",
                    }
                ]
            },
            "originalAirDate": "2019-01-01",
            "genres": ["Comedy"],
        },
        {
            "programID": "EP0000098765",
            "titles": [{"title120": "Third Show"}],
            "descriptions": {
                "description1000": [
                    {
                        "descriptionLanguage": "en",
                        "description": "Third show description.",
                    }
                ]
            },
            "originalAirDate": "2021-01-01",
            "genres": ["Action"],
        },
    ]


@pytest.fixture
def mock_tmdb_search_response():
    """Mock TMDB search response."""
    from api.tmdb.models import MCTvItem

    return [
        MCTvItem(
            mc_type="tv",
            tmdb_id=12345,
            name="The Test Show",
            title="The Test Show",
            overview="A test show",
            first_air_date="2020-01-01",
            poster_path="/test.jpg",
            source_id="12345",
        )
    ]
