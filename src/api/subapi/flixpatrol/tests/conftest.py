"""
Shared fixtures and utilities for FlixPatrol service tests.

This conftest uses real API data that has been captured from FlixPatrol.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

import json
import os
from pathlib import Path

import pytest

# Load fixtures from JSON files
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"


def load_fixture(filename: str) -> dict:
    """Load a fixture from JSON file.

    Args:
        filename: Name of the fixture file

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}\n"
            f"Run 'bash fixtures/seed_mock_data.sh' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_flixpatrol_html():
    """Mock FlixPatrol HTML content."""
    return """
    <html>
        <body>
            <div id="toc-netflix-tv-shows">
                <h2>Top 10 TV Shows on Netflix on January 1, 2024</h2>
                <table>
                    <tbody>
                        <tr>
                            <td>1</td>
                            <td><a href="#">Test Show 1</a></td>
                            <td>1,000</td>
                        </tr>
                        <tr>
                            <td>2</td>
                            <td><a href="#">Test Show 2</a></td>
                            <td>900</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div id="toc-netflix-movies">
                <h2>Top 10 Movies on Netflix on January 1, 2024</h2>
                <table>
                    <tbody>
                        <tr>
                            <td>1</td>
                            <td><a href="#">Test Movie 1</a></td>
                            <td>2,000</td>
                        </tr>
                        <tr>
                            <td>2</td>
                            <td><a href="#">Test Movie 2</a></td>
                            <td>1,800</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </body>
    </html>
    """
