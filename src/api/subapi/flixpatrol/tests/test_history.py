"""Historical FlixPatrol backfill and title transformer."""

import json
from datetime import date
from pathlib import Path

import pytest

from api.subapi.flixpatrol.client import FlixPatrolAPIError, FlixPatrolClient, JsonObject
from api.subapi.flixpatrol.history import (
    backfill_us_monthly_rankings,
    iter_months,
    resolve_backfill_range,
    run_nightly_flixpatrol,
    transform_flixpatrol_rankings,
)

BASE = "https://api.flixpatrol.com/v2"


def _row(
    *,
    row_id: str,
    title_id: str,
    title: str,
    tmdb_id: int,
    link: str,
    company_id: str,
    media_type: int,
    date_type: int,
    date_from: str,
    date_to: str,
    ranking: int,
    value_total: int,
    value_change: str = "-56\u00a0%",
) -> JsonObject:
    return {
        "type": "rankings",
        "data": {
            "id": row_id,
            "movie": {
                "type": "titles",
                "data": {
                    "id": title_id,
                    "title": title,
                    "tmdbId": tmdb_id,
                    "tmdbLink": link,
                },
            },
            "company": {"type": "companies", "data": {"id": company_id}},
            "country": {
                "type": "countries",
                "data": {"id": "cnt_iMUHNbZvnNHK5YdhgwtOoP4u"},
            },
            "audience": 1,
            "type": media_type,
            "date": {"type": date_type, "from": date_from, "to": date_to},
            "ranking": ranking,
            "rankingLast": ranking + 1,
            "value": 16,
            "valueLast": 20,
            "valueTotal": value_total,
            "valueChange": value_change,
            "days": 15,
            "daysTotal": 194,
        },
    }


class FakeRankings:
    """Two-page list, then a refusal if a completed file is fetched again."""

    def __init__(self, pages: list[JsonObject]) -> None:
        self.base_url = BASE
        self.pages = pages
        self.calls: list[str] = []

    async def get_json(self, url: str) -> JsonObject:
        self.calls.append(url)
        index = len(self.calls) - 1
        if index >= len(self.pages):
            raise AssertionError(f"unexpected rankings request {url}")
        return self.pages[index]


def _two_pages(first_row: JsonObject, second_row: JsonObject) -> list[JsonObject]:
    return [
        {
            "links": {"next": f"{BASE}/rankings?starting_after[id]=rnk_page_2"},
            "data": [first_row, first_row],
        },
        {"links": {"next": None}, "data": [second_row]},
    ]


def test_default_range_is_january_2021_through_today() -> None:
    today = date(2026, 9, 24)
    start, end = resolve_backfill_range(None, None, today)
    assert start == date(2021, 1, 1)
    assert end == today


def test_inclusive_dates_cover_partial_endpoint_months() -> None:
    months = list(iter_months(date(2021, 1, 20), date(2021, 2, 2)))
    assert months == [
        (date(2021, 1, 1), date(2021, 1, 31)),
        (date(2021, 2, 1), date(2021, 2, 28)),
    ]


def test_end_before_start_is_rejected() -> None:
    with pytest.raises(ValueError, match="before start"):
        resolve_backfill_range(date(2024, 2, 1), date(2024, 1, 1), date(2026, 9, 24))


@pytest.mark.asyncio
async def test_backfill_writes_only_after_next_is_absent_and_skips_closed_month(
    tmp_path: Path,
) -> None:
    harry = _row(
        row_id="rnk_1",
        title_id="ttl_harry",
        title="Harry Potter and the Philosopher's Stone",
        tmdb_id=671,
        link="https://www.themoviedb.org/movie/671",
        company_id="cmp_a",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=28,
        value_total=516,
    )
    other_company = _row(
        row_id="rnk_2",
        title_id="ttl_harry",
        title="Harry Potter and the Philosopher's Stone",
        tmdb_id=671,
        link="https://www.themoviedb.org/movie/671",
        company_id="cmp_b",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=10,
        value_total=900,
    )
    client = FakeRankings(_two_pages(harry, other_company))
    results = await backfill_us_monthly_rankings(
        date(2025, 1, 15),
        date(2025, 1, 15),
        output_root=tmp_path,
        today=date(2026, 9, 24),
        client=client,
        transform=False,
    )
    assert len(client.calls) == 2
    assert "country%5Beq%5D=cnt_iMUHNbZvnNHK5YdhgwtOoP4u" in client.calls[0]
    assert "date%5Btype%5D%5Beq%5D=4" in client.calls[0]
    assert "date%5Bfrom%5D%5Beq%5D=2025-01-01" in client.calls[0]
    assert "date%5Bto%5D%5Beq%5D=2025-01-31" in client.calls[0]
    assert "starting_after" in client.calls[1]
    path = tmp_path / "us-monthly" / "2025-01.json"
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["complete"] is True
    assert stored["page_count"] == 2
    assert len(stored["data"]) == 3
    assert results[0].skipped is False

    again = await backfill_us_monthly_rankings(
        date(2025, 1, 1),
        date(2025, 1, 31),
        output_root=tmp_path,
        today=date(2026, 9, 24),
        client=client,
        transform=False,
    )
    assert len(client.calls) == 2
    assert again[0].skipped is True


@pytest.mark.asyncio
async def test_incomplete_pagination_does_not_write_a_month_file(tmp_path: Path) -> None:
    client = FakeRankings(
        [
            {
                "links": {"next": "https://example.test/rankings?starting_after[id]=rnk_x"},
                "data": [],
            }
        ]
    )
    with pytest.raises(FlixPatrolAPIError, match="outside the configured API base"):
        await backfill_us_monthly_rankings(
            date(2025, 1, 1),
            date(2025, 1, 1),
            output_root=tmp_path,
            today=date(2026, 9, 24),
            client=client,
            transform=False,
        )
    assert list((tmp_path / "us-monthly").glob("*.json")) == []


def test_transformer_peaks_dedupes_and_keeps_companies(tmp_path: Path) -> None:
    month_dir = tmp_path / "us-monthly"
    month_dir.mkdir()
    movie = _row(
        row_id="rnk_movie",
        title_id="ttl_movie",
        title="Fixture Movie",
        tmdb_id=654321,
        link="https://www.themoviedb.org/movie/654321",
        company_id="cmp_netflix",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=4,
        value_total=100,
    )
    movie_again = _row(
        row_id="rnk_movie_dup",
        title_id="ttl_movie",
        title="Fixture Movie",
        tmdb_id=654321,
        link="https://www.themoviedb.org/movie/654321",
        company_id="cmp_netflix",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=4,
        value_total=100,
    )
    other_company = _row(
        row_id="rnk_movie_hulu",
        title_id="ttl_movie",
        title="Fixture Movie",
        tmdb_id=654321,
        link="https://www.themoviedb.org/movie/654321",
        company_id="cmp_hulu",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=2,
        value_total=250,
    )
    mismatched = _row(
        row_id="rnk_bad",
        title_id="ttl_bad",
        title="Bad Link",
        tmdb_id=1,
        link="https://www.themoviedb.org/tv/1",
        company_id="cmp_netflix",
        media_type=1,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=1,
        value_total=9999,
    )
    week_dir = tmp_path / "us-weekly"
    week_dir.mkdir()
    weekly = _row(
        row_id="rnk_week",
        title_id="ttl_movie",
        title="Fixture Movie",
        tmdb_id=654321,
        link="https://www.themoviedb.org/movie/654321",
        company_id="cmp_netflix",
        media_type=1,
        date_type=3,
        date_from="2025-01-06",
        date_to="2025-01-12",
        ranking=9,
        value_total=40,
    )
    show = _row(
        row_id="rnk_show",
        title_id="ttl_show",
        title="Fixture Show",
        tmdb_id=456789,
        link="https://www.themoviedb.org/tv/456789",
        company_id="cmp_netflix",
        media_type=2,
        date_type=4,
        date_from="2025-01-01",
        date_to="2025-01-31",
        ranking=3,
        value_total=80,
    )
    (month_dir / "2025-01.json").write_text(
        json.dumps(
            {
                "complete": True,
                "data": [movie, movie_again, other_company, mismatched, show],
            }
        ),
        encoding="utf-8",
    )
    (week_dir / "2025-01-06.json").write_text(
        json.dumps({"complete": True, "data": [weekly]}),
        encoding="utf-8",
    )
    (month_dir / "incomplete.json").write_text(
        json.dumps({"complete": False, "data": [movie]}),
        encoding="utf-8",
    )

    assert transform_flixpatrol_rankings(tmp_path) == 2
    movie_file = json.loads(
        (tmp_path / "us-titles" / "tmdb_movie_654321.json").read_text(encoding="utf-8")
    )
    data = movie_file["data"]
    assert movie_file["title"] == "Fixture Movie"
    assert data["peak_total"] == 250
    assert data["peak_rank"] == 2
    assert data["max_days"] == 194
    assert data["start_date"] == "2025-01-01"
    assert data["last_date"] == "2025-01-31"
    assert len(data["records"]) == 3
    assert data["records"][0]["valueChange"] == "-56\u00a0%"
    assert {record["date_type"] for record in data["records"]} == {3, 4}
    show_file = json.loads(
        (tmp_path / "us-titles" / "tmdb_tv_456789.json").read_text(encoding="utf-8")
    )
    assert show_file["mc_id"] == "tmdb_tv_456789"
    assert not (tmp_path / "us-titles" / "tmdb_movie_1.json").exists()


@pytest.mark.asyncio
async def test_wednesday_fetches_completed_week_and_other_days_do_not(tmp_path: Path) -> None:
    weekly = _row(
        row_id="rnk_week",
        title_id="ttl_movie",
        title="Fixture Movie",
        tmdb_id=654321,
        link="https://www.themoviedb.org/movie/654321",
        company_id="cmp_netflix",
        media_type=1,
        date_type=3,
        date_from="2026-09-14",
        date_to="2026-09-20",
        ranking=1,
        value_total=10,
    )
    client = FakeRankings([{"links": {"next": None}, "data": [weekly]}])
    skipped = await run_nightly_flixpatrol(
        date(2026, 9, 24),
        output_root=tmp_path,
        client=client,
    )
    assert skipped is None
    assert client.calls == []

    result = await run_nightly_flixpatrol(
        date(2026, 9, 23),
        output_root=tmp_path,
        client=client,
    )
    assert result is not None
    assert len(client.calls) == 1
    assert "date%5Btype%5D%5Beq%5D=3" in client.calls[0]
    assert "date%5Bfrom%5D%5Beq%5D=2026-09-14" in client.calls[0]
    assert "date%5Bto%5D%5Beq%5D=2026-09-20" in client.calls[0]
    assert (tmp_path / "us-weekly" / "2026-09-14.json").is_file()
    assert result.title_count == 1


@pytest.mark.asyncio
async def test_get_json_rejects_off_base_url() -> None:
    client = FlixPatrolClient(base_url=BASE)
    with pytest.raises(FlixPatrolAPIError, match="outside the configured API base"):
        await client.get_json("https://example.test/v2/rankings")
