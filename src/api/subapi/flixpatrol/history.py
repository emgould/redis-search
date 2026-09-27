"""United States FlixPatrol history: monthly backfill, Wednesday weeks, title files.

The chart pull defines which titles are stored. Closed periods are written once
and are not requested again. Each rankings HTTP GET is cached by its full URL,
including ``starting_after[id]``, so a retry does not repeat a successful page.
"""

import argparse
import asyncio
import calendar
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import urlencode

from api.subapi.flixpatrol.client import (
    MAX_PAGES,
    FlixPatrolAPIError,
    FlixPatrolClient,
    JsonObject,
    JsonValue,
)
from api.subapi.flixpatrol.models import (
    FlixPatrolHistory,
    FlixPatrolHistoryRecord,
    FlixPatrolTitleFile,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

FlixPatrolRankingsCache = RedisCache(
    defaultTTL=60 * 60 * 24,
    prefix="flixpatrol",
    verbose=False,
    isClassMethod=True,
)

US_COUNTRY_ID = "cnt_iMUHNbZvnNHK5YdhgwtOoP4u"
HISTORY_START = date(2021, 1, 1)
RANKING_MONTH_TYPE = 4
RANKING_WEEK_TYPE = 3
RANKING_MOVIE_TYPE = 1
RANKING_TV_TYPE = 2
CLOSED_PERIOD_EXPIRY_SECONDS = 60 * 60 * 24 * 365
OPEN_PERIOD_EXPIRY_SECONDS = 60 * 60 * 24
WEDNESDAY = 2


class RankingsTransport(Protocol):
    """Page fetch used by the historical backfill."""

    base_url: str

    async def get_json(self, url: str) -> JsonObject:
        """Return one rankings list envelope."""


@dataclass(frozen=True)
class PeriodWrite:
    """Outcome of one month or week file."""

    period_start: date
    period_end: date
    path: Path
    page_count: int
    row_count: int
    skipped: bool


@dataclass(frozen=True)
class NightlyFlixPatrolResult:
    """Wednesday maintenance: completed weeks fetched, then title files rebuilt."""

    weeks: list[PeriodWrite]
    title_count: int


class CachedRankingsClient:
    """Cache each rankings GET. The cache key is the request URL."""

    def __init__(self, client: RankingsTransport) -> None:
        self._client = client

    @RedisCache.use_cache(FlixPatrolRankingsCache, prefix="rankings_request")
    async def get_rankings_page(self, url: str, expiry: int | None = None) -> JsonObject:
        """Fetch one page. ``expiry`` is consumed by the cache decorator."""
        del expiry
        return await self._client.get_json(url)


def default_output_root() -> Path:
    """Return ``<repo>/data/flixpatrol``."""
    return Path(__file__).resolve().parents[4] / "data" / "flixpatrol"


def resolve_backfill_range(
    start: date | None,
    end: date | None,
    today: date,
) -> tuple[date, date]:
    """Resolve an inclusive date range. Omitted dates mean January 2021 through today."""
    resolved_start = HISTORY_START if start is None else start
    resolved_end = today if end is None else end
    if resolved_end < resolved_start:
        raise ValueError(
            f"end {resolved_end.isoformat()} is before start {resolved_start.isoformat()}"
        )
    return resolved_start, resolved_end


def iter_months(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Yield each calendar month touched by an inclusive start and end date."""
    year = start.year
    month = start.month
    end_key = (end.year, end.month)
    while (year, month) <= end_key:
        last_day = calendar.monthrange(year, month)[1]
        yield date(year, month, 1), date(year, month, last_day)
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def previous_completed_week(today: date) -> tuple[date, date]:
    """Return the Monday–Sunday week that ended before the current week."""
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    return last_monday, last_monday + timedelta(days=6)


def rankings_url(
    base_url: str,
    *,
    date_type: int,
    period_start: date,
    period_end: date,
) -> str:
    """Build a US chart URL with no title, company, or rank filter."""
    query = urlencode(
        {
            "date[type][eq]": str(date_type),
            "date[from][eq]": period_start.isoformat(),
            "date[to][eq]": period_end.isoformat(),
            "country[eq]": US_COUNTRY_ID,
        }
    )
    return f"{base_url}/rankings?{query}"


def period_is_open(period_end: date, today: date) -> bool:
    """A period is open while its last day has not passed."""
    return period_end >= today


def _read_json_object(path: Path) -> dict[str, JsonValue] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return cast(dict[str, JsonValue], payload)


def period_file_is_complete(path: Path) -> bool:
    """A complete file exists only after the last page had no ``links.next``."""
    payload = _read_json_object(path)
    if payload is None or payload.get("complete") is not True:
        return False
    return isinstance(payload.get("data"), list)


def _write_json(path: Path, payload: dict[str, JsonValue]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.partial")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(path)


def _as_object(value: JsonValue) -> dict[str, JsonValue] | None:
    if not isinstance(value, dict):
        return None
    return value


def _as_int(value: JsonValue) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def _as_str(value: JsonValue) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value


def _relationship_id(value: JsonValue) -> str | None:
    relationship = _as_object(value)
    if relationship is None:
        return None
    data = _as_object(relationship.get("data"))
    if data is None:
        return None
    return _as_str(data.get("id"))


async def fetch_complete_rankings(
    cached: CachedRankingsClient,
    first_url: str,
    *,
    expiry_seconds: int,
    base_url: str,
) -> tuple[list[JsonObject], int]:
    """Follow ``links.next`` until it is absent. Do not write a file here."""
    url: str | None = first_url
    rows: list[JsonObject] = []
    pages = 0
    while url is not None:
        if pages >= MAX_PAGES:
            raise FlixPatrolAPIError(
                f"FlixPatrol pagination exceeded {MAX_PAGES} pages for {first_url}"
            )
        payload = await cached.get_rankings_page(url, expiry=expiry_seconds)
        pages += 1
        rows.extend(FlixPatrolClient._extract_items(payload))
        next_url = FlixPatrolClient._extract_next_url(payload)
        if next_url is None:
            return rows, pages
        if not next_url.startswith(f"{base_url}/"):
            raise FlixPatrolAPIError(
                "FlixPatrol pagination link points outside the configured API base"
            )
        url = next_url
    return rows, pages


async def _fetch_period(
    *,
    client: RankingsTransport,
    output_dir: Path,
    file_name: str,
    date_type: int,
    period_start: date,
    period_end: date,
    grain: str,
    today: date,
) -> PeriodWrite:
    path = output_dir / file_name
    closed = not period_is_open(period_end, today)
    if closed and period_file_is_complete(path):
        logger.info("Skipping complete %s file %s", grain, path.name)
        return PeriodWrite(period_start, period_end, path, 0, 0, True)

    first_url = rankings_url(
        client.base_url,
        date_type=date_type,
        period_start=period_start,
        period_end=period_end,
    )
    expiry = CLOSED_PERIOD_EXPIRY_SECONDS if closed else OPEN_PERIOD_EXPIRY_SECONDS
    rows, page_count = await fetch_complete_rankings(
        CachedRankingsClient(client),
        first_url,
        expiry_seconds=expiry,
        base_url=client.base_url,
    )
    payload: dict[str, JsonValue] = {
        "grain": grain,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "query_url": first_url,
        "fetched_at": datetime.now(UTC).isoformat(),
        "page_count": page_count,
        "complete": True,
        "data": cast(JsonValue, rows),
    }
    _write_json(path, payload)
    logger.info("Wrote %s %s (%s pages, %s rows)", grain, path.name, page_count, len(rows))
    return PeriodWrite(period_start, period_end, path, page_count, len(rows), False)


async def backfill_us_monthly_rankings(
    start: date | None = None,
    end: date | None = None,
    *,
    output_root: Path | None = None,
    today: date | None = None,
    client: RankingsTransport | None = None,
    transform: bool = True,
) -> list[PeriodWrite]:
    """Pull inclusive US months. Omitted dates cover January 2021 through today.

    Each request uses the first and last day of the calendar month that contains
    the endpoint date. A complete closed month file is not requested again.
    """
    current_day = date.today() if today is None else today
    resolved_start, resolved_end = resolve_backfill_range(start, end, current_day)
    root = default_output_root() if output_root is None else output_root
    transport = FlixPatrolClient() if client is None else client
    results: list[PeriodWrite] = []
    for month_start, month_end in iter_months(resolved_start, resolved_end):
        results.append(
            await _fetch_period(
                client=transport,
                output_dir=root / "us-monthly",
                file_name=f"{month_start:%Y-%m}.json",
                date_type=RANKING_MONTH_TYPE,
                period_start=month_start,
                period_end=month_end,
                grain="month",
                today=current_day,
            )
        )
    if transform:
        transform_flixpatrol_rankings(root)
    return results


def _ranking_body(item: JsonValue) -> dict[str, JsonValue] | None:
    envelope = _as_object(item)
    if envelope is None or envelope.get("type") != "rankings":
        return None
    return _as_object(envelope.get("data"))


def _mc_id_for_row(body: dict[str, JsonValue]) -> str | None:
    movie = _as_object(body.get("movie"))
    if movie is None:
        return None
    title = _as_object(movie.get("data"))
    if title is None:
        return None
    tmdb_id = _as_int(title.get("tmdbId"))
    link = _as_str(title.get("tmdbLink"))
    media_type = _as_int(body.get("type"))
    if tmdb_id is None or link is None:
        return None
    if media_type == RANKING_MOVIE_TYPE and "/movie/" in link:
        return f"tmdb_movie_{tmdb_id}"
    if media_type == RANKING_TV_TYPE and "/tv/" in link:
        return f"tmdb_tv_{tmdb_id}"
    return None


def _title_for_row(body: dict[str, JsonValue]) -> str:
    movie = _as_object(body.get("movie"))
    if movie is None:
        return ""
    title = _as_object(movie.get("data"))
    if title is None:
        return ""
    return _as_str(title.get("title")) or ""


def _record_for_row(body: dict[str, JsonValue]) -> FlixPatrolHistoryRecord | None:
    date_range = _as_object(body.get("date"))
    if date_range is None:
        return None
    date_type = _as_int(date_range.get("type"))
    date_from = _as_str(date_range.get("from"))
    date_to = _as_str(date_range.get("to"))
    ranking = _as_int(body.get("ranking"))
    if date_type is None or date_from is None or date_to is None or ranking is None:
        return None
    value_change = body.get("valueChange")
    return FlixPatrolHistoryRecord.model_validate(
        {
            "date_type": date_type,
            "date_from": date_from,
            "date_to": date_to,
            "ranking": ranking,
            "rankingLast": _as_int(body.get("rankingLast")),
            "value": _as_int(body.get("value")),
            "valueLast": _as_int(body.get("valueLast")),
            "valueTotal": _as_int(body.get("valueTotal")),
            "valueChange": value_change if isinstance(value_change, str) else None,
            "days": _as_int(body.get("days")),
            "daysTotal": _as_int(body.get("daysTotal")),
        }
    )


def _dedupe_key(body: dict[str, JsonValue]) -> tuple[str, str, str, int, int, int, str, str] | None:
    date_range = _as_object(body.get("date"))
    if date_range is None:
        return None
    date_type = _as_int(date_range.get("type"))
    date_from = _as_str(date_range.get("from"))
    date_to = _as_str(date_range.get("to"))
    audience = _as_int(body.get("audience"))
    media_type = _as_int(body.get("type"))
    title_id = _relationship_id(body.get("movie"))
    company_id = _relationship_id(body.get("company"))
    country_id = _relationship_id(body.get("country"))
    if (
        date_type is None
        or date_from is None
        or date_to is None
        or audience is None
        or media_type is None
        or title_id is None
        or company_id is None
        or country_id is None
    ):
        return None
    return (title_id, company_id, country_id, audience, media_type, date_type, date_from, date_to)


def _history_for_rows(
    rows: list[tuple[dict[str, JsonValue], FlixPatrolHistoryRecord]],
) -> FlixPatrolHistory:
    records = [record for _body, record in rows]
    records.sort(key=lambda record: (record.date_to, record.date_type, record.ranking))
    totals = [record.value_total for record in records if record.value_total is not None]
    day_totals = [record.days_total for record in records if record.days_total is not None]
    return FlixPatrolHistory(
        peak_total=max(totals) if totals else None,
        peak_rank=min(record.ranking for record in records),
        max_days=max(day_totals) if day_totals else None,
        last_date=max(record.date_to for record in records),
        start_date=min(record.date_from for record in records),
        records=records,
    )


def _iter_complete_rows(directory: Path) -> Iterator[JsonValue]:
    if not directory.is_dir():
        return
    for path in sorted(directory.glob("*.json")):
        payload = _read_json_object(path)
        if payload is None or payload.get("complete") is not True:
            continue
        data = payload.get("data")
        if not isinstance(data, list):
            continue
        yield from data


def transform_flixpatrol_rankings(output_root: Path | None = None) -> int:
    """Group complete month and week files into one title file per ``mc_id``.

    Weekly and monthly rows stay separate records. ``peak_total`` is the maximum
    ``valueTotal``, not a sum. Page duplicates of the same title, company,
    country, audience, type, and date range count once.
    """
    root = default_output_root() if output_root is None else output_root
    grouped: dict[str, list[tuple[dict[str, JsonValue], FlixPatrolHistoryRecord]]] = {}
    titles: dict[str, str] = {}
    seen: set[tuple[str, tuple[str, str, str, int, int, int, str, str]]] = set()
    for item in _iter_complete_rows(root / "us-monthly"):
        _collect_row(item, grouped, titles, seen)
    for item in _iter_complete_rows(root / "us-weekly"):
        _collect_row(item, grouped, titles, seen)

    title_dir = root / "us-titles"
    written = 0
    for mc_id, rows in grouped.items():
        if not rows:
            continue
        title_file = FlixPatrolTitleFile(
            mc_id=mc_id,
            title=titles.get(mc_id, ""),
            data=_history_for_rows(rows),
        )
        _write_json(
            title_dir / f"{mc_id}.json",
            cast(dict[str, JsonValue], title_file.model_dump(mode="json", by_alias=True)),
        )
        written += 1
    logger.info("Wrote %s FlixPatrol title files", written)
    return written


def _collect_row(
    item: JsonValue,
    grouped: dict[str, list[tuple[dict[str, JsonValue], FlixPatrolHistoryRecord]]],
    titles: dict[str, str],
    seen: set[tuple[str, tuple[str, str, str, int, int, int, str, str]]],
) -> None:
    body = _ranking_body(item)
    if body is None:
        return
    mc_id = _mc_id_for_row(body)
    dedupe_key = _dedupe_key(body)
    record = _record_for_row(body)
    if mc_id is None or dedupe_key is None or record is None:
        return
    identity = (mc_id, dedupe_key)
    if identity in seen:
        return
    seen.add(identity)
    grouped.setdefault(mc_id, []).append((body, record))
    if mc_id not in titles:
        titles[mc_id] = _title_for_row(body)


def _latest_complete_week_start(weekly_dir: Path) -> date | None:
    latest: date | None = None
    if not weekly_dir.is_dir():
        return None
    for path in weekly_dir.glob("*.json"):
        if not period_file_is_complete(path):
            continue
        payload = _read_json_object(path)
        if payload is None:
            continue
        raw_start = _as_str(payload.get("period_start"))
        if raw_start is None:
            continue
        try:
            period_start = date.fromisoformat(raw_start)
        except ValueError:
            continue
        if latest is None or period_start > latest:
            latest = period_start
    return latest


def weeks_to_maintain(today: date, weekly_dir: Path) -> list[tuple[date, date]]:
    """Missing completed weeks through the previous Sunday.

    With no week files, only the latest completed week is requested. A missed
    Wednesday continues from the week after the latest stored week. Monthly
    files remain the historical backfill.
    """
    last_monday, _last_sunday = previous_completed_week(today)
    latest_week = _latest_complete_week_start(weekly_dir)
    if latest_week is None:
        return [(last_monday, last_monday + timedelta(days=6))]
    cursor = latest_week + timedelta(days=7)
    weeks: list[tuple[date, date]] = []
    while cursor <= last_monday:
        weeks.append((cursor, cursor + timedelta(days=6)))
        cursor += timedelta(days=7)
    return weeks


async def run_nightly_flixpatrol(
    on: date | None = None,
    *,
    output_root: Path | None = None,
    client: RankingsTransport | None = None,
) -> NightlyFlixPatrolResult | None:
    """On Wednesday, fetch missing completed US weeks and rebuild title files.

    Uses ``date[type]=3`` with the same United States country and no company or
    rank filter. This is not the Explore weekly chart.
    """
    current_day = date.today() if on is None else on
    if current_day.weekday() != WEDNESDAY:
        return None
    root = default_output_root() if output_root is None else output_root
    transport = FlixPatrolClient() if client is None else client
    weeks: list[PeriodWrite] = []
    for week_start, week_end in weeks_to_maintain(current_day, root / "us-weekly"):
        weeks.append(
            await _fetch_period(
                client=transport,
                output_dir=root / "us-weekly",
                file_name=f"{week_start.isoformat()}.json",
                date_type=RANKING_WEEK_TYPE,
                period_start=week_start,
                period_end=week_end,
                grain="week",
                today=current_day,
            )
        )
    title_count = transform_flixpatrol_rankings(root)
    return NightlyFlixPatrolResult(weeks=weeks, title_count=title_count)


def _parse_cli_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> None:
    """Backfill US months. ``--start`` and ``--end`` are inclusive; both optional."""
    parser = argparse.ArgumentParser(description="Backfill United States FlixPatrol rankings")
    parser.add_argument("--start", type=_parse_cli_date, default=None)
    parser.add_argument("--end", type=_parse_cli_date, default=None)
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Rebuild title files from complete month and week files",
    )
    args = parser.parse_args(argv)
    if args.transform_only:
        print(transform_flixpatrol_rankings())
        return
    results = asyncio.run(backfill_us_monthly_rankings(args.start, args.end))
    for result in results:
        state = "skipped" if result.skipped else "fetched"
        print(
            f"{result.period_start.isoformat()} {state} "
            f"pages={result.page_count} rows={result.row_count}"
        )


if __name__ == "__main__":
    main()
