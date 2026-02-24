#!/usr/bin/env python3
"""
Full integration test for backfill normalization and document shape validation.

Loads sample cache data (movie + TV), enriches each item via real TMDB API
calls (get_content_rating, get_streaming_platform_summary_for_title), runs
the full normalize → document_to_redis pipeline, and validates the output
matches the canonical schema.

ZERO writes to Redis. Real API calls to TMDB.

Usage:
    python scripts/test_backfill_dryrun_sample.py
    python scripts/test_backfill_dryrun_sample.py --verbose
    python scripts/test_backfill_dryrun_sample.py --output /tmp/backfill_sample.json
    python scripts/test_backfill_dryrun_sample.py --count 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from api.tmdb.core import TMDBService  # noqa: E402
from api.tmdb.get_providers import get_streaming_platform_summary_for_title  # noqa: E402
from contracts.models import MCSources, MCType  # noqa: E402
from core.normalize import (  # noqa: E402
    BACKFILL_DEFAULT_TS,
    document_to_redis,
    normalize_document,
    resolve_timestamps,
)
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

DATA_DIR = Path("data/us")

WATCH_PROVIDERS_REQUIRED_KEYS: set[str] = {
    "streaming_platform_ids",
    "streaming_platforms",
    "primary_provider",
    "primary_provider_id",
    "primary_provider_type",
    "watch_region",
    "on_demand_platform_ids",
    "on_demand_platforms",
}

MEDIA_REQUIRED_KEYS: dict[str, tuple[type, ...]] = {
    "id": (str,),
    "title": (str,),
    "search_title": (str,),
    "mc_type": (str,),
    "mc_subtype": (str, type(None)),
    "source": (str,),
    "source_id": (str,),
    "year": (int, type(None)),
    "popularity": (float, int),
    "rating": (float, int),
    "image": (str, type(None)),
    "overview": (str, type(None)),
    "genre_ids": (list,),
    "genres": (list,),
    "cast_ids": (list,),
    "cast_names": (list,),
    "cast": (list,),
    "director": (dict, type(None)),
    "keywords": (list,),
    "origin_country": (list,),
    "release_date": (str, type(None)),
    "first_air_date": (str, type(None)),
    "last_air_date": (str, type(None)),
    "us_rating": (str, type(None)),
    "watch_providers": (dict, type(None)),
    "created_at": (int, type(None)),
    "modified_at": (int, type(None)),
    "_source": (str, type(None)),
}

DIRECTOR_KEYS: set[str] = {"id", "name", "name_normalized"}


class TestResult:
    def __init__(self, name: str) -> None:
        self.name = name
        self.passed = True
        self.errors: list[str] = []
        self.info: list[str] = []

    def fail(self, msg: str) -> None:
        self.passed = False
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.info.append(msg)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        out = f"  [{status}] {self.name}"
        for err in self.errors:
            out += f"\n         ERROR: {err}"
        for inf in self.info:
            out += f"\n         INFO:  {inf}"
        return out


# ---------- Shape validation ----------


def validate_schema_keys(doc: dict[str, Any], result: TestResult) -> None:
    for key, expected_types in MEDIA_REQUIRED_KEYS.items():
        if key not in doc:
            result.fail(f"Missing key: {key}")
            continue
        val = doc[key]
        if not isinstance(val, expected_types):
            result.fail(
                f"Key '{key}': expected {expected_types}, got {type(val).__name__} = {val!r}"
            )


def validate_director_shape(doc: dict[str, Any], result: TestResult) -> None:
    director = doc.get("director")
    if director is None:
        return
    if not isinstance(director, dict):
        result.fail(f"director should be dict|None, got {type(director).__name__}")
        return
    for k in DIRECTOR_KEYS:
        if k not in director:
            result.fail(f"director missing key: {k}")
        elif not isinstance(director[k], str):
            result.fail(f"director.{k} should be str, got {type(director[k]).__name__}")


def validate_watch_providers_shape(doc: dict[str, Any], result: TestResult) -> None:
    wp = doc.get("watch_providers")
    if wp is None:
        result.warn("watch_providers is None (API returned no data for this title)")
        return
    if not isinstance(wp, dict):
        result.fail(f"watch_providers should be dict|None, got {type(wp).__name__}")
        return
    for k in WATCH_PROVIDERS_REQUIRED_KEYS:
        if k not in wp:
            result.fail(f"watch_providers missing key: {k}")
    if "watch_region" in wp and wp["watch_region"] != "US":
        result.fail(f"watch_providers.watch_region should be 'US', got {wp['watch_region']!r}")
    ppt = wp.get("primary_provider_type")
    valid_ppt = {None, "flatrate", "in theater", "on_demand"}
    if ppt not in valid_ppt:
        result.fail(f"watch_providers.primary_provider_type={ppt!r} not in {valid_ppt}")


def validate_movie_specifics(doc: dict[str, Any], result: TestResult) -> None:
    if doc.get("mc_type") != "movie":
        result.fail(f"Expected mc_type='movie', got {doc.get('mc_type')!r}")
    if doc.get("first_air_date") is not None:
        result.fail(f"Movie should have first_air_date=None, got {doc['first_air_date']!r}")
    if doc.get("last_air_date") is not None:
        result.fail(f"Movie should have last_air_date=None, got {doc['last_air_date']!r}")


def validate_tv_specifics(doc: dict[str, Any], result: TestResult) -> None:
    if doc.get("mc_type") != "tv":
        result.fail(f"Expected mc_type='tv', got {doc.get('mc_type')!r}")
    if doc.get("release_date") is not None:
        result.fail(f"TV should have release_date=None, got {doc['release_date']!r}")
    if doc.get("director") is not None:
        result.fail(f"TV should have director=None, got {doc['director']!r}")


def validate_timestamps(
    doc: dict[str, Any],
    result: TestResult,
    *,
    expect_source: str | None = None,
) -> None:
    ca = doc.get("created_at")
    ma = doc.get("modified_at")
    if ca is None:
        result.fail("created_at is None after resolve")
    if ma is None:
        result.fail("modified_at is None after resolve")
    if ca is not None and ma is not None and ma < ca:
        result.fail(f"modified_at ({ma}) < created_at ({ca})")
    if expect_source is not None and doc.get("_source") != expect_source:
        result.fail(f"Expected _source={expect_source!r}, got {doc.get('_source')!r}")


def validate_list_contents(doc: dict[str, Any], result: TestResult) -> None:
    for key in ("genre_ids", "genres", "cast_ids", "cast_names", "cast", "keywords", "origin_country"):
        val = doc.get(key, [])
        if not isinstance(val, list):
            continue
        for i, item in enumerate(val):
            if not isinstance(item, str):
                result.fail(f"{key}[{i}] should be str, got {type(item).__name__}")
                break


# ---------- Data loading ----------


def load_sample_items(
    media_type: str, count: int, *, recent: bool = True
) -> list[dict[str, Any]]:
    """Load sample items from cache files.

    Args:
        media_type: "movie" or "tv"
        count: Number of items to load
        recent: If True, pick from the most recent files (better API coverage).
    """
    subdir = DATA_DIR / ("movie" if media_type == "movie" else "tv")
    files = sorted(subdir.glob("*.json"), reverse=recent)
    items: list[dict[str, Any]] = []
    for fp in files:
        if len(items) >= count:
            break
        with open(fp) as fh:
            data = json.load(fh)
        results = data.get("results", []) if isinstance(data, dict) else data
        for item in results:
            items.append(item)
            if len(items) >= count:
                break
    return items


# ---------- API enrichment ----------


async def enrich_item(
    item: dict[str, Any],
    content_type: str,
    service: TMDBService,
) -> dict[str, Any]:
    """Call TMDB APIs to populate us_rating and watch_providers on a raw item."""
    tmdb_id = int(item.get("source_id") or item.get("tmdb_id") or item.get("id") or 0)
    if tmdb_id == 0:
        return item

    enriched = dict(item)

    try:
        rating_result = await service.get_content_rating(tmdb_id, "US", content_type)
        enriched["us_rating"] = rating_result.get("rating") if rating_result else None
    except Exception as e:
        logger.warning("get_content_rating failed for tmdb_id=%s: %s", tmdb_id, e)
        enriched["us_rating"] = None

    try:
        wp_result = await get_streaming_platform_summary_for_title(
            tmdb_id, content_type, "US"
        )
        enriched["watch_providers"] = wp_result
    except Exception as e:
        logger.warning("get_streaming_platform_summary failed for tmdb_id=%s: %s", tmdb_id, e)
        enriched["watch_providers"] = None

    return enriched


# Pinned TMDB IDs known to have content ratings
PINNED_TV_ITEMS: list[dict[str, Any]] = [
    {
        "mc_type": "tv",
        "mc_id": "tmdb_tv_224372",
        "source_id": "224372",
        "tmdb_id": 224372,
        "source": "tmdb",
        "name": "A Knight of the Seven Kingdoms",
        "first_air_date": "2026-01-18",
        "last_air_date": "2026-02-22",
        "genre_ids": [18, 10765, 10759],
        "genres": ["Drama", "Sci-Fi & Fantasy", "Action & Adventure"],
        "overview": "A century before the events of Game of Thrones.",
        "popularity": 500.0,
        "vote_average": 7.5,
        "vote_count": 2000,
        "images": [],
        "metrics": {"popularity": 500.0, "vote_average": 7.5, "vote_count": 2000},
        "main_cast": [],
        "keywords": [],
        "origin_country": ["US"],
    },
]


# ---------- Test cases ----------


async def test_movie_full_integration(
    items: list[dict[str, Any]],
    service: TMDBService,
    verbose: bool,
) -> list[TestResult]:
    """Movie items: enrich via API, normalize, validate full schema."""
    results: list[TestResult] = []
    for i, item in enumerate(items):
        mc_id = item.get("mc_id", "?")
        r = TestResult(f"movie_integration [{i}] id={mc_id}")

        enriched = await enrich_item(item, "movie", service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue

        doc._source = "backfill"
        redis_doc = document_to_redis(doc)
        now_ts = 1709000000
        ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src

        validate_schema_keys(redis_doc, r)
        validate_movie_specifics(redis_doc, r)
        validate_director_shape(redis_doc, r)
        validate_watch_providers_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")

        if verbose:
            print(f"    {'OK' if r.passed else 'FAIL'}: {redis_doc['id']} — "
                  f"title={redis_doc['title']!r}, "
                  f"release_date={redis_doc.get('release_date')}, "
                  f"us_rating={redis_doc.get('us_rating')!r}, "
                  f"director={redis_doc.get('director')}, "
                  f"wp_keys={list(redis_doc['watch_providers'].keys()) if redis_doc.get('watch_providers') else None}")

        results.append(r)
    return results


async def test_pinned_tv_integration(
    service: TMDBService,
    verbose: bool,
) -> list[TestResult]:
    """Pinned TV items with known content ratings: enrich, normalize, validate."""
    results: list[TestResult] = []
    for i, item in enumerate(PINNED_TV_ITEMS):
        mc_id = item.get("mc_id", "?")
        r = TestResult(f"pinned_tv_integration [{i}] id={mc_id}")

        enriched = await enrich_item(item, "tv", service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue

        redis_doc = document_to_redis(doc)
        now_ts = 1709000000
        ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src

        validate_schema_keys(redis_doc, r)
        validate_tv_specifics(redis_doc, r)
        validate_watch_providers_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")

        if redis_doc.get("us_rating") is None:
            r.fail(f"Pinned TV {mc_id} should have a us_rating but got None")

        if verbose:
            print(f"    {'OK' if r.passed else 'FAIL'}: {redis_doc['id']} — "
                  f"title={redis_doc['title']!r}, "
                  f"first_air_date={redis_doc.get('first_air_date')}, "
                  f"us_rating={redis_doc.get('us_rating')!r}, "
                  f"wp_keys={list(redis_doc['watch_providers'].keys()) if redis_doc.get('watch_providers') else None}")

        results.append(r)
    return results


async def test_tv_full_integration(
    items: list[dict[str, Any]],
    service: TMDBService,
    verbose: bool,
) -> list[TestResult]:
    """TV items: enrich via API, normalize, validate full schema."""
    results: list[TestResult] = []
    for i, item in enumerate(items):
        mc_id = item.get("mc_id", "?")
        r = TestResult(f"tv_integration [{i}] id={mc_id}")

        enriched = await enrich_item(item, "tv", service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue

        redis_doc = document_to_redis(doc)
        now_ts = 1709000000
        ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src

        validate_schema_keys(redis_doc, r)
        validate_tv_specifics(redis_doc, r)
        validate_watch_providers_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")

        if verbose:
            print(f"    {'OK' if r.passed else 'FAIL'}: {redis_doc['id']} — "
                  f"title={redis_doc['title']!r}, "
                  f"first_air_date={redis_doc.get('first_air_date')}, "
                  f"us_rating={redis_doc.get('us_rating')!r}, "
                  f"wp_keys={list(redis_doc['watch_providers'].keys()) if redis_doc.get('watch_providers') else None}")

        results.append(r)
    return results


def test_timestamps_new_doc() -> TestResult:
    r = TestResult("timestamps_new_doc")
    now_ts = 1709000000
    ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
    if ca != now_ts:
        r.fail(f"New doc created_at should be {now_ts}, got {ca}")
    if ma != now_ts:
        r.fail(f"New doc modified_at should be {now_ts}, got {ma}")
    if src != "backfill":
        r.fail(f"_source should be 'backfill', got {src!r}")
    return r


def test_timestamps_existing_with_created_at() -> TestResult:
    r = TestResult("timestamps_existing_with_created_at")
    now_ts = 1709000000
    existing = {"created_at": 1700000000, "modified_at": 1700000000}
    ca, ma, src = resolve_timestamps(existing, now_ts)
    if ca != 1700000000:
        r.fail(f"Should preserve existing created_at=1700000000, got {ca}")
    if ma != now_ts:
        r.fail(f"modified_at should be {now_ts}, got {ma}")
    if src is not None:
        r.fail(f"_source should be None when no tag given, got {src!r}")
    return r


def test_timestamps_existing_without_created_at() -> TestResult:
    r = TestResult("timestamps_existing_missing_created_at")
    now_ts = 1709000000
    existing: dict[str, Any] = {"mc_type": "movie"}
    ca, ma, _ = resolve_timestamps(existing, now_ts, source_tag="backfill")
    if ca != BACKFILL_DEFAULT_TS:
        r.fail(f"Should use BACKFILL_DEFAULT_TS={BACKFILL_DEFAULT_TS}, got {ca}")
    if ma != now_ts:
        r.fail(f"modified_at should be {now_ts}, got {ma}")
    return r


async def test_us_rating_populated(
    items: list[dict[str, Any]],
    content_type: str,
    service: TMDBService,
) -> TestResult:
    """At least one item returns a non-null us_rating from the API."""
    label = "movie" if content_type == "movie" else "tv"
    mc_type = MCType.MOVIE if content_type == "movie" else MCType.TV_SERIES
    r = TestResult(f"{label}_us_rating_populated")
    found = False
    for item in items:
        enriched = await enrich_item(item, content_type, service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=mc_type)
        if doc and doc.us_rating:
            r.warn(f"Found us_rating={doc.us_rating!r} for {doc.id}")
            found = True
            break
    if not found:
        r.fail(f"No {label} item returned a non-null us_rating from TMDB API")
    return r


async def test_watch_providers_populated(
    items: list[dict[str, Any]],
    content_type: str,
    service: TMDBService,
) -> TestResult:
    """At least one item returns a non-null watch_providers from the API."""
    label = "movie" if content_type == "movie" else "tv"
    mc_type = MCType.MOVIE if content_type == "movie" else MCType.TV_SERIES
    r = TestResult(f"{label}_watch_providers_populated")
    found = False
    for item in items:
        enriched = await enrich_item(item, content_type, service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=mc_type)
        if doc and doc.watch_providers:
            r.warn(f"Found watch_providers for {doc.id} with region={doc.watch_providers.get('watch_region')}")
            found = True
            break
    if not found:
        r.warn(f"No {label} returned watch_providers (may be expected for older titles)")
    return r


def test_movie_release_date_populated(items: list[dict[str, Any]]) -> TestResult:
    r = TestResult("movie_release_date_populated")
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc and doc.release_date:
            return r
    r.fail("No movie item had a non-null release_date from cache data")
    return r


def test_tv_air_dates_populated(items: list[dict[str, Any]]) -> TestResult:
    r = TestResult("tv_first_air_date_populated")
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc and doc.first_air_date:
            return r
    r.fail("No TV item had a non-null first_air_date from cache data")
    return r


def test_movie_director_populated(items: list[dict[str, Any]]) -> TestResult:
    r = TestResult("movie_director_populated")
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc and doc.director is not None:
            return r
    r.fail("No movie item yielded a non-null director from cache data")
    return r


def test_document_to_redis_roundtrip_completeness() -> TestResult:
    from core.normalize import SearchDocument

    r = TestResult("document_to_redis_completeness")
    doc = SearchDocument(
        id="tmdb_movie_99999",
        search_title="Test Movie",
        mc_type=MCType.MOVIE,
        mc_subtype=None,
        source=MCSources.TMDB,
        source_id="99999",
        year=2024,
        popularity=50.0,
        rating=7.5,
        image="https://example.com/img.jpg",
        overview="A test movie",
        genre_ids=["18"],
        genres=["drama"],
        cast_ids=["111"],
        cast_names=["test_actor"],
        cast=["Test Actor"],
        director={"id": "222", "name": "Test Director", "name_normalized": "test_director"},
        keywords=["test"],
        origin_country=["us"],
        release_date="2024-01-01",
        first_air_date=None,
        last_air_date=None,
        us_rating="PG",
        watch_providers={
            "streaming_platform_ids": [], "primary_provider": None,
            "primary_provider_id": None, "primary_provider_type": None,
            "watch_region": "US", "streaming_platforms": [],
            "on_demand_platform_ids": [], "on_demand_platforms": [],
        },
        created_at=1700000000,
        modified_at=1709000000,
        _source="backfill",
    )
    redis_doc = document_to_redis(doc)
    for key in MEDIA_REQUIRED_KEYS:
        if key not in redis_doc:
            r.fail(f"document_to_redis missing key: {key}")
    return r


# ---------- Runner ----------


async def run_tests(args: argparse.Namespace) -> int:
    service = TMDBService()

    movie_items = load_sample_items("movie", args.count)
    tv_items = load_sample_items("tv", args.count)

    print(f"\nLoaded {len(movie_items)} movie items, {len(tv_items)} TV items from cache")
    print("Enriching via TMDB API (get_content_rating + get_streaming_platform_summary)...\n")

    all_results: list[TestResult] = []
    all_enriched_docs: list[dict[str, Any]] = []

    # Tests 1-N: Movie full integration (cache + API enrich + normalize + validate)
    movie_results = await test_movie_full_integration(movie_items, service, args.verbose)
    all_results.extend(movie_results)

    # Pinned TV integration (known to have content ratings)
    pinned_tv_results = await test_pinned_tv_integration(service, args.verbose)
    all_results.extend(pinned_tv_results)

    # Tests N+1-2N: TV full integration (from cache)
    tv_results = await test_tv_full_integration(tv_items, service, args.verbose)
    all_results.extend(tv_results)

    # Timestamp unit tests
    all_results.append(test_timestamps_new_doc())
    all_results.append(test_timestamps_existing_with_created_at())
    all_results.append(test_timestamps_existing_without_created_at())

    # Data population checks (cache fields)
    all_results.append(test_movie_release_date_populated(movie_items))
    all_results.append(test_tv_air_dates_populated(tv_items))
    all_results.append(test_movie_director_populated(movie_items))

    # API field population checks
    all_results.append(await test_us_rating_populated(movie_items, "movie", service))
    all_results.append(await test_us_rating_populated(PINNED_TV_ITEMS + tv_items, "tv", service))
    all_results.append(await test_watch_providers_populated(movie_items, "movie", service))
    all_results.append(await test_watch_providers_populated(tv_items, "tv", service))

    # Roundtrip completeness
    all_results.append(test_document_to_redis_roundtrip_completeness())

    # Print results
    print()
    print("=" * 70)
    print(f"  BACKFILL INTEGRATION TEST — {len(all_results)} test cases")
    print("=" * 70)
    for r in all_results:
        print(r)
    print("=" * 70)

    passed = sum(1 for r in all_results if r.passed)
    failed = sum(1 for r in all_results if not r.passed)
    print(f"\n  {passed} passed, {failed} failed out of {len(all_results)} tests\n")

    if args.output:
        for item in PINNED_TV_ITEMS:
            enriched = await enrich_item(item, "tv", service)
            doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
            if doc:
                redis_doc = document_to_redis(doc)
                ca, ma, src = resolve_timestamps(None, 1709000000, source_tag="backfill")
                redis_doc["created_at"] = ca
                redis_doc["modified_at"] = ma
                redis_doc["_source"] = src
                all_enriched_docs.append(redis_doc)
        for item in movie_items:
            enriched = await enrich_item(item, "movie", service)
            doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.MOVIE)
            if doc:
                doc._source = "backfill"
                redis_doc = document_to_redis(doc)
                ca, ma, src = resolve_timestamps(None, 1709000000, source_tag="backfill")
                redis_doc["created_at"] = ca
                redis_doc["modified_at"] = ma
                redis_doc["_source"] = src
                all_enriched_docs.append(redis_doc)
        for item in tv_items:
            enriched = await enrich_item(item, "tv", service)
            doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
            if doc:
                redis_doc = document_to_redis(doc)
                ca, ma, src = resolve_timestamps(None, 1709000000, source_tag="backfill")
                redis_doc["created_at"] = ca
                redis_doc["modified_at"] = ma
                redis_doc["_source"] = src
                all_enriched_docs.append(redis_doc)
        out_path = Path(args.output)
        out_path.write_text(json.dumps(all_enriched_docs, indent=2))
        print(f"  Wrote {len(all_enriched_docs)} enriched sample docs to {out_path}\n")

    return 1 if failed > 0 else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill integration test (no Redis writes)")
    parser.add_argument("--verbose", action="store_true", help="Print per-doc details")
    parser.add_argument("--output", type=str, default=None, help="Write enriched docs to JSON file")
    parser.add_argument("--count", type=int, default=5, help="Sample items per type (default 5)")
    args = parser.parse_args()

    exit_code = asyncio.run(run_tests(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
