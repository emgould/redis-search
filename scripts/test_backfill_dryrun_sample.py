#!/usr/bin/env python3
"""
Full integration test for backfill normalization and document shape + data fidelity.

Four test groups:
  1. 5 movies from historical cached TMDB data (Phase 1 path)
  2. 5 TV shows from historical cached TMDB data (Phase 1 path)
  3. 5 movies from Redis docs year >= 2026 (Phase 1b path)
  4. 5 TV shows from Redis docs year >= 2026 (Phase 1b path)

Every normalized document is compared against its source dict to verify that
cast, keywords, image, overview, popularity, rating, genres, and director
survive the normalization pipeline.

ZERO writes to Redis. Reads from Redis and makes real API calls to TMDB.

Usage:
    python scripts/test_backfill_dryrun_sample.py
    python scripts/test_backfill_dryrun_sample.py --verbose
    python scripts/test_backfill_dryrun_sample.py --output /tmp/backfill_sample.json
    python scripts/test_backfill_dryrun_sample.py --count 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402
from redis.commands.search.query import Query as SearchQuery  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
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


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


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


# ---------- Data fidelity validation ----------


def _source_cast_entries(source: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract named cast entries from source dict."""
    main_cast = source.get("main_cast", [])
    if not main_cast:
        tmdb_cast = source.get("tmdb_cast", {})
        if isinstance(tmdb_cast, dict):
            main_cast = tmdb_cast.get("cast", [])
    return [a for a in main_cast if isinstance(a, dict) and a.get("name")]


def _source_has_medium_poster(source: dict[str, Any]) -> bool:
    """Check if source has a medium poster image."""
    for img in source.get("images", []):
        if (
            isinstance(img, dict)
            and img.get("key") == "medium"
            and img.get("description") == "poster"
        ):
            return True
    return False


def _source_has_director(source: dict[str, Any]) -> bool:
    """Check if source has a director with an ID."""
    candidate = source.get("director", {})
    if isinstance(candidate, dict) and candidate.get("id"):
        return True
    tmdb_cast = source.get("tmdb_cast", {})
    if isinstance(tmdb_cast, dict):
        candidate = tmdb_cast.get("director", {})
        if isinstance(candidate, dict) and candidate.get("id"):
            return True
    return False


def validate_data_fidelity(
    source: dict[str, Any],
    redis_doc: dict[str, Any],
    result: TestResult,
) -> None:
    """Compare normalized output against source dict to verify data survives the pipeline."""
    src_title = source.get("title") or source.get("name") or ""
    if src_title and not redis_doc.get("title"):
        result.fail(f"Source has title={src_title!r} but normalized doc has no title")

    src_overview = source.get("overview") or ""
    if src_overview and not redis_doc.get("overview"):
        result.fail("Source has overview but normalized doc overview is None")

    if _source_has_medium_poster(source) and not redis_doc.get("image"):
        result.fail("Source has medium poster image but normalized doc image is None")

    named_cast = _source_cast_entries(source)
    if named_cast and not redis_doc.get("cast"):
        result.fail(
            f"Source has {len(named_cast)} cast members but normalized doc has empty cast"
        )

    src_keywords = source.get("keywords", [])
    if src_keywords and not redis_doc.get("keywords"):
        result.fail(
            f"Source has {len(src_keywords)} keywords but normalized doc has empty keywords"
        )

    metrics = source.get("metrics", {})
    src_pop = metrics.get("popularity") or source.get("popularity") or 0
    if src_pop > 1.0 and redis_doc.get("popularity", 0) == 0:
        result.fail(f"Source popularity={src_pop} but normalized doc popularity is 0")

    src_vote = metrics.get("vote_average") or source.get("vote_average") or 0
    if src_vote > 0 and redis_doc.get("rating", 0) == 0:
        result.fail(f"Source vote_average={src_vote} but normalized doc rating is 0")

    src_genre_ids = source.get("genre_ids", [])
    src_genres = source.get("genres", [])
    if (src_genre_ids or src_genres) and not redis_doc.get("genre_ids") and not redis_doc.get("genres"):
        result.fail("Source has genres but normalized doc has empty genre_ids and genres")

        if redis_doc.get("mc_type") == "movie" and _source_has_director(source) and redis_doc.get("director") is None:
            result.fail("Source has director but normalized doc director is None")

    src_countries = source.get("origin_country", [])
    if src_countries and not redis_doc.get("origin_country"):
        result.fail(
            f"Source has origin_country={src_countries} but normalized doc has empty list"
        )


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


async def load_redis_2026_items(
    mc_type_filter: str,
    count: int,
    year_gte: int = 2026,
) -> list[dict[str, Any]]:
    """Query idx:media for docs with year >= year_gte, return random sample.

    Uses FT.SEARCH on the media index — fast even with millions of keys.
    Returns dicts with at least {source_id, mc_type, year, title}.
    """
    redis = _connect_redis()
    candidates: list[dict[str, Any]] = []
    try:
        await redis.ping()  # type: ignore[misc]

        query_str = f"@year:[{year_gte} +inf] @mc_type:{{{mc_type_filter}}}"
        q = SearchQuery(query_str).paging(0, 500).no_content()
        result = await redis.ft("idx:media").search(q)

        keys = [doc.id for doc in result.docs]
        if not keys:
            return []

        pipe = redis.pipeline()
        for key in keys:
            pipe.json().get(key)
        docs: list[object] = await pipe.execute()

        for doc in docs:
            if isinstance(doc, dict) and doc.get("source_id"):
                candidates.append(doc)

    finally:
        await redis.aclose()

    if len(candidates) <= count:
        return candidates
    return random.sample(candidates, count)


# ---------- API enrichment (Phase 1 path) ----------


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
        mc_type_enum = MCType.MOVIE if content_type == "movie" else MCType.TV_SERIES
        raw_wp = await service._get_watch_providers(tmdb_id, mc_type_enum, region="US")
        enriched["watch_providers"] = await service.custom_watch_provider(
            tmdb_id, content_type,
            watch_providers=raw_wp.get("watch_providers", {}),
            watch_region="US",
        )
    except Exception as e:
        logger.warning("watch_provider enrichment failed for tmdb_id=%s: %s", tmdb_id, e)
        enriched["watch_providers"] = None

    return enriched


def _apply_timestamps(redis_doc: dict[str, Any]) -> None:
    """Apply backfill timestamps in-place."""
    now_ts = int(time.time())
    ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
    redis_doc["created_at"] = ca
    redis_doc["modified_at"] = ma
    redis_doc["_source"] = src


# ---------- Integration test cases ----------


async def test_cache_integration(
    items: list[dict[str, Any]],
    content_type: str,
    service: TMDBService,
    verbose: bool,
    collected_docs: list[dict[str, Any]],
) -> list[TestResult]:
    """Phase 1 path: cache dict -> enrich (us_rating + watch_providers) -> normalize -> validate."""
    mc_type = MCType.MOVIE if content_type == "movie" else MCType.TV_SERIES
    results: list[TestResult] = []

    for i, item in enumerate(items):
        mc_id = item.get("mc_id", "?")
        r = TestResult(f"cache_{content_type} [{i}] id={mc_id}")

        enriched = await enrich_item(item, content_type, service)
        doc = normalize_document(enriched, source=MCSources.TMDB, mc_type=mc_type)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue

        doc._source = "backfill"
        redis_doc = document_to_redis(doc)
        _apply_timestamps(redis_doc)

        validate_schema_keys(redis_doc, r)
        if content_type == "movie":
            validate_movie_specifics(redis_doc, r)
            validate_director_shape(redis_doc, r)
        else:
            validate_tv_specifics(redis_doc, r)
        validate_watch_providers_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")
        validate_data_fidelity(enriched, redis_doc, r)

        collected_docs.append(redis_doc)

        if verbose:
            cast_count = len(redis_doc.get("cast", []))
            kw_count = len(redis_doc.get("keywords", []))
            print(
                f"    {'OK' if r.passed else 'FAIL'}: {redis_doc['id']} — "
                f"title={redis_doc['title']!r}, "
                f"cast={cast_count}, kw={kw_count}, "
                f"img={'yes' if redis_doc.get('image') else 'no'}, "
                f"us_rating={redis_doc.get('us_rating')!r}, "
                f"dir={redis_doc.get('director') is not None}"
            )

        results.append(r)
    return results


async def test_phase1b_redis_sourced(
    redis_items: list[dict[str, Any]],
    content_type: str,
    service: TMDBService,
    verbose: bool,
    collected_docs: list[dict[str, Any]],
) -> list[TestResult]:
    """Phase 1b path: Redis doc -> get_media_details -> model_dump -> normalize -> validate.

    get_media_details returns fully enriched data (us_rating, watch_providers, cast, etc.).
    Tests the exact path used by phase1b() in the backfill script.
    """
    mc_type_enum = MCType.MOVIE if content_type == "movie" else MCType.TV_SERIES
    results: list[TestResult] = []

    for i, redis_item in enumerate(redis_items):
        tmdb_id = int(redis_item.get("source_id", 0))
        title = redis_item.get("title", "?")
        r = TestResult(f"phase1b_{content_type} [{i}] tmdb_id={tmdb_id} title={title!r}")

        if tmdb_id == 0:
            r.fail("Redis doc missing source_id")
            results.append(r)
            continue

        try:
            api_result = await service.get_media_details(
                tmdb_id,
                mc_type_enum,
                include_cast=True,
                include_videos=True,
                include_watch_providers=True,
                include_keywords=True,
            )
        except Exception as e:
            r.fail(f"get_media_details raised: {e}")
            results.append(r)
            continue

        if api_result is None:
            r.fail("get_media_details returned None")
            results.append(r)
            continue

        if hasattr(api_result, "model_dump"):
            item_dict: dict[str, Any] = api_result.model_dump(mode="json")
        elif isinstance(api_result, dict):
            item_dict = api_result
        else:
            r.fail(f"Unexpected result type: {type(api_result)}")
            results.append(r)
            continue

        if not item_dict or item_dict.get("status_code") == 404:
            r.fail("Empty or 404 from API")
            results.append(r)
            continue

        if item_dict.get("error"):
            r.fail(f"API returned error: {item_dict['error']}")
            results.append(r)
            continue

        doc = normalize_document(item_dict)
        if doc is None:
            r.fail("normalize_document returned None on API-fetched data")
            results.append(r)
            continue

        doc._source = "backfill"
        redis_doc = document_to_redis(doc)
        _apply_timestamps(redis_doc)

        validate_schema_keys(redis_doc, r)
        if content_type == "movie":
            validate_movie_specifics(redis_doc, r)
            validate_director_shape(redis_doc, r)
        else:
            validate_tv_specifics(redis_doc, r)
        validate_watch_providers_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")
        validate_data_fidelity(item_dict, redis_doc, r)

        collected_docs.append(redis_doc)

        if verbose:
            cast_count = len(redis_doc.get("cast", []))
            kw_count = len(redis_doc.get("keywords", []))
            print(
                f"    {'OK' if r.passed else 'FAIL'}: {redis_doc['id']} — "
                f"title={redis_doc['title']!r}, "
                f"cast={cast_count}, kw={kw_count}, "
                f"img={'yes' if redis_doc.get('image') else 'no'}, "
                f"us_rating={redis_doc.get('us_rating')!r}, "
                f"dir={redis_doc.get('director') is not None}"
            )

        results.append(r)

    return results


# ---------- Unit tests ----------


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
        if content_type == "tv":
            r.warn(f"No {label} item returned a non-null us_rating (new TV shows may lack ratings)")
        else:
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
    all_results: list[TestResult] = []
    all_docs: list[dict[str, Any]] = []

    # --- Load data ---
    movie_items = load_sample_items("movie", args.count)
    tv_items = load_sample_items("tv", args.count)
    print("\n--- Cache data ---")
    print(f"Loaded {len(movie_items)} movie items, {len(tv_items)} TV items from cache files")

    print("\n--- Redis 2026+ docs ---")
    redis_movies = await load_redis_2026_items("movie", args.count)
    redis_tv = await load_redis_2026_items("tv", args.count)
    print(f"Loaded {len(redis_movies)} movie items, {len(redis_tv)} TV items from Redis (year >= 2026)")

    if not redis_movies:
        print("  WARNING: No 2026+ movies found in Redis — Phase 1b movie tests will be skipped")
    if not redis_tv:
        print("  WARNING: No 2026+ TV shows found in Redis — Phase 1b TV tests will be skipped")

    print("\nRunning tests...\n")

    # --- Group 1: Movies from cache (Phase 1 path) ---
    print("  Group 1: Movies from cache (Phase 1 path)")
    all_results.extend(
        await test_cache_integration(movie_items, "movie", service, args.verbose, all_docs)
    )

    # --- Group 2: TV from cache (Phase 1 path) ---
    print("  Group 2: TV shows from cache (Phase 1 path)")
    all_results.extend(
        await test_cache_integration(tv_items, "tv", service, args.verbose, all_docs)
    )

    # --- Group 3: Movies from Redis 2026+ (Phase 1b path) ---
    print("  Group 3: Movies from Redis 2026+ (Phase 1b path)")
    all_results.extend(
        await test_phase1b_redis_sourced(redis_movies, "movie", service, args.verbose, all_docs)
    )

    # --- Group 4: TV from Redis 2026+ (Phase 1b path) ---
    print("  Group 4: TV shows from Redis 2026+ (Phase 1b path)")
    all_results.extend(
        await test_phase1b_redis_sourced(redis_tv, "tv", service, args.verbose, all_docs)
    )

    # --- Unit tests ---
    all_results.append(test_timestamps_new_doc())
    all_results.append(test_timestamps_existing_with_created_at())
    all_results.append(test_timestamps_existing_without_created_at())

    # Data population checks (cache fields)
    all_results.append(test_movie_release_date_populated(movie_items))
    all_results.append(test_tv_air_dates_populated(tv_items))
    all_results.append(test_movie_director_populated(movie_items))

    # API field population checks (cache-based)
    all_results.append(await test_us_rating_populated(movie_items, "movie", service))
    all_results.append(await test_us_rating_populated(tv_items, "tv", service))
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
        out_path = Path(args.output)
        out_path.write_text(json.dumps(all_docs, indent=2))
        print(f"  Wrote {len(all_docs)} normalized docs (all groups) to {out_path}\n")

    return 1 if failed > 0 else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill integration test (no Redis writes)")
    parser.add_argument("--verbose", action="store_true", help="Print per-doc details")
    parser.add_argument("--output", type=str, default=None, help="Write normalized docs to JSON file")
    parser.add_argument("--count", type=int, default=5, help="Sample items per type (default 5)")
    args = parser.parse_args()

    exit_code = asyncio.run(run_tests(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
