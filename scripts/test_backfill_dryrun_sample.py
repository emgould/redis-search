#!/usr/bin/env python3
"""
Dry-run test for backfill normalization and document shape validation.

Loads a small sample of cache data (movie + TV), runs through the full
normalize_document → document_to_redis pipeline, and validates the output
matches the canonical schema defined in media-index-enhancements-plan.mdc.

ZERO writes to Redis. This is a pure normalization + shape validation test.

Usage:
    python scripts/test_backfill_dryrun_sample.py
    python scripts/test_backfill_dryrun_sample.py --verbose
    python scripts/test_backfill_dryrun_sample.py --output /tmp/backfill_sample.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from contracts.models import MCSources, MCType  # noqa: E402
from core.normalize import (  # noqa: E402
    BACKFILL_DEFAULT_TS,
    document_to_redis,
    normalize_document,
    resolve_timestamps,
)

DATA_DIR = Path("data/us")

# ---- Canonical schema: every field and its expected type(s) ----

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

    def fail(self, msg: str) -> None:
        self.passed = False
        self.errors.append(msg)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        out = f"  [{status}] {self.name}"
        for err in self.errors:
            out += f"\n         {err}"
        return out


# ---------- Shape validation ----------


def validate_schema_keys(doc: dict[str, Any], result: TestResult) -> None:
    """Verify all expected keys exist and have correct types."""
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
    """If director is present, validate its sub-keys."""
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


def validate_movie_specifics(doc: dict[str, Any], result: TestResult) -> None:
    """Movie-specific shape rules."""
    if doc.get("mc_type") != "movie":
        result.fail(f"Expected mc_type='movie', got {doc.get('mc_type')!r}")
    if doc.get("first_air_date") is not None:
        result.fail(f"Movie should have first_air_date=None, got {doc['first_air_date']!r}")
    if doc.get("last_air_date") is not None:
        result.fail(f"Movie should have last_air_date=None, got {doc['last_air_date']!r}")


def validate_tv_specifics(doc: dict[str, Any], result: TestResult) -> None:
    """TV-specific shape rules."""
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
    """Validate created_at/modified_at/source after resolve_timestamps."""
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
    """Verify list fields contain strings (not nested dicts/lists)."""
    for key in ("genre_ids", "genres", "cast_ids", "cast_names", "cast", "keywords", "origin_country"):
        val = doc.get(key, [])
        if not isinstance(val, list):
            continue
        for i, item in enumerate(val):
            if not isinstance(item, str):
                result.fail(f"{key}[{i}] should be str, got {type(item).__name__}")
                break


# ---------- Data loading ----------


def load_sample_items(media_type: str, count: int) -> list[dict[str, Any]]:
    """Load a sample of items from cache files."""
    subdir = DATA_DIR / ("movie" if media_type == "movie" else "tv")
    files = sorted(subdir.glob("*.json"))
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


# ---------- Test cases ----------


def test_movie_cache_normalization(items: list[dict[str, Any]], verbose: bool) -> list[TestResult]:
    """Test: Movie cache items normalize with correct schema."""
    results: list[TestResult] = []
    for i, item in enumerate(items):
        r = TestResult(f"movie_cache_normalize [{i}] id={item.get('mc_id', '?')}")
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue
        redis_doc = document_to_redis(doc)
        validate_schema_keys(redis_doc, r)
        validate_movie_specifics(redis_doc, r)
        validate_director_shape(redis_doc, r)
        validate_list_contents(redis_doc, r)
        if verbose and r.passed:
            print(f"    OK: {redis_doc['id']} — title={redis_doc['title']!r}, "
                  f"release_date={redis_doc.get('release_date')}, "
                  f"director={redis_doc.get('director')}")
        results.append(r)
    return results


def test_tv_cache_normalization(items: list[dict[str, Any]], verbose: bool) -> list[TestResult]:
    """Test: TV cache items normalize with correct schema."""
    results: list[TestResult] = []
    for i, item in enumerate(items):
        r = TestResult(f"tv_cache_normalize [{i}] id={item.get('mc_id', '?')}")
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue
        redis_doc = document_to_redis(doc)
        validate_schema_keys(redis_doc, r)
        validate_tv_specifics(redis_doc, r)
        validate_list_contents(redis_doc, r)
        if verbose and r.passed:
            print(f"    OK: {redis_doc['id']} — title={redis_doc['title']!r}, "
                  f"first_air_date={redis_doc.get('first_air_date')}, "
                  f"last_air_date={redis_doc.get('last_air_date')}")
        results.append(r)
    return results


def test_timestamps_new_doc() -> TestResult:
    """Test: resolve_timestamps for a brand-new document (no existing)."""
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
    """Test: resolve_timestamps preserves existing created_at."""
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
    """Test: resolve_timestamps falls back to BACKFILL_DEFAULT_TS."""
    r = TestResult("timestamps_existing_missing_created_at")
    now_ts = 1709000000
    existing: dict[str, Any] = {"mc_type": "movie"}
    ca, ma, _ = resolve_timestamps(existing, now_ts, source_tag="backfill")
    if ca != BACKFILL_DEFAULT_TS:
        r.fail(f"Should use BACKFILL_DEFAULT_TS={BACKFILL_DEFAULT_TS}, got {ca}")
    if ma != now_ts:
        r.fail(f"modified_at should be {now_ts}, got {ma}")
    return r


def test_movie_with_timestamps(items: list[dict[str, Any]]) -> list[TestResult]:
    """Test: Full pipeline — normalize + resolve_timestamps + source tag."""
    results: list[TestResult] = []
    now_ts = 1709000000
    for i, item in enumerate(items[:3]):
        r = TestResult(f"movie_full_pipeline_with_timestamps [{i}]")
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue
        doc._source = "backfill"
        redis_doc = document_to_redis(doc)
        ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src
        validate_schema_keys(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")
        validate_movie_specifics(redis_doc, r)
        results.append(r)
    return results


def test_tv_with_timestamps(items: list[dict[str, Any]]) -> list[TestResult]:
    """Test: Full pipeline — normalize + resolve_timestamps for TV."""
    results: list[TestResult] = []
    now_ts = 1709000000
    for i, item in enumerate(items[:3]):
        r = TestResult(f"tv_full_pipeline_with_timestamps [{i}]")
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc is None:
            r.fail("normalize_document returned None")
            results.append(r)
            continue
        redis_doc = document_to_redis(doc)
        ca, ma, src = resolve_timestamps(None, now_ts, source_tag="backfill")
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src
        validate_schema_keys(redis_doc, r)
        validate_timestamps(redis_doc, r, expect_source="backfill")
        validate_tv_specifics(redis_doc, r)
        results.append(r)
    return results


def test_movie_release_date_populated(items: list[dict[str, Any]]) -> TestResult:
    """Test: At least one movie from cache has a non-null release_date."""
    r = TestResult("movie_release_date_populated")
    found = False
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc and doc.release_date:
            found = True
            break
    if not found:
        r.fail("No movie item had a non-null release_date from cache data")
    return r


def test_tv_air_dates_populated(items: list[dict[str, Any]]) -> TestResult:
    """Test: At least one TV item has non-null first_air_date."""
    r = TestResult("tv_first_air_date_populated")
    found = False
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
        if doc and doc.first_air_date:
            found = True
            break
    if not found:
        r.fail("No TV item had a non-null first_air_date from cache data")
    return r


def test_movie_director_populated(items: list[dict[str, Any]]) -> TestResult:
    """Test: At least one movie from cache yields a director object."""
    r = TestResult("movie_director_populated")
    found = False
    for item in items:
        doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
        if doc and doc.director is not None:
            found = True
            break
    if not found:
        r.fail("No movie item yielded a non-null director from cache data")
    return r


def test_document_to_redis_roundtrip_completeness() -> TestResult:
    """Test: document_to_redis includes every SearchDocument field in output."""
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
        watch_providers={"streaming_platform_ids": [], "primary_provider": None,
                         "primary_provider_id": None, "primary_provider_type": None,
                         "watch_region": "US", "streaming_platforms": [],
                         "on_demand_platform_ids": [], "on_demand_platforms": []},
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run backfill shape validation")
    parser.add_argument("--verbose", action="store_true", help="Print passing doc details")
    parser.add_argument("--output", type=str, default=None, help="Write sample docs to JSON file")
    parser.add_argument("--count", type=int, default=5, help="Sample items per type (default 5)")
    args = parser.parse_args()

    movie_items = load_sample_items("movie", args.count)
    tv_items = load_sample_items("tv", args.count)

    print(f"\nLoaded {len(movie_items)} movie items, {len(tv_items)} TV items from cache\n")

    all_results: list[TestResult] = []

    # Test 1-5: Movie cache normalization (one per item)
    all_results.extend(test_movie_cache_normalization(movie_items, args.verbose))

    # Test 6-10: TV cache normalization (one per item)
    all_results.extend(test_tv_cache_normalization(tv_items, args.verbose))

    # Test 11: Timestamps — new document
    all_results.append(test_timestamps_new_doc())

    # Test 12: Timestamps — existing with created_at
    all_results.append(test_timestamps_existing_with_created_at())

    # Test 13: Timestamps — existing without created_at (backfill default)
    all_results.append(test_timestamps_existing_without_created_at())

    # Test 14-16: Movie full pipeline with timestamps
    all_results.extend(test_movie_with_timestamps(movie_items))

    # Test 17-19: TV full pipeline with timestamps
    all_results.extend(test_tv_with_timestamps(tv_items))

    # Test 20: Movie release_date populated
    all_results.append(test_movie_release_date_populated(movie_items))

    # Test 21: TV first_air_date populated
    all_results.append(test_tv_air_dates_populated(tv_items))

    # Test 22: Movie director populated
    all_results.append(test_movie_director_populated(movie_items))

    # Test 23: document_to_redis roundtrip completeness
    all_results.append(test_document_to_redis_roundtrip_completeness())

    # Print results
    print("=" * 65)
    print(f"  BACKFILL DRY-RUN VALIDATION — {len(all_results)} test cases")
    print("=" * 65)
    for r in all_results:
        print(r)
    print("=" * 65)

    passed = sum(1 for r in all_results if r.passed)
    failed = sum(1 for r in all_results if not r.passed)
    print(f"\n  {passed} passed, {failed} failed out of {len(all_results)} tests\n")

    if args.output:
        sample_docs: list[dict[str, Any]] = []
        for item in movie_items:
            doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.MOVIE)
            if doc:
                sample_docs.append(document_to_redis(doc))
        for item in tv_items:
            doc = normalize_document(item, source=MCSources.TMDB, mc_type=MCType.TV_SERIES)
            if doc:
                sample_docs.append(document_to_redis(doc))
        out_path = Path(args.output)
        out_path.write_text(json.dumps(sample_docs, indent=2))
        print(f"  Wrote {len(sample_docs)} sample docs to {out_path}\n")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
