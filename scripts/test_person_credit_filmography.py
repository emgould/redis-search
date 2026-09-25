#!/usr/bin/env python3
"""Acceptance checks for person exact-match filmography rewrite.

Builds the expected tv/movie lists from the person Redis doc
(``movie_credit_ids`` / ``tv_credit_ids``) by ``JSON.MGET`` of
``media:tmdb_{movie|tv}_{id}``, dropping keys that are not in the catalog
and sorting the hits by Redis popularity. Then compares that oracle to the
running web app.

Requires:
- Redis from ``config/local.env`` (or ``ENV_FILE``)
- Web app serving this branch, default ``http://localhost:9001``

Usage:
    make test-person-credit-filmography
    make test-person-credit-filmography PERSON_ID=10297
    python scripts/test_person_credit_filmography.py --person-id 10297 --limit 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from redis import Redis
from redis.exceptions import RedisError

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

DEFAULT_HOST = "http://localhost:9001"
DEFAULT_PERSON_ID = 10297  # Matthew McConaughey
AUTOCOMPLETE_LIMIT = 10
CATALOG_LIMIT = 50
SOURCES = "person,tv,movie"
IMPOSSIBLE_YEAR = 2099


@dataclass
class MediaHit:
    """One catalog media doc hydrated from a credit id."""

    mc_id: str
    credit_id: str
    title: str
    popularity: float


@dataclass
class Check:
    """One acceptance assertion."""

    name: str
    passed: bool
    detail: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--person-id",
        type=int,
        default=DEFAULT_PERSON_ID,
        help=f"TMDB person id (default {DEFAULT_PERSON_ID})",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"Web app base URL (default {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=AUTOCOMPLETE_LIMIT,
        help=f"Per-source limit for search/autocomplete parity (default {AUTOCOMPLETE_LIMIT})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default 30)",
    )
    return parser.parse_args()


def _connect_redis() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _load_person(redis: Redis, person_id: int) -> tuple[str, dict[str, Any]]:
    keys = [f"person:tmdb_person_{person_id}", f"person:person_{person_id}"]
    for key in keys:
        raw = redis.json().get(key)
        if isinstance(raw, dict):
            return key, raw
    raise SystemExit(
        f"No person doc for TMDB id {person_id}. Tried {', '.join(keys)}."
    )


def _credit_ids(doc: dict[str, Any], field: str) -> list[str]:
    raw = doc.get(field)
    if not isinstance(raw, list):
        return []
    ids: list[str] = []
    for item in raw:
        if item is None:
            continue
        value = str(item).strip()
        if value:
            ids.append(value)
    return ids


def _person_name(doc: dict[str, Any]) -> str:
    name = doc.get("name") or doc.get("search_title") or ""
    if not isinstance(name, str) or not name.strip():
        raise SystemExit("Person doc has no name or search_title.")
    return name.strip()


def _doc_from_mget(raw: object) -> dict[str, Any] | None:
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return raw[0]
    if isinstance(raw, dict):
        return raw
    return None


def _hydrate(
    redis: Redis,
    credit_ids: list[str],
    media_type: str,
) -> tuple[list[MediaHit], list[str]]:
    """Return popularity-sorted catalog hits and credit ids with no media key."""
    if not credit_ids:
        return [], []

    keys = [f"media:tmdb_{media_type}_{credit_id}" for credit_id in credit_ids]
    raw_docs = redis.json().mget(keys, "$")
    if not isinstance(raw_docs, list) or len(raw_docs) != len(credit_ids):
        raise SystemExit(f"JSON.MGET returned an unexpected payload for {media_type}.")

    hits: list[MediaHit] = []
    missing: list[str] = []
    for raw, credit_id in zip(raw_docs, credit_ids, strict=True):
        doc = _doc_from_mget(raw)
        if doc is None:
            missing.append(credit_id)
            continue
        mc_id = str(doc.get("mc_id") or f"tmdb_{media_type}_{credit_id}")
        title = str(doc.get("title") or doc.get("name") or doc.get("search_title") or mc_id)
        hits.append(
            MediaHit(
                mc_id=mc_id,
                credit_id=credit_id,
                title=title,
                popularity=float(doc.get("popularity") or 0),
            )
        )

    hits.sort(key=lambda hit: hit.popularity, reverse=True)
    return hits, missing


def _mc_id(item: object) -> str:
    if not isinstance(item, dict):
        return ""
    mc_id = item.get("mc_id") or item.get("id") or ""
    return str(mc_id)


def _title(item: object) -> str:
    if not isinstance(item, dict):
        return ""
    title = item.get("title") or item.get("name") or item.get("search_title") or ""
    return str(title)


def _bucket(payload: object, source: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw = payload.get(source)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _ids(items: list[dict[str, Any]]) -> list[str]:
    return [_mc_id(item) for item in items]


def _format_hits(hits: list[MediaHit]) -> str:
    if not hits:
        return "(none)"
    return "\n".join(
        f"    {hit.mc_id}  pop={hit.popularity:.3f}  {hit.title}" for hit in hits
    )


def _format_items(items: list[dict[str, Any]]) -> str:
    if not items:
        return "(none)"
    lines: list[str] = []
    for item in items:
        pop = item.get("popularity")
        pop_text = f"{float(pop):.3f}" if isinstance(pop, (int, float)) else "?"
        lines.append(f"    {_mc_id(item)}  pop={pop_text}  {_title(item)}")
    return "\n".join(lines)


def _id_diff(expected: list[str], actual: list[str]) -> str:
    return f"expected={expected}\nactual=  {actual}"


def _get_json(
    client: httpx.Client,
    host: str,
    path: str,
    params: dict[str, str | int],
) -> tuple[int, object]:
    response = client.get(f"{host.rstrip('/')}{path}", params=params)
    try:
        payload: object = response.json()
    except ValueError:
        payload = response.text
    return response.status_code, payload


def _parse_sse(body: str) -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    event_name = "message"
    data_lines: list[str] = []

    def _flush() -> None:
        nonlocal event_name, data_lines
        if not data_lines:
            event_name = "message"
            return
        raw = "\n".join(data_lines)
        parsed: object = json.loads(raw) if raw else {}
        if isinstance(parsed, dict):
            events.append((event_name, parsed))
        data_lines = []
        event_name = "message"

    for line in body.splitlines():
        if line.startswith("event:"):
            event_name = line.split(":", maxsplit=1)[1].strip()
        elif line.startswith("data:"):
            data_lines.append(line.split(":", maxsplit=1)[1].strip())
        elif line == "":
            _flush()
    _flush()
    return events


def _result_events(
    events: list[tuple[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    return [payload for name, payload in events if name == "result"]


def _check_catalog_match(
    name: str,
    expected: list[MediaHit],
    actual_items: list[dict[str, Any]],
) -> Check:
    expected_ids = [hit.mc_id for hit in expected]
    actual_ids = _ids(actual_items)
    if actual_ids == expected_ids:
        return Check(name, True, f"{len(actual_ids)} ids match the catalog hydrate")
    return Check(
        name,
        False,
        _id_diff(expected_ids, actual_ids)
        + "\nexpected detail:\n"
        + _format_hits(expected)
        + "\nactual detail:\n"
        + _format_items(actual_items),
    )


def _person_id_of(item: dict[str, Any]) -> str:
    source_id = item.get("source_id")
    if source_id is not None and str(source_id).strip():
        return str(source_id).strip()
    mc_id = _mc_id(item)
    tail = mc_id.rsplit("_", maxsplit=1)
    if len(tail) == 2:
        return tail[1]
    return ""


def _stream_bucket_ok(
    results: list[dict[str, Any]],
    source: str,
    expected: list[MediaHit],
) -> tuple[bool, str]:
    """A non-empty catalog bucket must be emitted exactly once and match ids."""
    events = [item for item in results if item.get("source") == source]
    expected_ids = [hit.mc_id for hit in expected]
    if not expected_ids:
        if events:
            return False, f"expected no {source} event, got {len(events)}"
        return True, "no catalog hits, no event"
    if len(events) != 1:
        return False, f"expected 1 {source} event, got {len(events)}"
    actual_ids = _ids(_bucket(events[0], "results"))
    if actual_ids != expected_ids:
        return False, f"ids differ expected={expected_ids} actual={actual_ids}"
    return True, f"{len(actual_ids)} ids match"


def _run(args: argparse.Namespace) -> int:
    if args.limit < 1 or args.limit > CATALOG_LIMIT:
        print(f"--limit must be between 1 and {CATALOG_LIMIT}", file=sys.stderr)
        return 2

    redis = _connect_redis()
    try:
        redis.ping()
    except RedisError as exc:
        print(f"Redis ping failed: {exc}", file=sys.stderr)
        return 2

    key, doc = _load_person(redis, args.person_id)
    name = _person_name(doc)
    movie_ids = _credit_ids(doc, "movie_credit_ids")
    tv_ids = _credit_ids(doc, "tv_credit_ids")
    if not isinstance(doc.get("movie_credit_ids"), list) or not isinstance(
        doc.get("tv_credit_ids"), list
    ):
        print(
            f"{key} is missing movie_credit_ids/tv_credit_ids. "
            "Run: make backfill-person-credit-ids REDIS=local "
            f"ARGS='--person-id {args.person_id}'",
            file=sys.stderr,
        )
        return 2
    if not movie_ids and not tv_ids:
        print(
            f"{key} has empty credit id lists, so search will not rewrite. "
            "Backfill this person before acceptance.",
            file=sys.stderr,
        )
        return 2

    movie_hits, movie_missing = _hydrate(redis, movie_ids, "movie")
    tv_hits, tv_missing = _hydrate(redis, tv_ids, "tv")
    redis.close()

    if not movie_hits and not tv_hits:
        print(
            f"{key} credit ids are not in the media catalog "
            f"(movies missing={len(movie_missing)}, tv missing={len(tv_missing)}).",
            file=sys.stderr,
        )
        return 2

    limit_hits_movie = movie_hits[: args.limit]
    limit_hits_tv = tv_hits[: args.limit]
    catalog_hits_movie = movie_hits[:CATALOG_LIMIT]
    catalog_hits_tv = tv_hits[:CATALOG_LIMIT]

    print("=" * 72)
    print("Person credit filmography acceptance")
    print("=" * 72)
    print(f"  person: {name} ({key})")
    print(f"  host:   {args.host}")
    print(
        f"  credits stored: movies={len(movie_ids)} tv={len(tv_ids)} | "
        f"in catalog: movies={len(movie_hits)} tv={len(tv_hits)} | "
        f"dropped (not indexed): movies={len(movie_missing)} tv={len(tv_missing)}"
    )
    print()

    checks: list[Check] = []
    base_params: dict[str, str | int] = {
        "q": name,
        "sources": SOURCES,
        "limit": args.limit,
    }

    with httpx.Client(timeout=args.timeout) as client:
        try:
            status, search_payload = _get_json(client, args.host, "/api/search", base_params)
        except httpx.HTTPError as exc:
            print(f"HTTP error calling {args.host}/api/search: {exc}", file=sys.stderr)
            return 2

        if status != 200 or not isinstance(search_payload, dict):
            print(f"/api/search failed status={status} body={search_payload}", file=sys.stderr)
            return 2

        search_tv = _bucket(search_payload, "tv")
        search_movie = _bucket(search_payload, "movie")
        search_people = _bucket(search_payload, "person")

        top_person = search_people[0] if search_people else None
        if top_person is None or _person_id_of(top_person) != str(args.person_id):
            checks.append(
                Check(
                    "search returns this person first",
                    False,
                    f"top person id={_person_id_of(top_person) if top_person else '(none)'} "
                    f"name={_title(top_person) if top_person else '(none)'}",
                )
            )
        else:
            checks.append(
                Check(
                    "search returns this person first",
                    True,
                    f"{_title(top_person)} id={args.person_id}",
                )
            )

        checks.append(
            _check_catalog_match(
                f"search tv matches catalog hydrate (limit={args.limit})",
                limit_hits_tv,
                search_tv,
            )
        )
        checks.append(
            _check_catalog_match(
                f"search movie matches catalog hydrate (limit={args.limit})",
                limit_hits_movie,
                search_movie,
            )
        )

        dropped_ids = {f"tmdb_tv_{cid}" for cid in tv_missing} | {
            f"tmdb_movie_{cid}" for cid in movie_missing
        }
        returned_ids = set(_ids(search_tv) + _ids(search_movie))
        leaked = sorted(returned_ids & dropped_ids)
        checks.append(
            Check(
                "unindexed credit ids are absent from search",
                not leaked,
                "none returned"
                if not leaked
                else f"returned unindexed ids: {leaked}",
            )
        )

        status, full_payload = _get_json(
            client,
            args.host,
            "/api/search",
            {"q": name, "sources": SOURCES, "limit": CATALOG_LIMIT},
        )
        if status != 200 or not isinstance(full_payload, dict):
            checks.append(Check("search limit=50", False, f"status={status}"))
        else:
            checks.append(
                _check_catalog_match(
                    "search tv matches full catalog hydrate (limit=50)",
                    catalog_hits_tv,
                    _bucket(full_payload, "tv"),
                )
            )
            checks.append(
                _check_catalog_match(
                    "search movie matches full catalog hydrate (limit=50)",
                    catalog_hits_movie,
                    _bucket(full_payload, "movie"),
                )
            )

        status, ac_payload = _get_json(
            client,
            args.host,
            "/api/autocomplete",
            {"q": name, "sources": SOURCES},
        )
        if status != 200 or not isinstance(ac_payload, dict):
            checks.append(Check("autocomplete", False, f"status={status}"))
        else:
            # JSON autocomplete always delegates to search(limit=10).
            checks.append(
                _check_catalog_match(
                    "autocomplete tv matches catalog hydrate (limit=10)",
                    tv_hits[:AUTOCOMPLETE_LIMIT],
                    _bucket(ac_payload, "tv"),
                )
            )
            checks.append(
                _check_catalog_match(
                    "autocomplete movie matches catalog hydrate (limit=10)",
                    movie_hits[:AUTOCOMPLETE_LIMIT],
                    _bucket(ac_payload, "movie"),
                )
            )

        status, filtered = _get_json(
            client,
            args.host,
            "/api/search",
            {
                "q": name,
                "sources": SOURCES,
                "limit": args.limit,
                "year_min": IMPOSSIBLE_YEAR,
            },
        )
        if status != 200 or not isinstance(filtered, dict):
            checks.append(Check("year filter disables rewrite", False, f"status={status}"))
        else:
            filtered_tv = _ids(_bucket(filtered, "tv"))
            filtered_movie = _ids(_bucket(filtered, "movie"))
            checks.append(
                Check(
                    f"year_min={IMPOSSIBLE_YEAR} keeps TAG filtering (empty tv/movie)",
                    filtered_tv == [] and filtered_movie == [],
                    "tv and movie buckets empty"
                    if filtered_tv == [] and filtered_movie == []
                    else f"tv={filtered_tv} movie={filtered_movie}",
                )
            )

        status, minimal = _get_json(
            client,
            args.host,
            "/api/autocomplete/minimal",
            {"q": name, "sources": SOURCES, "limit": args.limit, "fields": "mc_id,title,popularity"},
        )
        if status != 200 or not isinstance(minimal, dict):
            checks.append(Check("minimal autocomplete skips rewrite", False, f"status={status}"))
        else:
            minimal_ids = _ids(_bucket(minimal, "tv")) + _ids(_bucket(minimal, "movie"))
            rewrite_ids = [hit.mc_id for hit in limit_hits_tv + limit_hits_movie]
            checks.append(
                Check(
                    "minimal autocomplete is not the credit filmography",
                    minimal_ids != rewrite_ids,
                    "lite TAG results differ from credit hydrate"
                    if minimal_ids != rewrite_ids
                    else "minimal tv+movie ids equal the credit hydrate; lite applied the rewrite",
                )
            )

        for path in ("/api/search/stream", "/api/autocomplete/stream"):
            stream_params: dict[str, str | int] = {"q": name, "sources": SOURCES}
            if path == "/api/search/stream":
                stream_params["limit"] = args.limit
            try:
                response = client.get(f"{args.host.rstrip('/')}{path}", params=stream_params)
            except httpx.HTTPError as exc:
                checks.append(Check(path, False, str(exc)))
                continue
            if response.status_code != 200:
                checks.append(Check(path, False, f"status={response.status_code}"))
                continue
            events = _parse_sse(response.text)
            results = _result_events(events)
            sources_in_order = [str(item.get("source") or "") for item in results]
            expected_tv = (
                tv_hits[: args.limit] if path == "/api/search/stream" else tv_hits[:AUTOCOMPLETE_LIMIT]
            )
            expected_movie = (
                movie_hits[: args.limit]
                if path == "/api/search/stream"
                else movie_hits[:AUTOCOMPLETE_LIMIT]
            )
            tv_ok, tv_detail = _stream_bucket_ok(results, "tv", expected_tv)
            movie_ok, movie_detail = _stream_bucket_ok(results, "movie", expected_movie)
            person_at = next(
                (idx for idx, source in enumerate(sources_in_order) if source == "person"),
                None,
            )
            media_at = next(
                (idx for idx, source in enumerate(sources_in_order) if source in {"tv", "movie"}),
                None,
            )
            person_first = person_at is not None and (media_at is None or person_at < media_at)
            passed = tv_ok and movie_ok and person_first
            order_detail = (
                "person emitted before tv/movie"
                if person_first
                else "person was not emitted before tv/movie"
            )
            checks.append(
                Check(
                    f"{path} emits filmography once, after person",
                    passed,
                    f"order={sources_in_order}; {order_detail}; tv: {tv_detail}; movie: {movie_detail}",
                )
            )

    failed = [check for check in checks if not check.passed]
    for check in checks:
        mark = "PASS" if check.passed else "FAIL"
        print(f"[{mark}] {check.name}")
        print(f"       {check.detail.replace(chr(10), chr(10) + '       ')}")
    print()
    print(f"{len(checks) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


def main() -> None:
    args = _parse_args()
    raise SystemExit(_run(args))


if __name__ == "__main__":
    main()
