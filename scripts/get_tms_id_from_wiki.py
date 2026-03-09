#!/usr/bin/env python3
"""
Fetch TMDB → external ID cross-references from Wikidata SPARQL endpoint.

Queries Wikidata for all entities that have a TMDB ID (movie or TV) and
pulls available cross-reference IDs: Rotten Tomatoes, IMDb, Metacritic,
Letterboxd, JustWatch, and TCM.

Outputs a JSON file keyed by "movie:{tmdb_id}" or "tv:{tmdb_id}".

Usage:
    python fetch_wikidata_crossref.py
    python fetch_wikidata_crossref.py --output /path/to/crossref.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

QUERY = """\
SELECT ?item ?tmdbId ?mediaType
       ?rtId ?imdbId ?metacriticId ?letterboxdId ?justWatchId ?tcmId
WHERE {
  {
    ?item wdt:P4947 ?tmdbId .
    BIND("movie" AS ?mediaType)
  }
  UNION
  {
    ?item wdt:P4983 ?tmdbId .
    BIND("tv" AS ?mediaType)
  }
  OPTIONAL { ?item wdt:P1258 ?rtId . }
  OPTIONAL { ?item wdt:P345 ?imdbId . }
  OPTIONAL { ?item wdt:P1712 ?metacriticId . }
  OPTIONAL { ?item wdt:P4529 ?letterboxdId . }
  OPTIONAL { ?item wdt:P8055 ?justWatchId . }
  OPTIONAL { ?item wdt:P2631 ?tcmId . }
}
"""

DEFAULT_OUTPUT = Path("wikidata_tmdb_crossref.json")


def fetch_sparql(query: str) -> list[dict[str, dict[str, str]]]:
    """Execute a SPARQL query and return the bindings."""
    params = urlencode({"query": query, "format": "json"})
    url = f"{SPARQL_ENDPOINT}?{params}"

    req = Request(url)
    req.add_header("User-Agent", "MediaManager-WikidataCrossref/1.0")
    req.add_header("Accept", "application/sparql-results+json")

    print(f"Querying {SPARQL_ENDPOINT} ...")
    print("(This may take 10-30s due to OPTIONAL joins across ~1M+ entities)")
    start = time.monotonic()

    with urlopen(req, timeout=300) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    elapsed = time.monotonic() - start
    bindings: list[dict[str, dict[str, str]]] = data["results"]["bindings"]
    print(f"Received {len(bindings)} results in {elapsed:.1f}s")
    return bindings


def _val(row: dict[str, dict[str, str]], key: str) -> str | None:
    """Extract a string value from a SPARQL binding, or None if absent."""
    binding = row.get(key)
    if binding is None:
        return None
    return binding.get("value")


def parse_bindings(
    bindings: list[dict[str, dict[str, str]]],
) -> dict[str, dict[str, str | None]]:
    """Convert SPARQL bindings to a keyed dict.

    Output shape per entry:
    {
        "movie:550": {
            "wikidata_id": "Q25188",
            "rt_id": "m/fight_club",
            "imdb_id": "tt0137523",
            "metacritic_id": "movie/fight-club",
            "letterboxd_id": "fight-club",
            "justwatch_id": "us/movie/fight-club",
            "tcm_id": "343"
        }
    }

    Fields are null when the entity lacks that property on Wikidata.
    If an entity appears in multiple rows (multi-valued properties),
    the first non-null value wins.
    """
    result: dict[str, dict[str, str | None]] = {}

    for row in bindings:
        tmdb_id = _val(row, "tmdbId")
        media_type = _val(row, "mediaType")
        if not tmdb_id or not media_type:
            continue

        key = f"{media_type}:{tmdb_id}"

        wikidata_uri = _val(row, "item") or ""
        wikidata_id = wikidata_uri.rsplit("/", 1)[-1] if wikidata_uri else None

        entry: dict[str, str | None] = {
            "wikidata_id": wikidata_id,
            "rt_id": _val(row, "rtId"),
            "imdb_id": _val(row, "imdbId"),
            "metacritic_id": _val(row, "metacriticId"),
            "letterboxd_id": _val(row, "letterboxdId"),
            "justwatch_id": _val(row, "justWatchId"),
            "tcm_id": _val(row, "tcmId"),
        }

        existing = result.get(key)
        if existing is None:
            result[key] = entry
        else:
            for field, value in entry.items():
                if value is not None and existing.get(field) is None:
                    existing[field] = value

    return result


def print_coverage(crossref: dict[str, dict[str, str | None]]) -> None:
    """Print coverage statistics for each external ID."""
    movies = {k: v for k, v in crossref.items() if k.startswith("movie:")}
    tv = {k: v for k, v in crossref.items() if k.startswith("tv:")}

    fields = ["rt_id", "imdb_id", "metacritic_id", "letterboxd_id", "justwatch_id", "tcm_id"]
    labels = {
        "rt_id": "Rotten Tomatoes",
        "imdb_id": "IMDb",
        "metacritic_id": "Metacritic",
        "letterboxd_id": "Letterboxd",
        "justwatch_id": "JustWatch",
        "tcm_id": "TCM",
    }

    print(f"\n{'='*60}")
    print("Coverage Summary")
    print(f"{'='*60}")
    print(f"Total entries: {len(crossref)} ({len(movies)} movies, {len(tv)} TV)")
    print(f"\n{'Field':<20} {'Movies':>10} {'TV':>10} {'Total':>10}")
    print(f"{'-'*20} {'-'*10} {'-'*10} {'-'*10}")

    for field in fields:
        m_count = sum(1 for v in movies.values() if v.get(field))
        t_count = sum(1 for v in tv.values() if v.get(field))
        total = m_count + t_count
        print(f"{labels[field]:<20} {m_count:>10,} {t_count:>10,} {total:>10,}")

    print(f"{'='*60}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch TMDB → external ID cross-references from Wikidata.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    bindings = fetch_sparql(QUERY)
    if not bindings:
        print("No results returned — check query or endpoint availability.")
        sys.exit(1)

    crossref = parse_bindings(bindings)
    print_coverage(crossref)

    output_path: Path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(crossref, indent=2, sort_keys=True))
    print(f"\nWritten to {output_path}")


if __name__ == "__main__":
    main()
