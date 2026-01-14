"""
JSON Lookup Utility for aggregating TMDB data from local JSON files.

This utility reads all JSON files from the data directory and builds
a lookup dictionary keyed by TMDB ID for quick enrichment lookups.

Usage:
    from src.utils.json_lookup import build_lookup_from_json, get_enrichment_data

    # Build the lookup (reads all JSON files)
    lookup = build_lookup_from_json(Path("data/us"))

    # Get enrichment data for a specific item
    data = get_enrichment_data("tv", "12345", lookup)
    # Returns: {"genre_ids": [35, 18], "cast": [...]} or None
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def build_lookup_from_json(
    data_dir: Path,
    media_types: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Aggregate all JSON files into a lookup dictionary by TMDB ID.

    Args:
        data_dir: Path to data directory (e.g., Path("data/us"))
        media_types: List of media types to process (default: ["movie", "tv"])

    Returns:
        Dict keyed by "{mc_type}_{source_id}" containing enrichment data:
        {
            "movie_12345": {
                "genre_ids": [28, 12, 878],
                "cast": [
                    {"id": 123, "name": "Actor Name", "profile_image_url": "https://..."},
                    ...
                ]
            },
            "tv_67890": {...},
        }
    """
    if media_types is None:
        media_types = ["movie", "tv"]

    lookup: dict[str, dict[str, Any]] = {}
    files_processed = 0
    items_processed = 0

    for media_type in media_types:
        type_dir = data_dir / media_type

        if not type_dir.exists():
            logger.warning(f"Directory not found: {type_dir}")
            continue

        json_files = sorted(type_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {type_dir}")

        for json_file in json_files:
            try:
                with open(json_file) as f:
                    data = json.load(f)

                results = data.get("results", [])

                for item in results:
                    source_id = item.get("source_id") or str(item.get("tmdb_id", ""))
                    if not source_id:
                        continue

                    # Determine mc_type from item or directory
                    mc_type = item.get("mc_type") or media_type
                    if mc_type == "tv_series":
                        mc_type = "tv"

                    key = f"{mc_type}_{source_id}"

                    # Extract enrichment data
                    enrichment = _extract_enrichment_data(item)

                    if enrichment:
                        lookup[key] = enrichment
                        items_processed += 1

                files_processed += 1

            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")
                continue

    logger.info(
        f"Built lookup from {files_processed} files, "
        f"{items_processed} items indexed"
    )

    return lookup


def _extract_enrichment_data(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract enrichment data from a raw TMDB item.

    Args:
        item: Raw item from JSON file

    Returns:
        Dict with genre_ids and cast data, or None if no useful data
    """
    genre_ids = item.get("genre_ids", [])

    # Extract cast from main_cast or tmdb_cast.cast
    cast_data = []
    main_cast = item.get("main_cast", [])

    if not main_cast:
        # Try tmdb_cast.cast as fallback
        tmdb_cast = item.get("tmdb_cast", {})
        if isinstance(tmdb_cast, dict):
            main_cast = tmdb_cast.get("cast", [])

    # Extract relevant cast fields
    for actor in main_cast[:10]:  # Limit to first 10 cast members
        if not actor.get("name"):
            continue

        cast_member = {
            "id": actor.get("id"),
            "name": actor.get("name"),
            "profile_image_url": (
                actor.get("profile_image_url")
                or actor.get("image_url")
                or _build_profile_url(actor.get("profile_path"))
            ),
        }
        cast_data.append(cast_member)

    # Only return if we have some useful data
    if not genre_ids and not cast_data:
        return None

    return {
        "genre_ids": genre_ids,
        "cast": cast_data,
    }


def _build_profile_url(profile_path: str | None) -> str | None:
    """Build full profile image URL from TMDB path."""
    if not profile_path:
        return None
    return f"https://image.tmdb.org/t/p/w185{profile_path}"


def get_enrichment_data(
    mc_type: str,
    source_id: str,
    lookup: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Get enrichment data for a specific item from the lookup.

    Args:
        mc_type: Media type ("movie" or "tv")
        source_id: TMDB ID as string
        lookup: Lookup dict from build_lookup_from_json()

    Returns:
        Dict with genre_ids and cast, or None if not found
    """
    # Normalize mc_type
    if mc_type == "tv_series":
        mc_type = "tv"

    key = f"{mc_type}_{source_id}"
    return lookup.get(key)


def get_lookup_stats(lookup: dict[str, dict[str, Any]]) -> dict[str, int]:
    """
    Get statistics about the lookup dictionary.

    Returns:
        Dict with counts by media type
    """
    stats: dict[str, int] = {"movie": 0, "tv": 0, "total": 0}

    for key in lookup:
        stats["total"] += 1
        if key.startswith("movie_"):
            stats["movie"] += 1
        elif key.startswith("tv_"):
            stats["tv"] += 1

    return stats
