"""Shared Spotify enrichment helper for podcast loaders/backfills.

Wraps a Spotify show search by title and returns the matched
``(spotify_url, spotify_id)`` pair so both can be persisted on the
``podcast:*`` Redis document.

The lookup is intentionally limited to the top Spotify show match for the
title (mirrors the existing ``PodcastIndexClient.get_podcast_link`` helper).
Network/rate-limited calls are cached upstream by the spotify wrapper, so
re-running the loaders is cheap.
"""

from __future__ import annotations

from api.subapi.spotify import parse_spotify_show_id, spotify_wrapper
from api.subapi.spotify.models import SpotifyPodcastShow


async def fetch_spotify_ids_for_title(title: str | None) -> tuple[str | None, str | None]:
    """Look up the Spotify show URL and id for a podcast title.

    Returns ``(spotify_url, spotify_id)``. Either or both may be ``None`` when
    the title is empty, the Spotify search returns nothing, or the top match
    is not a show.
    """
    if not title:
        return None, None
    cleaned = title.strip()
    if not cleaned:
        return None, None

    try:
        response = await spotify_wrapper.search_podcasts(
            query=cleaned, limit=1, include_episodes=False
        )
    except Exception:
        return None, None

    if response.error or not response.results:
        return None, None

    top = response.results[0]
    if not isinstance(top, SpotifyPodcastShow):
        return None, None

    spotify_url = top.spotify_url
    spotify_id = top.spotify_show_id or parse_spotify_show_id(spotify_url)
    return spotify_url, spotify_id
