from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import aiohttp
from firebase_functions.params import SecretParam

# Import Spotify auth service from centralized Spotify auth module
from api.subapi.spotify.auth import spotify_auth
from utils.base_api_client import BaseAPIClient
from utils.redis_cache import RedisCache

logger = logging.getLogger(__name__)
PODCASTINDEX_API_KEY = SecretParam("PODCASTINDEX_API_KEY")
PODCASTINDEX_API_SECRET = SecretParam("PODCASTINDEX_API_SECRET")

# Request cache - separate from other caches, independent refresh
PodcastRequestCache = RedisCache(
    defaultTTL=2 * 60 * 60,  # 2 hours - podcast data changes moderately
    prefix="podcast_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.0",  # Request cache version - independent from other caches
)

# ---------------------------
# Dataclass models (typed)
# ---------------------------


def _ts_to_dt(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=UTC)
    except Exception:
        return None


@dataclass
class PodcastFeed:
    id: int
    title: str
    url: str
    original_url: str | None
    site: str | None  # "link" in API
    description: str | None
    author: str | None
    owner_name: str | None
    image: str | None
    artwork: str | None
    last_update_time: datetime | None
    last_crawl_time: datetime | None
    last_parse_time: datetime | None
    last_good_http_status_time: datetime | None
    last_http_status: int | None
    content_type: str | None
    itunes_id: int | None
    trend_score: int | None
    language: str | None
    popularity_score: int | None = None  # 0-29 scale from PodcastIndex
    categories: dict[str, str] = field(default_factory=dict)
    dead: int | None = None
    locked: int | None = None
    podcast_guid: str | None = None
    episode_count: int | None = None
    spotify_url: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> PodcastFeed:
        id_value = d.get("id")
        if id_value is None:
            raise ValueError("Missing required field: id")
        return PodcastFeed(
            id=int(id_value),
            title=d.get("title") or "",
            url=d.get("url") or "",
            original_url=d.get("originalUrl"),
            site=d.get("link"),
            description=d.get("description"),
            author=d.get("author"),
            owner_name=d.get("ownerName"),
            image=d.get("image"),
            artwork=d.get("artwork"),
            last_update_time=_ts_to_dt(d.get("lastUpdateTime")),
            last_crawl_time=_ts_to_dt(d.get("lastCrawlTime")),
            last_parse_time=_ts_to_dt(d.get("lastParseTime")),
            last_good_http_status_time=_ts_to_dt(d.get("lastGoodHttpStatusTime")),
            last_http_status=(
                int(d["lastHttpStatus"]) if d.get("lastHttpStatus") is not None else None
            ),
            content_type=d.get("contentType"),
            itunes_id=(int(d["itunesId"]) if d.get("itunesId") is not None else None),
            trend_score=(int(d["trendScore"]) if d.get("trendScore") is not None else None),
            language=d.get("language"),
            popularity_score=(
                int(d["popularityScore"]) if d.get("popularityScore") is not None else None
            ),
            categories={str(k): str(v) for k, v in (d.get("categories") or {}).items()},
            dead=(int(d["dead"]) if d.get("dead") is not None else None),
            locked=(int(d["locked"]) if d.get("locked") is not None else None),
            podcast_guid=d.get("podcastGuid"),
            episode_count=(int(d["episodeCount"]) if d.get("episodeCount") is not None else None),
            spotify_url=d.get("spotify_url"),
            raw=d,
        )


@dataclass
class EpisodeItem:
    id: int
    title: str
    link: str | None
    description: str | None
    guid: str | None
    date_published: datetime | None
    enclosure_url: str | None
    enclosure_type: str | None
    enclosure_length: int | None
    duration_seconds: int | None
    explicit: int | None
    episode_type: str | None
    season: int | None
    episode: int | None
    feed_id: int | None
    feed_title: str | None
    image: str | None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> EpisodeItem:
        id_value = d.get("id")
        if id_value is None:
            raise ValueError("Missing required field: id")
        return EpisodeItem(
            id=int(id_value),
            title=d.get("title") or "",
            link=d.get("link"),
            description=d.get("description"),
            guid=d.get("guid"),
            date_published=_ts_to_dt(d.get("datePublished")),
            enclosure_url=d.get("enclosureUrl"),
            enclosure_type=d.get("enclosureType"),
            enclosure_length=(
                int(d["enclosureLength"]) if d.get("enclosureLength") is not None else None
            ),
            duration_seconds=(int(d["duration"]) if d.get("duration") is not None else None),
            explicit=(int(d["explicit"]) if d.get("explicit") is not None else None),
            episode_type=d.get("episodeType"),
            season=(int(d["season"]) if d.get("season") is not None else None),
            episode=(int(d["episode"]) if d.get("episode") is not None else None),
            feed_id=(int(d["feedId"]) if d.get("feedId") is not None else None),
            feed_title=d.get("feedTitle"),
            image=d.get("image"),
            raw=d,
        )


# ---------------------------
# Async API client
# ---------------------------


class PodcastIndexClient(BaseAPIClient):
    BASE_URL = "https://api.podcastindex.org/api/1.0"

    # Rate limiter configuration: PodcastIndex API limits
    # Conservative limit: 3 requests per second (180 requests per minute)
    _rate_limit_max = 3
    _rate_limit_period = 1

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        session: aiohttp.ClientSession | None = None,
        user_agent: str = "yourapp/1.0",
        timeout_seconds: int = 5,
    ):
        self.api_key = api_key or os.getenv("PODCASTINDEX_API_KEY")
        self.api_secret = api_secret or os.getenv("PODCASTINDEX_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "API key and secret are required (env: PODCASTINDEX_API_KEY, PODCASTINDEX_API_SECRET)"
            )

        self._external_session = session is not None
        self._session = session
        self._user_agent = user_agent
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._spotify_token: str | None = None

    async def get_spotify_token(self):
        """
        Get Spotify token using centralized Spotify auth service.
        """
        async with self._session or aiohttp.ClientSession() as session:
            token = await spotify_auth.get_spotify_token(session)
            if token:
                self._spotify_token = token
            return token

    async def get_podcast_link(self, podcast_name):
        """
        Search for a podcast by name and return the Spotify link.
        """
        search_url = "https://api.spotify.com/v1/search"
        params = {"q": podcast_name, "type": "show", "limit": 1}
        if not self._spotify_token:
            await self.get_spotify_token()

        async with (
            aiohttp.ClientSession() as session,
            session.get(
                search_url,
                headers={"Authorization": f"Bearer {self._spotify_token}"},
                params=params,
            ) as resp,
        ):
            resp.raise_for_status()
            results = await resp.json()
            items = results.get("shows", {}).get("items", [])
            if not items:
                return None
            return items[0]["external_urls"]["spotify"]

    def _headers(self) -> dict[str, str]:
        epoch = int(time.time())
        # The hash is api_key + api_secret + epoch_time, then SHA-1'd
        api_key = self.api_key or ""
        api_secret = self.api_secret or ""
        data_to_hash = api_key + api_secret + str(epoch)
        signature = hashlib.sha1(data_to_hash.encode()).hexdigest()
        return {
            "User-Agent": self._user_agent,
            "X-Auth-Key": api_key,
            "X-Auth-Date": str(epoch),
            "Authorization": signature,
            "Accept": "application/json",
        }

    async def __aenter__(self) -> PodcastIndexClient:
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session and not self._external_session:
            await self._session.close()
            self._session = None

    async def _ensure_session(self) -> None:
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=self._timeout)

    @RedisCache.use_cache(PodcastRequestCache, prefix="podcast_api")
    async def _get(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        max_retries: int = 3,
        timeout: int = 5,
    ) -> Mapping[str, Any]:
        """
        Make an async request to the PodcastIndex API with rate limiting and retry logic.

        This method brokers the call to _core_async_request with PodcastIndex-specific config.

        Args:
            path: API endpoint path (e.g., '/search/byterm')
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)
            timeout: Request timeout in seconds (default: 5)

        Returns:
            JSON response as Mapping

        Raises:
            aiohttp.ClientResponseError: On API errors after all retries
        """
        url = f"{self.BASE_URL}{path}"
        headers = self._headers()

        result = await self._core_async_request(
            url=url,
            params=dict(params) if params else None,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_max=self._rate_limit_max,
            rate_limit_period=self._rate_limit_period,
            return_exceptions=False,
        )

        if result is None:
            # Handle event loop shutdown gracefully
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    raise RuntimeError(f"Event loop is closed, cannot complete request to {url}")
            except RuntimeError:
                # No running loop or loop is closed
                raise RuntimeError(
                    f"Event loop shutdown detected, cannot complete request to {url}"
                )
            # Create a minimal error response - simplified to avoid complex RequestInfo construction
            raise RuntimeError(f"Request to {url} failed after all retries")

        # Cast to expected type since return_status_code=False
        # result is dict[str, Any] when return_status_code=False, cast to Mapping
        return cast(Mapping[str, Any], cast(dict[str, Any], result))

        # Should never reach here, but just in case
        raise RuntimeError("Request failed after all retries")

    # ---------- Public, typed methods ----------

    async def search_podcasts(self, query: str, max_results: int = 20) -> list[PodcastFeed]:
        """Typed search over podcasts by term."""
        try:
            data = await self._get("/search/byterm", {"q": query, "max": max_results})
            # Defensive check: handle case where data might be None during event loop shutdown
            if data is None:
                return []
            feeds_raw = data.get("feeds", []) or []
            return [PodcastFeed.from_dict(f) for f in feeds_raw]
        except (RuntimeError, AttributeError) as e:
            # Handle event loop shutdown or other runtime errors gracefully
            error_msg = str(e).lower()
            if (
                "cannot schedule" in error_msg
                or "event loop is closed" in error_msg
                or "shutdown" in error_msg
            ):
                logger.warning(
                    f"Event loop shutdown detected in search_podcasts, returning empty list: {e}"
                )
                return []
            # Re-raise other RuntimeErrors
            raise

    async def trending_podcasts(
        self, max_results: int = 25, lang: str | None = None
    ) -> list[PodcastFeed]:
        """Typed trending podcasts list."""
        params: MutableMapping[str, Any] = {"max": max_results}
        if lang:
            params["lang"] = lang
        data = await self._get("/podcasts/trending", params)
        feeds_raw = data.get("feeds", []) or []
        return [PodcastFeed.from_dict(f) for f in feeds_raw]

    async def recent_episodes(
        self, max_results: int = 25, lang: str | None = None
    ) -> list[EpisodeItem]:
        """Typed list of recent episodes across the index."""
        params: MutableMapping[str, Any] = {"max": max_results}
        if lang:
            params["lang"] = lang
        data = await self._get("/recent/episodes", params)
        items_raw = data.get("items", []) or []
        return [EpisodeItem.from_dict(it) for it in items_raw]

    async def podcast_by_feedid(self, feed_id: int) -> PodcastFeed | None:
        """Typed single podcast fetch by PodcastIndex feedId."""
        data = await self._get("/podcasts/byfeedid", {"id": feed_id})
        # API returns {'status':'true','feed':{...}} or possibly empty
        feed_raw = data.get("feed")
        return PodcastFeed.from_dict(feed_raw) if isinstance(feed_raw, Mapping) else None

    async def episodes_by_feedid(
        self, feed_id: int, max_results: int = 25, since: int | None = None
    ) -> list[EpisodeItem]:
        """Typed list of episodes for a specific podcast feed."""
        params: MutableMapping[str, Any] = {"id": feed_id, "max": max_results}
        if since:
            params["since"] = since
        # Use longer timeout (15s) for episode requests in cloud environment
        # where rate limiting can cause requests to queue
        data = await self._get("/episodes/byfeedid", params, timeout=15)
        items_raw = data.get("items", []) or []
        return [EpisodeItem.from_dict(it) for it in items_raw]

    async def search_episodes_by_person(
        self, person_name: str, max_results: int = 20
    ) -> list[EpisodeItem]:
        """Search for episodes by person name (host, guest, creator)."""
        data = await self._get("/search/byperson", {"q": person_name, "max": max_results})
        items_raw = data.get("items", []) or []
        return [EpisodeItem.from_dict(it) for it in items_raw]


# ---------------------------
# Example usage
# ---------------------------
# import asyncio
# async def main():
#     async with PodcastIndexClient() as client:
#         trending = await client.trending_podcasts(max_results=5, lang="en")
#         for f in trending:
#             print(f"{f.title} — {f.site or f.url} (last updated: {f.last_update_time})")
#
#         results = await client.search_podcasts("true crime", max_results=5)
#         for f in results:
#             print("SEARCH:", f.title, f.itunes_id)
#
#         recent = await client.recent_episodes(max_results=5, lang="en")
#         for ep in recent:
#             print("EP:", ep.title, ep.feed_title, ep.date_published)
#
#         single = await client.podcast_by_feedid(trending[0].id)
#         print("Single:", single.title if single else None)
#
# # asyncio.run(main())
