"""Authenticated HTTP transport for the FlixPatrol v2 API."""

import os
from typing import TypeAlias, cast

import aiohttp
from dotenv import find_dotenv, load_dotenv

from utils.get_logger import get_logger

load_dotenv(find_dotenv())

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

DEFAULT_BASE_URL = "https://api.flixpatrol.com/v2"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_PAGES = 50

logger = get_logger(__name__)


class FlixPatrolCredentialsError(RuntimeError):
    """Raised when required FlixPatrol API credentials are unavailable."""


class FlixPatrolAPIError(RuntimeError):
    """Raised when FlixPatrol returns an invalid or unsuccessful response."""


class FlixPatrolClient:
    """Small authenticated client for FlixPatrol list endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        configured_base_url = base_url or os.getenv("FLIXPATROL_API_BASE_URL") or DEFAULT_BASE_URL
        self.base_url = configured_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds or int(
            os.getenv("FLIXPATROL_API_TIMEOUT", str(DEFAULT_TIMEOUT_SECONDS))
        )

    def _auth(self) -> aiohttp.BasicAuth:
        username = os.getenv("FLIXPATROL_USERNAME")
        api_key = os.getenv("FLIXPATROL_API_KEY")
        if not username or not api_key:
            raise FlixPatrolCredentialsError(
                "FLIXPATROL_USERNAME and FLIXPATROL_API_KEY are required"
            )
        return aiohttp.BasicAuth(login=username, password=api_key)

    async def get_list(self, path: str, params: dict[str, str]) -> list[JsonObject]:
        """Fetch every page from a FlixPatrol list endpoint."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_params: dict[str, str] | None = params
        items: list[JsonObject] = []
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout, auth=self._auth()) as session:
            for _page_number in range(MAX_PAGES):
                payload = await self._get_page(session, url, request_params)
                items.extend(self._extract_items(payload))
                next_url = self._extract_next_url(payload)
                if next_url is None:
                    return items
                if not next_url.startswith(f"{self.base_url}/"):
                    raise FlixPatrolAPIError(
                        "FlixPatrol pagination link points outside the configured API base"
                    )
                url = next_url
                request_params = None

        raise FlixPatrolAPIError(f"FlixPatrol pagination exceeded {MAX_PAGES} pages for {path}")

    async def get_json(self, url: str) -> JsonObject:
        """Fetch one absolute FlixPatrol URL, including a ``links.next`` page."""
        if not url.startswith(f"{self.base_url}/"):
            raise FlixPatrolAPIError(
                "FlixPatrol request URL points outside the configured API base"
            )
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout, auth=self._auth()) as session:
            return await self._get_page(session, url, None)

    async def _get_page(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, str] | None,
    ) -> JsonObject:
        try:
            async with session.get(url, params=params) as response:
                if response.status >= 400:
                    body = (await response.text())[:500]
                    raise FlixPatrolAPIError(
                        f"FlixPatrol API returned HTTP {response.status}: {body}"
                    )
                payload = await response.json()
                if not isinstance(payload, dict):
                    raise FlixPatrolAPIError("FlixPatrol API returned a non-object response")
                return cast(JsonObject, payload)
        except TimeoutError as exc:
            raise FlixPatrolAPIError("FlixPatrol API request timed out") from exc
        except aiohttp.ClientError as exc:
            raise FlixPatrolAPIError(f"FlixPatrol API request failed: {exc}") from exc

    @staticmethod
    def _extract_items(payload: JsonObject) -> list[JsonObject]:
        data = payload.get("data")
        if not isinstance(data, list):
            raise FlixPatrolAPIError("FlixPatrol list response is missing a data array")
        if not all(isinstance(item, dict) for item in data):
            raise FlixPatrolAPIError("FlixPatrol list response contains a non-object item")
        return cast(list[JsonObject], data)

    @staticmethod
    def _extract_next_url(payload: JsonObject) -> str | None:
        links = payload.get("links")
        if not isinstance(links, dict):
            return None
        next_url = links.get("next")
        if next_url is None:
            return None
        if not isinstance(next_url, str):
            raise FlixPatrolAPIError("FlixPatrol response contains an invalid next link")
        return next_url


flixpatrol_client = FlixPatrolClient()
