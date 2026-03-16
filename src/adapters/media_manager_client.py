"""Async HTTP client for the Media Manager insert-docs API."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, TypedDict
from urllib.parse import urlparse

import google.auth.transport.requests
import google.oauth2.id_token
import httpx

from utils.get_logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 3600.0  # 1 hour fail-safe
REBUILD_TIMEOUT = 900.0  # 15 minutes per index rebuild
_TOKEN_REFRESH_MARGIN = 300  # refresh OIDC token 5 min before expiry
_TOKEN_LIFETIME = 3600  # GCP identity tokens live 1 hour

MEDIA_INDEX_NAMES: dict[str, str] = {
    "movie": "movie-index",
    "tv": "tv-index",
}


class InsertDocsResponse(TypedDict):
    queued: int
    skipped: int
    errors: list[str]
    queue_depth: int


class StatusResponse(TypedDict):
    queue_depth: int
    in_flight: int
    total_processed: int
    total_dry_run: int
    total_errors: int
    worker_running: bool
    last_processed_at: str | None
    session: dict[str, Any]


class FinalizePublishResponse(TypedDict):
    email_sent: bool
    gcs_backed_up: bool
    reader_refresh_requested: bool
    readers_recycled: bool
    serving_service_name: str | None
    previous_serving_revision: str | None
    new_serving_revision: str | None
    reader_refresh_detail: str | None
    status: str
    movies_added: int
    tv_added: int
    movies_updated: int
    tv_updated: int
    metadata_only_updated: int
    total_errors: int


class RebuildIndexResponse(TypedDict):
    status: str
    index_name: str
    total_documents: int
    duration_seconds: float


class HealthResponse(TypedDict):
    status: str
    media_manager_initialized: bool


class MetadataResponse(TypedDict):
    metadata: dict[str, object]


class MediaManagerClient:
    """Thin async wrapper around the Media Manager /insert-docs endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        resolved_url = base_url or os.getenv("MEDIA_MANAGER_API_URL") or ""
        self._base_url = resolved_url.rstrip("/")
        if not self._base_url:
            raise ValueError(
                "Media Manager base URL required. "
                "Set MEDIA_MANAGER_API_URL or pass base_url."
            )
        self._token = token or os.getenv("MEDIA_MANAGER_INTERNAL_TOKEN")
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._id_token_fetched_at: float = 0.0

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._token:
            headers["X-Internal-Token"] = self._token
        if "host.docker.internal" in self._base_url:
            parsed = urlparse(self._base_url)
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            headers["Host"] = f"localhost:{port}"

        id_token = self._fetch_id_token()
        if id_token:
            headers["Authorization"] = f"Bearer {id_token}"

        return headers

    def _fetch_id_token(self) -> str | None:
        """Fetch a GCP identity token for Cloud Run authentication.

        Returns None when not running on GCE/Cloud Run or when the
        target is a local URL (localhost / host.docker.internal).
        """
        if any(h in self._base_url for h in ("localhost", "127.0.0.1", "host.docker.internal")):
            return None
        try:
            request = google.auth.transport.requests.Request()
            token: str = google.oauth2.id_token.fetch_id_token(request, self._base_url)
            self._id_token_fetched_at = time.monotonic()
            return token
        except Exception as exc:
            logger.debug("Could not fetch GCP identity token (expected in local dev): %s", exc)
            return None

    def _token_needs_refresh(self) -> bool:
        """True when the cached OIDC token is close to expiry."""
        if self._id_token_fetched_at == 0.0:
            return False
        age = time.monotonic() - self._id_token_fetched_at
        return age >= (_TOKEN_LIFETIME - _TOKEN_REFRESH_MARGIN)

    def _refresh_auth(self) -> None:
        """Re-fetch the OIDC token and update the live client headers."""
        if self._client is not None and not self._client.is_closed:
            self._client.headers.update(self._build_headers())

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None and not self._client.is_closed and self._token_needs_refresh():
            logger.debug("OIDC token approaching expiry, refreshing proactively")
            self._refresh_auth()
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=self._build_headers(),
                timeout=httpx.Timeout(self._timeout),
            )
        return self._client

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute a request with automatic OIDC token refresh on 401."""
        client = await self._get_client()
        resp = await client.request(method, url, **kwargs)
        if resp.status_code == 401 and self._id_token_fetched_at > 0:
            logger.info("Received 401 from %s %s, refreshing OIDC token and retrying", method, url)
            self._refresh_auth()
            resp = await client.request(method, url, **kwargs)
        return resp

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> HealthResponse:
        """Verify Media Manager is reachable and initialized.

        Raises ``RuntimeError`` if the service is unhealthy.
        """
        try:
            resp = await self._request("GET", "/health", timeout=60.0)
            resp.raise_for_status()
        except (httpx.HTTPError, httpx.StreamError) as exc:
            raise RuntimeError(
                f"Media Manager health check failed at {self._base_url}/health: {exc}"
            ) from exc

        data: dict[str, Any] = resp.json()
        if data.get("status") != "ok" or not data.get("media_manager_initialized"):
            raise RuntimeError(
                f"Media Manager not ready: {data}"
            )
        logger.info("Media Manager health check passed: %s", data)
        return HealthResponse(
            status=data["status"],
            media_manager_initialized=data["media_manager_initialized"],
        )

    async def insert_docs(
        self,
        documents: list[dict[str, Any]],
        dry_run: bool = False,
        metadata_only: bool = False,
    ) -> InsertDocsResponse:
        """POST a batch of documents (max 100) to /insert-docs.

        When *metadata_only* is True the Media Manager worker updates only
        the stored FAISS metadata, skipping wiki/LLM/embedding work.
        Documents not already in the index fall through to the full pipeline.
        """
        if len(documents) > 100:
            raise ValueError("Batch size must not exceed 100 documents")

        body: dict[str, Any] = {"documents": documents, "dry_run": dry_run}
        if metadata_only:
            body["metadata_only"] = True

        resp = await self._request("POST", "/insert-docs", json=body)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return InsertDocsResponse(
            queued=data["queued"],
            skipped=data["skipped"],
            errors=data.get("errors", []),
            queue_depth=data["queue_depth"],
        )

    async def get_metadata(
        self,
        media_id: str,
    ) -> MetadataResponse | None:
        """POST /api/metadata for a single media document lookup.

        Returns `None` when the media id is not present.
        """
        resp = await self._request("POST", "/api/metadata", json={"media_id": media_id})

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        data: dict[str, object] = resp.json()
        metadata = data.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("Unexpected /api/metadata response: missing metadata object")
        return MetadataResponse(metadata=metadata)

    async def get_status(self) -> StatusResponse:
        """GET /insert-docs/status for current processing state."""
        resp = await self._request("GET", "/insert-docs/status")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return StatusResponse(
            queue_depth=data["queue_depth"],
            in_flight=data.get("in_flight", 0),
            total_processed=data["total_processed"],
            total_dry_run=data.get("total_dry_run", 0),
            total_errors=data["total_errors"],
            worker_running=data["worker_running"],
            last_processed_at=data.get("last_processed_at"),
            session=data.get("session", {}),
        )

    async def finalize_publish(self) -> FinalizePublishResponse:
        """POST /api/etl/finalize-publish — blocks until publish completes."""
        resp = await self._request("POST", "/api/etl/finalize-publish")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return FinalizePublishResponse(
            email_sent=data.get("email_sent", False),
            gcs_backed_up=data.get("gcs_backed_up", False),
            reader_refresh_requested=data.get("reader_refresh_requested", False),
            readers_recycled=data.get("readers_recycled", False),
            serving_service_name=data.get("serving_service_name"),
            previous_serving_revision=data.get("previous_serving_revision"),
            new_serving_revision=data.get("new_serving_revision"),
            reader_refresh_detail=data.get("reader_refresh_detail"),
            status=data["status"],
            movies_added=data.get("movies_added", 0),
            tv_added=data.get("tv_added", 0),
            movies_updated=data.get("movies_updated", 0),
            tv_updated=data.get("tv_updated", 0),
            metadata_only_updated=data.get("metadata_only_updated", 0),
            total_errors=data.get("total_errors", 0),
        )

    async def flush(self) -> FinalizePublishResponse:
        """Legacy alias for finalize publish."""
        return await self.finalize_publish()

    async def rebuild_index(
        self,
        index_name: str,
        re_embedding: bool = False,
        max_retries: int = 5,
        initial_backoff: float = 30.0,
    ) -> RebuildIndexResponse:
        """POST /api/index/{index_name}/rebuild — blocks until rebuild completes.

        Retries with exponential backoff on 409 Conflict (index busy).
        """
        logger.info("Rebuilding index '%s' (re_embedding=%s)...", index_name, re_embedding)
        delay = initial_backoff
        for attempt in range(max_retries):
            resp = await self._request(
                "POST",
                f"/api/index/{index_name}/rebuild",
                json={"re_embedding": re_embedding},
                timeout=REBUILD_TIMEOUT,
            )
            if resp.status_code == 409 and attempt < max_retries - 1:
                logger.warning(
                    "Index '%s' returned 409 Conflict (attempt %d/%d), "
                    "retrying in %.0fs...",
                    index_name, attempt + 1, max_retries, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 300.0)
                continue
            resp.raise_for_status()
            break
        data: dict[str, Any] = resp.json()
        result = RebuildIndexResponse(
            status=data.get("status", "ok"),
            index_name=index_name,
            total_documents=data.get("total_documents", 0),
            duration_seconds=data.get("duration_seconds", 0.0),
        )
        logger.info(
            "Index '%s' rebuilt: %d documents in %.1fs",
            index_name,
            result["total_documents"],
            result["duration_seconds"],
        )
        return result

    async def rebuild_all_indexes(self) -> list[RebuildIndexResponse]:
        """Rebuild all media indexes (movie-index, tv-index) sequentially."""
        results: list[RebuildIndexResponse] = []
        for index_name in MEDIA_INDEX_NAMES.values():
            result = await self.rebuild_index(index_name)
            results.append(result)
        return results

    async def poll_until_drained(
        self,
        poll_interval: float = 5.0,
        max_wait: float = 21600.0,
    ) -> StatusResponse:
        """Poll ``/insert-docs/status`` until ``queue_depth == 0``.

        Returns the final status response once drained or after *max_wait*
        seconds elapse.
        """
        waited = 0.0
        while waited < max_wait:
            status = await self.get_status()
            if status["queue_depth"] == 0 and status["in_flight"] == 0:
                logger.info(
                    "Queue drained (total_processed=%d)", status["total_processed"]
                )
                return status
            logger.info(
                "Waiting for queue drain: queue_depth=%d, in_flight=%d, "
                "total_processed=%d (%.0fs elapsed)",
                status["queue_depth"],
                status["in_flight"],
                status["total_processed"],
                waited,
            )
            await asyncio.sleep(poll_interval)
            waited += poll_interval
        final_status = await self.get_status()
        raise TimeoutError(
            f"Queue not drained after {max_wait:.0f}s — "
            f"queue_depth={final_status['queue_depth']}, "
            f"in_flight={final_status['in_flight']}, "
            f"total_processed={final_status['total_processed']}"
        )
