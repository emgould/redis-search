"""Async HTTP client for the Media Manager insert-docs API."""

from __future__ import annotations

import asyncio
import os
from typing import Any, TypedDict
from urllib.parse import urlparse

import httpx

from utils.get_logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 3600.0  # 1 hour fail-safe
REBUILD_TIMEOUT = 900.0  # 15 minutes per index rebuild

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
    total_processed: int
    total_dry_run: int
    total_errors: int
    worker_running: bool
    last_processed_at: str | None
    session: dict[str, Any]


class FlushResponse(TypedDict):
    email_sent: bool
    status: str
    movies_added: int
    tv_added: int
    movies_updated: int
    tv_updated: int
    total_errors: int


class RebuildIndexResponse(TypedDict):
    status: str
    index_name: str
    total_documents: int
    duration_seconds: float


class HealthResponse(TypedDict):
    status: str
    media_manager_initialized: bool


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

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._token:
            headers["X-Internal-Token"] = self._token
        if "host.docker.internal" in self._base_url:
            parsed = urlparse(self._base_url)
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            headers["Host"] = f"localhost:{port}"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=self._build_headers(),
                timeout=httpx.Timeout(self._timeout),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> HealthResponse:
        """Verify Media Manager is reachable and initialized.

        Raises ``RuntimeError`` if the service is unhealthy.
        """
        client = await self._get_client()
        try:
            resp = await client.get("/health", timeout=10.0)
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
    ) -> InsertDocsResponse:
        """POST a batch of documents (max 100) to /insert-docs."""
        if len(documents) > 100:
            raise ValueError("Batch size must not exceed 100 documents")

        client = await self._get_client()
        resp = await client.post(
            "/insert-docs",
            json={"documents": documents, "dry_run": dry_run},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return InsertDocsResponse(
            queued=data["queued"],
            skipped=data["skipped"],
            errors=data.get("errors", []),
            queue_depth=data["queue_depth"],
        )

    async def get_status(self) -> StatusResponse:
        """GET /insert-docs/status for current processing state."""
        client = await self._get_client()
        resp = await client.get("/insert-docs/status")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return StatusResponse(
            queue_depth=data["queue_depth"],
            total_processed=data["total_processed"],
            total_dry_run=data.get("total_dry_run", 0),
            total_errors=data["total_errors"],
            worker_running=data["worker_running"],
            last_processed_at=data.get("last_processed_at"),
            session=data.get("session", {}),
        )

    async def flush(self) -> FlushResponse:
        """POST /insert-docs/flush — blocks until queue is drained."""
        client = await self._get_client()
        resp = await client.post("/insert-docs/flush")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return FlushResponse(
            email_sent=data.get("email_sent", False),
            status=data["status"],
            movies_added=data.get("movies_added", 0),
            tv_added=data.get("tv_added", 0),
            movies_updated=data.get("movies_updated", 0),
            tv_updated=data.get("tv_updated", 0),
            total_errors=data.get("total_errors", 0),
        )

    async def rebuild_index(
        self,
        index_name: str,
        re_embedding: bool = False,
    ) -> RebuildIndexResponse:
        """POST /api/index/{index_name}/rebuild — blocks until rebuild completes."""
        client = await self._get_client()
        logger.info("Rebuilding index '%s' (re_embedding=%s)...", index_name, re_embedding)
        resp = await client.post(
            f"/api/index/{index_name}/rebuild",
            json={"re_embedding": re_embedding},
            timeout=REBUILD_TIMEOUT,
        )
        resp.raise_for_status()
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
        max_wait: float = 1800.0,
    ) -> StatusResponse:
        """Poll ``/insert-docs/status`` until ``queue_depth == 0``.

        Returns the final status response once drained or after *max_wait*
        seconds elapse.
        """
        waited = 0.0
        while waited < max_wait:
            status = await self.get_status()
            if status["queue_depth"] == 0:
                logger.info(
                    "Queue drained (total_processed=%d)", status["total_processed"]
                )
                return status
            logger.info(
                "Waiting for queue drain: queue_depth=%d, total_processed=%d (%.0fs elapsed)",
                status["queue_depth"],
                status["total_processed"],
                waited,
            )
            await asyncio.sleep(poll_interval)
            waited += poll_interval
        logger.warning("Max wait %.0fs reached, queue may not be fully drained", max_wait)
        return await self.get_status()
