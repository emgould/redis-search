"""Open a Media Manager client for an ETL run.

The ETL waits for Media Manager to finish cold start before it decides to
skip the FAISS push. One failure email is sent when that wait expires.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from adapters.config import load_env
from adapters.media_manager_client import HEALTH_DEADLINE_SECONDS, MediaManagerClient
from etl.notifications import send_media_manager_failure_email
from utils.get_logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class MediaManagerConnection:
    """Result of waiting for Media Manager before an ETL run."""

    client: MediaManagerClient | None
    configured: bool
    error: str | None = None


async def open_media_manager_client() -> MediaManagerConnection:
    """Return a healthy client, or no client when Media Manager is unset or still down.

    When the URL is configured and Media Manager does not become healthy within
    ``HEALTH_DEADLINE_SECONDS``, this sends one failure email and returns
    ``client=None``. Redis ETL continues without a FAISS push.
    """
    load_env()
    base_url = os.getenv("MEDIA_MANAGER_API_URL")
    if not base_url:
        return MediaManagerConnection(client=None, configured=False, error=None)

    client = MediaManagerClient(base_url=base_url)
    logger.info(
        "Waiting for Media Manager at %s (up to %.0fs)",
        base_url,
        HEALTH_DEADLINE_SECONDS,
    )
    try:
        await client.wait_until_healthy(deadline_seconds=HEALTH_DEADLINE_SECONDS)
    except Exception as exc:
        error = str(exc).strip() or type(exc).__name__
        logger.warning("Media Manager unavailable, skipping FAISS push: %s", error)
        try:
            await client.close()
        except Exception as close_exc:
            logger.warning("Failed to close Media Manager client: %s", close_exc)
        sent = send_media_manager_failure_email(base_url, error, HEALTH_DEADLINE_SECONDS)
        if not sent:
            logger.warning("Media Manager failure email was not sent")
        return MediaManagerConnection(client=None, configured=True, error=error)

    logger.info("Media Manager health check passed")
    return MediaManagerConnection(client=client, configured=True, error=None)
