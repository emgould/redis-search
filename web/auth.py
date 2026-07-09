"""
Web app authentication utilities.

Shared auth functions used across web routes.
"""

import os

from fastapi import Header, HTTPException


def verify_api_key(x_api_key: str | None) -> bool:
    """
    Verify API key for protected endpoints.

    Authorization is granted if:
    1. Valid X-API-Key header matching ETL_API_KEY env var
    2. Running locally (ENVIRONMENT=local)

    Args:
        x_api_key: API key header

    Returns:
        True if authorized, False otherwise
    """
    # Allow API key
    expected_key = os.getenv("ETL_API_KEY")
    if expected_key and x_api_key == expected_key:
        return True

    # If running in Cloud Run, auth is REQUIRED (no bypass)
    if os.getenv("K_SERVICE"):
        return False

    # Local development: only skip auth if explicitly set to "local"
    return os.getenv("ENVIRONMENT") == "local"


def require_api_key(x_api_key: str | None = Header(None, alias="X-API-Key")) -> None:
    """
    FastAPI dependency that requires API key authentication.

    Use with: Depends(require_api_key)

    Raises HTTPException 401 if not authorized.
    """
    if not verify_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized: X-API-Key header required")


def verify_public_list_write_key(x_api_key: str | None) -> bool:
    """
    Verify the shared secret for public-list index writes.

    The public List index is runtime-owned by the MediaCircle backend, so
    writes require the dedicated PUBLIC_LISTS_API_KEY (falling back to
    ETL_API_KEY when a dedicated key is not configured).

    Authorization is granted if:
    1. Valid X-API-Key header matching PUBLIC_LISTS_API_KEY (or ETL_API_KEY)
    2. Running locally (ENVIRONMENT=local, not in Cloud Run)
    """
    expected_key = os.getenv("PUBLIC_LISTS_API_KEY") or os.getenv("ETL_API_KEY")
    if expected_key and x_api_key == expected_key:
        return True

    # If running in Cloud Run, auth is REQUIRED (no bypass)
    if os.getenv("K_SERVICE"):
        return False

    # Local development: only skip auth if explicitly set to "local"
    return os.getenv("ENVIRONMENT") == "local"


def require_public_list_write_key(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> None:
    """
    FastAPI dependency guarding public-list index write endpoints.

    Raises HTTPException 401 if not authorized.
    """
    if not verify_public_list_write_key(x_api_key):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized: X-API-Key header required for public-list writes",
        )


def verify_etl_auth(
    x_cloudscheduler_jobname: str | None = None,
    x_api_key: str | None = None,
) -> bool:
    """
    Verify request is authorized to trigger ETL.

    Authorization is granted if:
    1. Request has X-CloudScheduler-JobName header (Cloud Scheduler)
    2. Request has valid X-API-Key header matching ETL_API_KEY env var
    3. Running locally (not in Cloud Run) AND ENVIRONMENT=local

    Cloud Run detection: K_SERVICE env var is automatically set by Cloud Run.

    Args:
        x_cloudscheduler_jobname: Cloud Scheduler job name header
        x_api_key: API key header

    Returns:
        True if authorized, False otherwise
    """
    # Always allow Cloud Scheduler
    if x_cloudscheduler_jobname:
        return True

    # Allow API key
    expected_key = os.getenv("ETL_API_KEY")
    if expected_key and x_api_key == expected_key:
        return True

    # If running in Cloud Run, auth is REQUIRED (no bypass)
    if os.getenv("K_SERVICE"):
        return False

    # Local development: only skip auth if explicitly set to "local"
    return os.getenv("ENVIRONMENT") == "local"



