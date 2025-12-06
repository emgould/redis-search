"""
Utility to check SchedulesDirect service status by scraping their website.
"""

import httpx
from bs4 import BeautifulSoup

from utils.get_logger import get_logger
from utils.pydantic_tools import BaseModelWithMethods

logger = get_logger(__name__)

SD_WEBSITE_URL = "https://www.schedulesdirect.org"
REQUEST_TIMEOUT = 10.0


class ServiceStatus(BaseModelWithMethods):
    """Service status response."""

    offline: bool
    status: str | None


async def check_service_status() -> ServiceStatus:
    """
    Check if SchedulesDirect service is offline by checking their website.

    Looks for an element with id="user_errors" containing "Server offline".

    Returns:
        ServiceStatus dict with:
            - offline: True if service appears to be offline, False otherwise
            - status: Text content of the notice if found, None otherwise

    Example:
        >>> status = await check_service_status()
        >>> if status["offline"]:
        ...     print(f"Service is down: {status['status']}")
    """
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.get(SD_WEBSITE_URL)
            response.raise_for_status()
            html = response.text

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")

            # Look for element with id="user_errors"
            user_errors = soup.find(id="user_errors")

            if user_errors:
                # Get the text content, stripping whitespace
                text_content = user_errors.get_text(strip=True)

                if text_content:
                    # Check if it mentions server being offline
                    is_offline = "server offline" in text_content.lower()
                    logger.info(
                        "SchedulesDirect status check: found user_errors - offline=%s, status='%s'",
                        is_offline,
                        text_content[:100],
                    )
                    return ServiceStatus(offline=is_offline, status=text_content)

            # No user_errors element found or it's empty - service appears to be up
            logger.debug("SchedulesDirect status check: no errors found, service appears online")
            return ServiceStatus(offline=False, status=None)

    except httpx.TimeoutException:
        logger.warning("SchedulesDirect status check: timeout connecting to website")
        return ServiceStatus(
            offline=True, status="Unable to reach SchedulesDirect website (timeout)"
        )
    except httpx.HTTPStatusError as e:
        logger.warning("SchedulesDirect status check: HTTP error %s", e.response.status_code)
        return ServiceStatus(
            offline=True,
            status=f"SchedulesDirect website returned HTTP {e.response.status_code}",
        )
    except Exception as e:
        logger.error("SchedulesDirect status check: unexpected error - %s", e)
        return ServiceStatus(
            offline=True, status=f"Error checking SchedulesDirect status: {str(e)}"
        )


def check_service_status_sync() -> ServiceStatus:
    """
    Synchronous wrapper for check_service_status.

    For use in non-async contexts.
    """
    import asyncio

    return asyncio.run(check_service_status())
