"""
Firebase HTTPS handlers for SchedulesDirect APIs.
"""

import json
import logging
from typing import Any

import requests
from firebase_functions import https_fn

from api.schedulesdirect.channel_filters import ChannelType
from api.schedulesdirect.models import (
    DEFAULT_PRIMETIME_WINDOW,
    SchedulesDirectServiceUnavailableError,
)
from api.schedulesdirect.wrappers import schedules_direct_wrapper
from contracts.models import MCType

logger = logging.getLogger(__name__)

# Valid mc_type values for the primetime endpoint
VALID_MC_TYPES = {"tv": MCType.TV_SERIES, "movie": MCType.MOVIE}

# Valid channel_type values for the primetime endpoint
VALID_CHANNEL_TYPES = {
    "broadcast": ChannelType.BROADCAST,
    "premium-cable": ChannelType.PREMIUM_CABLE,
    "non-premium-cable": ChannelType.NON_PREMIUM_CABLE,
}


class SchedulesDirectHandler:
    """HTTP handler entrypoints for SchedulesDirect-powered data APIs."""

    def __init__(self):
        logger.info("SchedulesDirectHandler initialized")

    async def get_primetime_schedule(self, req: https_fn.Request) -> https_fn.Response:
        """
        Return TMDB-enriched promotion schedule for national networks.

        Query Parameters:
            start: Start time in HH:MM format (default: "20:00")
            end: End time in HH:MM format (default: "23:00")
            mc_type: Filter by content type - "tv" or "movie" (optional)
            channel_type: Filter by channel type - "broadcast", "premium-cable",
                         or "non-premium-cable" (optional)
        """
        if req.method == "OPTIONS":
            headers = self._cors_headers()
            return https_fn.Response("", status=204, headers=headers)

        headers = self._cors_headers()
        params: dict[str, Any] = req.args or {}
        try:
            start_time = params.get("start", DEFAULT_PRIMETIME_WINDOW["start"])
            end_time = params.get("end", DEFAULT_PRIMETIME_WINDOW["end"])

            # Parse mc_type filter parameter (optional)
            mc_type_param = params.get("mc_type")
            mc_type: MCType | None = None
            if mc_type_param:
                mc_type_lower = mc_type_param.lower()
                if mc_type_lower not in VALID_MC_TYPES:
                    raise ValueError(
                        f"Invalid mc_type '{mc_type_param}'. Valid values: {list(VALID_MC_TYPES.keys())}"
                    )
                mc_type = VALID_MC_TYPES[mc_type_lower]

            # Parse channel_type filter parameter (optional)
            channel_type_param = params.get("channel_type")
            channel_type: ChannelType | None = None
            if channel_type_param:
                channel_type_lower = channel_type_param.lower()
                if channel_type_lower not in VALID_CHANNEL_TYPES:
                    raise ValueError(
                        f"Invalid channel_type '{channel_type_param}'. "
                        f"Valid values: {list(VALID_CHANNEL_TYPES.keys())}"
                    )
                channel_type = VALID_CHANNEL_TYPES[channel_type_lower]

            response = await schedules_direct_wrapper.get_primetime_schedule(
                start_time=start_time,
                end_time=end_time,
                mc_type=mc_type,
                channel_type=channel_type,
            )
            # Check if response is None (shouldn't happen, but handle gracefully)
            if response is None:
                raise RuntimeError("SchedulesDirect service returned None response")

            # Check if response has results attribute
            if not hasattr(response, "results") or response.results is None:
                raise RuntimeError("SchedulesDirect service returned invalid response structure")

            # Return just the list of MC items (the primetime lineup enriched with TMDB data)
            results = [item.model_dump(exclude_none=True) for item in response.results]

            logger.info(
                "get_primetime_schedule returning %d items (mc_type=%s, channel_type=%s)",
                len(results),
                mc_type_param or "all",
                channel_type_param or "all",
            )

            return https_fn.Response(
                json.dumps(results, default=str),
                status=200,
                headers=headers,
            )
        except ValueError as exc:
            logger.error("Validation error in primetime handler: %s", exc)
            return https_fn.Response(json.dumps({"error": str(exc)}), status=400, headers=headers)
        except SchedulesDirectServiceUnavailableError as exc:
            # Service unavailable - return empty results gracefully (not an error)
            logger.warning("SchedulesDirect service unavailable in primetime handler: %s", str(exc))
            return https_fn.Response(
                json.dumps([]),
                status=200,
                headers=headers,
            )
        except RuntimeError as exc:
            # Handle SchedulesDirect service errors (authentication, service offline, etc.)
            error_msg = str(exc)
            logger.error(
                "SchedulesDirect service error in primetime handler: %s", error_msg, exc_info=True
            )

            # Determine appropriate status code based on error message
            if "account status error 403" in error_msg or "account is not active" in error_msg:
                status_code = 503  # Service Unavailable
                user_message = (
                    "SchedulesDirect service is temporarily unavailable. Please try again later."
                )
            elif "SERVICE_OFFLINE" in error_msg or "offline" in error_msg.lower():
                status_code = 503  # Service Unavailable
                user_message = (
                    "SchedulesDirect service is currently offline. Please try again later."
                )
            else:
                status_code = 503  # Service Unavailable
                user_message = "Unable to retrieve primetime schedule. Please try again later."

            return https_fn.Response(
                json.dumps({"error": user_message, "details": error_msg}),
                status=status_code,
                headers=headers,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Unexpected error in primetime handler: %s", exc, exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}), status=500, headers=headers
            )

    async def get_status(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get SchedulesDirect account status and service information.

        Returns account status, lineups, system status, and token validity.
        """
        if req.method == "OPTIONS":
            headers = self._cors_headers()
            return https_fn.Response("", status=204, headers=headers)

        headers = self._cors_headers()
        try:
            response = requests.get("https://api.ipify.org?format=json", timeout=5)
            response.raise_for_status()  # Raise an exception for bad status codes
            public_ip = response.json()["ip"]
            # Get account status (same as test_get_account_status)
            account_status = await schedules_direct_wrapper.service.auth.get_account_status()

            # Return account status information
            result = {
                "account": account_status.account.model_dump(exclude_none=True),
                "lineups": [
                    lineup.model_dump(exclude_none=True) for lineup in account_status.lineups
                ],
                "lastDataUpdate": account_status.lastDataUpdate,
                "notifications": account_status.notifications,
                "systemStatus": [
                    status.model_dump(exclude_none=True) for status in account_status.systemStatus
                ],
                "serverID": account_status.serverID,
                "datetime": account_status.datetime,
                "code": account_status.code,
                "tokenExpires": account_status.tokenExpires,
                "serverTime": account_status.serverTime,
                "is_account_active": account_status.is_account_active,
                "is_token_valid": account_status.is_token_valid,
                "total_lineups": account_status.total_lineups,
                "public_ip": public_ip,
            }
            return https_fn.Response(
                json.dumps(result, default=str),
                status=200,
                headers=headers,
            )
        except SchedulesDirectServiceUnavailableError as exc:
            # Service unavailable - return a clear status response
            logger.warning("SchedulesDirect service unavailable in status handler: %s", str(exc))
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "SchedulesDirect service is currently unavailable",
                        "details": str(exc),
                        "is_account_active": False,
                        "is_token_valid": False,
                        "public_ip": public_ip,
                    }
                ),
                status=503,
                headers=headers,
            )
        except RuntimeError as exc:
            # Handle SchedulesDirect service errors (authentication, service offline, etc.)
            error_msg = str(exc)
            logger.error(
                "SchedulesDirect service error in status handler: %s", error_msg, exc_info=True
            )

            # Determine appropriate status code based on error message
            if "account status error 403" in error_msg or "account is not active" in error_msg:
                status_code = 503  # Service Unavailable
                user_message = (
                    "SchedulesDirect service is temporarily unavailable. Please try again later."
                )
            elif "SERVICE_OFFLINE" in error_msg or "offline" in error_msg.lower():
                status_code = 503  # Service Unavailable
                user_message = (
                    "SchedulesDirect service is currently offline. Please try again later."
                )
            else:
                status_code = 503  # Service Unavailable
                user_message = "Unable to retrieve SchedulesDirect status. Please try again later."

            return https_fn.Response(
                json.dumps({"error": user_message, "details": error_msg}),
                status=status_code,
                headers=headers,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Unexpected error in status handler: %s", exc, exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}), status=500, headers=headers
            )

    def _cors_headers(self) -> dict[str, Any]:
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Content-Type": "application/json",
        }


schedules_direct_handler = SchedulesDirectHandler()
