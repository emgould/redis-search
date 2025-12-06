"""
SchedulesDirect authentication helpers.
"""

import json

from api.schedulesdirect.models import AccountStatusResponse, SchedulesDirectServiceUnavailableError
from api.schedulesdirect.sd_token_client import get_schedulesdirect_token
from api.schedulesdirect.service_status import check_service_status
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger

logger = get_logger(__name__)


class SchedulesDirectAuth(BaseAPIClient):
    """SchedulesDirect authenticated API wrapper."""

    account_status: AccountStatusResponse | None = None

    _rate_limit_max = 5
    _rate_limit_period = 1.0
    base_url = "https://json.schedulesdirect.org/20141201"

    def __init__(self):
        self.account_status = None

    async def get_account_status(self) -> AccountStatusResponse:
        """
        Helper to check if authentication failed (returns None due to cached error).
        If auth failed, skip the test gracefully.
        """

        response, status = await self.sd_request(
            method="GET",
            endpoint="/status",
            no_account_status_check=True,
        )

        if status != 200:
            raise RuntimeError(
                f"SchedulesDirect account status error {status}: {json.dumps(response)}"
            )

        # Check for service offline or other error responses before parsing
        if isinstance(response, dict):
            error_response = response.get("response", "").upper()
            if error_response in ("SERVICE_OFFLINE", "OFFLINE", "ERROR"):
                site_check = await check_service_status()
                if site_check.offline:
                    error_msg = f"Known issue: Confirmed on site: {site_check.status}"
                    raise RuntimeError(error_msg)

                raise RuntimeError(
                    "Service unavailable but is unconfirmed on site. Could be a mediacircle issue"
                )
            # Also check for code-based errors
            if response.get("code") and response.get("code") != 0:
                raise RuntimeError(
                    f"SchedulesDirect error (code {response.get('code')}): "
                    f"{response.get('response', response.get('message', 'Unknown error'))}"
                )

        self.account_status = AccountStatusResponse.model_validate(response)
        return self.account_status

    async def sd_request(
        self, method: str, endpoint: str, no_account_status_check: bool = False, **kwargs
    ):
        """
        Universal SchedulesDirect request wrapper.
        Injects token, retries once on expiration.
        """
        if self.account_status is None and not no_account_status_check:
            self.account_status = await self.get_account_status()

            if not self.account_status.is_account_active:
                raise RuntimeError("SchedulesDirect account is not active")

        # === 1. Get token from Firestore ===
        token = await get_schedulesdirect_token()
        if token is None:
            raise SchedulesDirectServiceUnavailableError(
                "Unable to get SchedulesDirect token - service may be unavailable"
            )

        headers = kwargs.pop("headers", {})
        headers.setdefault("Accept", "application/json")
        headers["token"] = token
        kwargs["headers"] = {k: v for k, v in headers.items() if v is not None}

        url = f"{self.base_url}{endpoint}"

        # === 2. Make request ===
        response, status = await self._execute_sd_request(method, url, **kwargs)

        # === 3. Check if token invalid/expired ===
        if status in (401, 403) or self._is_token_invalid(response):
            logger.warning("SD token expired/invalid → forcing refresh")

            # Force-refresh the token via client
            fresh_token = await get_schedulesdirect_token(force_refresh=True)
            if fresh_token is None:
                raise SchedulesDirectServiceUnavailableError(
                    "SchedulesDirect token refresh failed - service may be unavailable"
                )

            # Rebuild headers
            headers["token"] = fresh_token
            kwargs["headers"] = headers

            # Retry once
            response, status = await self._execute_sd_request(method, url, **kwargs)

        return response, status

    async def _execute_sd_request(self, method: str, url: str, **kwargs):
        """Internal helper handling GET/POST + exception unwrapping."""

        if method.upper() == "POST":
            json_body = kwargs.pop("json_body", {})
            json_body = self._clean_json(json_body)
            return await self._core_async_post_request(url=url, json_body=json_body, **kwargs)
        elif method.upper() == "DELETE":
            return await self._core_async_delete_request(url=url, **kwargs)
        elif method.upper() == "PUT":
            return await self._core_async_put_request(url=url, **kwargs)
        elif method.upper() == "GET":
            response, status = await self._core_async_request(
                url=url, return_status_code=True, return_exceptions=True, **kwargs
            )
        else:
            raise ValueError(f"Invalid method: {method}")

        # If BaseAPIClient returned an exception instead of JSON:
        if isinstance(response, Exception):
            logger.error(f"Exception during SD GET {url}: {response}")
            return {"error": str(response)}, 500

        return response, status

    @staticmethod
    def _clean_json(value):
        """Deep-clean dict/lists of None values."""
        if isinstance(value, dict):
            return {
                k: SchedulesDirectAuth._clean_json(v) for k, v in value.items() if v is not None
            }
        if isinstance(value, list):
            return [SchedulesDirectAuth._clean_json(v) for v in value if v is not None]
        return value

    @staticmethod
    def _is_token_invalid(response_json):
        """Detect SchedulesDirect token error responses."""

        if not isinstance(response_json, dict):
            return False

        code = response_json.get("code")
        resp = (response_json.get("response") or "").upper()

        TOKEN_ERRORS = {
            4001,  # ACCOUNT_EXPIRED
            4005,  # TOKEN_MISSING
            4006,  # TOKEN_EXPIRED
            4007,  # INVALID_TOKEN
        }

        return code in TOKEN_ERRORS or "TOKEN" in resp


schedules_direct_auth = SchedulesDirectAuth()
