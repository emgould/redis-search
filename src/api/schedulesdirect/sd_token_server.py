"""
SchedulesDirect token refresh server.
Runs with max_instances=1 to avoid concurrency races.
"""

import asyncio
import hashlib
from datetime import UTC, datetime

from firebase_admin import firestore
from firebase_functions import scheduler_fn
from firebase_functions.params import SecretParam

from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger

logger = get_logger(__name__)

SCHEDULES_DIRECT_USERNAME = SecretParam("SCHEDULES_DIRECT_USERNAME")
SCHEDULES_DIRECT_PASSWORD = SecretParam("SCHEDULES_DIRECT_PASSWORD")

FIRESTORE_DOC = "schedulesdirect/token"


class SDTokenRefresher(BaseAPIClient):
    base_url = "https://json.schedulesdirect.org/20141201"

    async def request_token(self) -> dict:
        """Hit POST /token and return the token + expiration + error info."""
        username = SCHEDULES_DIRECT_USERNAME.value
        password = SCHEDULES_DIRECT_PASSWORD.value

        if not username or not password:
            return {"error": "Missing SD credentials", "token": None}

        password_hash = hashlib.sha1(password.encode("utf-8")).hexdigest()

        response, status = await self._core_async_post_request(
            url=f"{self.base_url}/token",
            json_body={"username": username, "password": password_hash},
            headers={"Accept": "application/json"},
            timeout=30,
            max_retries=1,
        )

        if not isinstance(response, dict) or status != 200 or "token" not in response:
            msg = (
                response.get("message", "Unknown error")
                if isinstance(response, dict)
                else str(response)
            )
            return {
                "error": msg,
                "token": None,
                "raw": response,
                "status": status,
            }

        expires_dt = datetime.now(UTC).timestamp() + 23 * 60 * 60
        try:
            expires_dt = response.get("tokenExpires", expires_dt)

        except Exception:
            pass

        return {
            "token": response["token"],
            "expiresAt": expires_dt,
            "raw": response,
            "status": status,
            "error": None,
        }


async def refresh_and_store_token():
    """Logic used both by scheduler and on-demand client refresh."""
    refresher = SDTokenRefresher()
    result = await refresher.request_token()

    if result["token"] is None:
        logger.error(f"SD token refresh FAILED: {result}")
        return result

    firestore.client().document(FIRESTORE_DOC).set(
        {
            "token": result["token"],
            "expiresAt": result["expiresAt"],
            "updatedAt": datetime.now(UTC).timestamp(),
        }
    )

    logger.info("SD token refreshed and saved to Firestore.")
    return result


# === CLOUD FUNCTION === #


@scheduler_fn.on_schedule(schedule="every 24 hours")
def scheduled_sd_token_refresh(event: scheduler_fn.ScheduledEvent):
    """Firebase scheduled function to refresh token daily."""
    asyncio.run(refresh_and_store_token())
