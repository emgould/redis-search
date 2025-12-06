"""
Client helper for retrieving the SchedulesDirect token from Firestore.

All SchedulesDirect API calls should import get_schedulesdirect_token().
"""

from datetime import UTC, datetime

from firebase_admin import firestore

from api.schedulesdirect.sd_token_server import refresh_and_store_token
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

FIRESTORE_DOC = "schedulesdirect/token"

SDTokenCache = RedisCache(
    defaultTTL=-1,  # Never expires
    prefix="sd_token",
    verbose=False,
    isClassMethod=False,
    version="1.0.0",
)


async def get_schedulesdirect_token(force_refresh=False, **kwargs) -> str | None:
    """
    Return a valid token from Firestore.
    Automatically refreshes token (via server function) if expired/missing.
    """

    doc_ref = firestore.client().document(FIRESTORE_DOC)
    doc = doc_ref.get()

    # No token yet → refresh immediately
    if not doc.exists or force_refresh:
        logger.warning("SD token missing or force_refresh — calling refresh_and_store_token()")
        result = await refresh_and_store_token()
        token = result.get("token")
        return str(token) if token else None

    data = doc.to_dict()
    if not data:
        logger.warning("SD token document has no data — refreshing...")
        result = await refresh_and_store_token()
        token = result.get("token")
        return str(token) if token else None

    token = data.get("token")
    expires_at = data.get("expiresAt")

    # Token expired or close to expiry → refresh
    if (
        not token
        or not isinstance(expires_at, (int, float))
        or expires_at < (datetime.now(UTC).timestamp() + 5 * 60)
    ):
        logger.info("SD token expired/expiring — refreshing...")
        result = await refresh_and_store_token()
        token = result.get("token")
        return str(token) if token else None

    return str(token) if isinstance(token, str) else None
