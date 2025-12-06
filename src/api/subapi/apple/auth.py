import os
import time

import jwt  # PyJWT
from dotenv import find_dotenv, load_dotenv
from firebase_functions.params import SecretParam

from utils.redis_cache import RedisCache

load_dotenv(find_dotenv())

TokenCache = RedisCache(
    defaultTTL=60 * 60 * 24 * 100,  # up to ~6 months
    prefix="apple_auth",
    verbose=False,
    isClassMethod=True,
    version="1.0.1",  # Version bump for Redis migration
)

# Apple API secret parameters (declared at module level for Firebase CLI detection)
APPLE_TEAM_ID = SecretParam("APPLE_TEAM_ID")
APPLE_KEY_ID = SecretParam("APPLE_KEY_ID")
APPLE_PRIVATE_KEY = SecretParam("APPLE_PRIVATE_KEY")


class AppleAuth:
    """
    Apple Auth Service - Base service with authentication utilities.
    Provides foundation for Apple API operations.
    """

    _team_id: str | None = None
    _key_id: str | None = None
    _apple_private_key: str | None = None

    def __init__(self):
        self._team_id = None
        self._key_id = None
        self._apple_private_key = None

    @property
    def team_id(self) -> str | None:
        if self._team_id is None:
            self._team_id = os.getenv("APPLE_TEAM_ID") or APPLE_TEAM_ID.value
        return self._team_id

    @property
    def key_id(self) -> str | None:
        if self._key_id is None:
            self._key_id = os.getenv("APPLE_KEY_ID") or APPLE_KEY_ID.value
        return self._key_id

    @property
    def apple_private_key(self) -> str | None:
        if self._apple_private_key is None:
            raw_key = os.getenv("APPLE_PRIVATE_KEY") or APPLE_PRIVATE_KEY.value
            if raw_key:
                self._apple_private_key = raw_key.replace("\\n", "\n")
        return self._apple_private_key

    @RedisCache.use_cache(TokenCache, prefix="apple_auth")
    async def get_developer_token(self, **kwargs) -> str | None:
        if self.team_id is None or self.key_id is None or self.apple_private_key is None:
            return None

        now = int(time.time())
        payload = {
            "iss": self.team_id,
            "iat": now,
            "exp": now + 60 * 60 * 24 * 180,  # up to ~6 months
        }

        headers = {
            "alg": "ES256",
            "kid": self.key_id,
        }

        developer_token = jwt.encode(
            payload,
            self.apple_private_key,
            algorithm="ES256",
            headers=headers,
        )

        return developer_token
