"""Redis cooldown pool for Streaming Availability RapidAPI keys.

Cooldown is monthly quota only. A per-second 429 that still has remaining
requests does not park the key until the monthly reset.
"""

import hashlib
import random
from collections.abc import Mapping

from redis.exceptions import RedisError

from utils.get_logger import get_logger
from utils.redis_cache import get_redis_client

logger = get_logger(__name__)

FALLBACK_COOLDOWN_SECONDS = 60
MAX_COOLDOWN_SECONDS = 40 * 24 * 60 * 60
_KEY_PREFIX = "sa_rapidapi_cooldown"

_LIMIT_HEADERS = ("x-ratelimit-api-request-limit", "x-ratelimit-requests-limit")
_REMAINING_HEADERS = (
    "x-ratelimit-api-request-remaining",
    "x-ratelimit-requests-remaining",
)
_RESET_HEADERS = ("x-ratelimit-api-request-reset", "x-ratelimit-requests-reset")


class RapidAPIQuotaExhaustedError(RuntimeError):
    """Raised when every pooled key is cooled or the last response was HTTP 429."""

    def __init__(self) -> None:
        super().__init__("Streaming Availability RapidAPI quota exhausted")


def rapidapi_key_fingerprint(api_key: str) -> str:
    """Stable Redis id for a key. The raw secret is never stored."""
    digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    return digest


def _key_suffix(api_key: str) -> str:
    return api_key[-4:] if len(api_key) >= 4 else "????"


def _cooldown_redis_key(api_key: str) -> str:
    return f"{_KEY_PREFIX}:{rapidapi_key_fingerprint(api_key)}"


def _folded_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(name).lower(): str(value) for name, value in headers.items()}


def _header_value(headers: Mapping[str, str], names: tuple[str, ...]) -> str | None:
    folded = _folded_headers(headers)
    for name in names:
        value = folded.get(name)
        if value:
            return value
    return None


def _parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def cooldown_ttl_seconds(raw_reset: str | None) -> int:
    """Clamp RapidAPI reset seconds. The header is a duration, not a Unix time."""
    parsed = _parse_int(raw_reset)
    if parsed is None or parsed <= 0:
        logger.warning(
            "RapidAPI reset header %r is missing or invalid; using %s second cooldown",
            raw_reset,
            FALLBACK_COOLDOWN_SECONDS,
        )
        return FALLBACK_COOLDOWN_SECONDS
    if parsed > MAX_COOLDOWN_SECONDS:
        logger.warning("RapidAPI reset header %s exceeds 40 days; clamping", parsed)
        return MAX_COOLDOWN_SECONDS
    return parsed


def _exists_flag(flag: object) -> bool:
    if isinstance(flag, bool):
        return flag
    if isinstance(flag, int):
        return flag != 0
    return bool(flag)


def is_rapidapi_key_in_cooldown(api_key: str) -> bool:
    """Return True when Redis still holds a monthly cooldown for this key."""
    try:
        return _exists_flag(get_redis_client().exists(_cooldown_redis_key(api_key)))
    except (RedisError, OSError) as exc:
        logger.warning(
            "RapidAPI cooldown lookup failed for key ...%s: %s",
            _key_suffix(api_key),
            exc,
        )
        return False


def mark_rapidapi_key_cooldown(api_key: str, reset_seconds: int, *, reason: str) -> None:
    """Park a key until the monthly quota resets. Failures are logged and ignored."""
    ttl = reset_seconds if reset_seconds > 0 else FALLBACK_COOLDOWN_SECONDS
    try:
        get_redis_client().set(
            _cooldown_redis_key(api_key),
            reason.encode("utf-8"),
            ex=ttl,
        )
    except (RedisError, OSError) as exc:
        logger.warning(
            "Failed to store RapidAPI cooldown for key ...%s: %s",
            _key_suffix(api_key),
            exc,
        )


def record_rapidapi_rate_limit_headers(
    api_key: str,
    headers: Mapping[str, str],
    *,
    http_status: int,
) -> None:
    """Apply monthly cooldown from Streaming Availability rate-limit headers."""
    limit_raw = _header_value(headers, _LIMIT_HEADERS)
    remaining_raw = _header_value(headers, _REMAINING_HEADERS)
    reset_raw = _header_value(headers, _RESET_HEADERS)
    remaining = _parse_int(remaining_raw)
    suffix = _key_suffix(api_key)

    if remaining == 0:
        ttl = cooldown_ttl_seconds(reset_raw)
        logger.info(
            "RapidAPI key ...%s entering monthly cooldown reason=remaining_zero "
            "reset_seconds=%s limit=%s remaining=0",
            suffix,
            ttl,
            limit_raw,
        )
        mark_rapidapi_key_cooldown(api_key, ttl, reason="remaining_zero")
        return

    if http_status == 429 and remaining is None:
        ttl = cooldown_ttl_seconds(reset_raw)
        logger.info(
            "RapidAPI key ...%s entering monthly cooldown reason=429_no_remaining "
            "reset_seconds=%s limit=%s remaining=%s",
            suffix,
            ttl,
            limit_raw,
            remaining_raw,
        )
        mark_rapidapi_key_cooldown(api_key, ttl, reason="429_no_remaining")
        return

    if http_status == 429 and remaining is not None and remaining > 0:
        logger.info(
            "RapidAPI key ...%s burst 429 remaining=%s; rotating, no monthly cooldown",
            suffix,
            remaining,
        )


def pick_rapidapi_key(candidates: list[str]) -> str | None:
    """Pick a random key that is not in monthly cooldown.

    Redis failures fall back to the full candidate list so a cache blip does not
    fail Watch Now.
    """
    if not candidates:
        return None
    try:
        client = get_redis_client()
        pipe = client.pipeline(transaction=False)
        for api_key in candidates:
            pipe.exists(_cooldown_redis_key(api_key))
        raw_flags = pipe.execute()
    except (RedisError, OSError) as exc:
        logger.warning("RapidAPI cooldown lookup failed; using full pool: %s", exc)
        return random.choice(candidates)

    eligible = [
        api_key
        for api_key, flag in zip(candidates, raw_flags, strict=False)
        if not _exists_flag(flag)
    ]
    if not eligible:
        logger.warning(
            "Every configured RapidAPI key is in monthly cooldown (%s keys)",
            len(candidates),
        )
        return None
    return random.choice(eligible)
