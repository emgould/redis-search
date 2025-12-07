"""
Tests for ResilientRateLimiter to ensure cross-loop safety and token tracking.
"""

import asyncio
from collections.abc import Awaitable
from typing import Any

import pytest

from utils.rate_limiter import ResilientRateLimiter


class _BaseTestLimiter:
    """Simple AsyncLimiter test double."""

    def __init__(self, max_rate: int, time_period: float) -> None:
        self.max_rate = max_rate
        self.time_period = time_period
        self.enter_count = 0
        self.exit_count = 0
        self._fail_next = False
        self._released: list[tuple[type[BaseException] | None, BaseException | None, Any]] = []

    async def __aenter__(self) -> "_BaseTestLimiter":
        if self._fail_next:
            self._fail_next = False
            raise RuntimeError("Future attached to a different loop")
        self.enter_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.exit_count += 1
        self._released.append((exc_type, exc, tb))


@pytest.mark.asyncio
async def test_resilient_rate_limiter_releases_original_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[_BaseTestLimiter] = []

    class TrackingLimiter(_BaseTestLimiter):
        def __init__(self, max_rate: int, time_period: float) -> None:
            super().__init__(max_rate, time_period)
            created.append(self)

    monkeypatch.setattr("utils.rate_limiter.AsyncLimiter", TrackingLimiter)

    limiter = ResilientRateLimiter(5, 1)

    async def _use_limiter() -> None:
        async with limiter:
            original = limiter._limiter  # noqa: SLF001 - accessing for test verification
            assert original is not None
            assert original.exit_count == 0

            # Simulate loop change mid-flight
            limiter._limiter = TrackingLimiter(5, 1)  # noqa: SLF001
            limiter._loop_id = id(asyncio.get_running_loop())  # noqa: SLF001

    await _use_limiter()

    original_limiter = created[0]
    replacement_limiter = created[-1]

    assert original_limiter.exit_count == 1, "Should release original limiter token"
    assert replacement_limiter.exit_count == 0, "Replacement limiter should not receive exit call"


@pytest.mark.asyncio
async def test_resilient_rate_limiter_retries_on_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[_BaseTestLimiter] = []

    class FlakyLimiter(_BaseTestLimiter):
        fail_calls = 1

        def __init__(self, max_rate: int, time_period: float) -> None:
            super().__init__(max_rate, time_period)
            created.append(self)
            if FlakyLimiter.fail_calls > 0:
                self._fail_next = True
                FlakyLimiter.fail_calls -= 1

    monkeypatch.setattr("utils.rate_limiter.AsyncLimiter", FlakyLimiter)

    limiter = ResilientRateLimiter(3, 1)

    async with limiter:
        pass

    assert len(created) == 2, "Limiter should recreate after loop mismatch"
    assert created[-1].enter_count == 1, "Second limiter should successfully acquire"
    assert created[-1].exit_count == 1, "Second limiter should release token"
