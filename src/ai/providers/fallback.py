"""
Shared fallback executor for AI prompt calls.

Owns provider construction and the fallback execution policy:
  1. Call Cerebras primary provider.
  2. If `response is None` or `response.error`, call OpenAI backup provider.

Prompt modules remain responsible for prompt construction, request parameters,
parsing, and module-specific error handling.
"""

from __future__ import annotations

from ai.providers.models import CerebrasModels
from ai.providers.openai import AIResponse, OpenAIProvider
from utils.get_logger import get_logger

logger = get_logger(__name__)

_primary_provider: OpenAIProvider | None = None
_backup_provider: OpenAIProvider | None = None


def _get_primary_provider() -> OpenAIProvider:
    """Lazily construct the Cerebras primary provider."""
    global _primary_provider
    if _primary_provider is None:
        _primary_provider = OpenAIProvider(
            provider="cerebras",
            model=CerebrasModels.GPT_OSS.value,
            verbose=False,
        )
    return _primary_provider


def _get_backup_provider() -> OpenAIProvider:
    """Lazily construct the OpenAI backup provider."""
    global _backup_provider
    if _backup_provider is None:
        _backup_provider = OpenAIProvider(
            provider="openai",
            model="gpt-4o-mini",
            verbose=False,
        )
    return _backup_provider


async def execute_with_fallback(
    prompt: str,
    *,
    temperature: float,
    timeout: int,
    max_tokens: int,
) -> AIResponse:
    """Execute a prompt against Cerebras, falling back to OpenAI on error.

    Args:
        prompt: The fully constructed prompt text.
        temperature: Sampling temperature for the model.
        timeout: Request timeout in seconds.
        max_tokens: Maximum completion tokens.

    Returns:
        The ``AIResponse`` from whichever provider succeeded, or the final
        error response if both providers failed.
    """
    primary = _get_primary_provider()
    response: AIResponse = await primary.prompt_execute(
        prompt,
        temperature=temperature,
        timeout=timeout,
        max_tokens=max_tokens,
    )

    if response is not None and not response.error:
        return response

    primary_error = response.error if response else "no response"
    logger.warning(
        "Primary provider (Cerebras) failed (%s) — falling back to OpenAI",
        primary_error,
    )

    backup = _get_backup_provider()
    response = await backup.prompt_execute(
        prompt,
        temperature=temperature,
        timeout=timeout,
        max_tokens=max_tokens,
    )

    if response is not None and not response.error:
        return response

    backup_error = response.error if response else "no response"
    logger.error(
        "Backup provider (OpenAI) also failed (%s). Primary error was: %s",
        backup_error,
        primary_error,
    )
    return response if response is not None else AIResponse(
        response=None,
        time=0,
        error=f"Both providers failed. Primary: {primary_error}; Backup: {backup_error}",
        text=None,
        parsed=None,
    )
