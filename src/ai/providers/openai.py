from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from dotenv import find_dotenv, load_dotenv
from openai import AsyncOpenAI
from pydantic import BaseModel, Field

# Import OpenAI exceptions if available
try:
    from openai import APIError, APITimeoutError, RateLimitError
except ImportError:
    # Fallback for older versions of OpenAI SDK
    APIError = None  # type: ignore[assignment,misc]
    RateLimitError = None  # type: ignore[assignment,misc]
    APITimeoutError = None  # type: ignore[assignment,misc]

from utils.get_logger import get_logger
from utils.parse_json import parse_json
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

load_dotenv(find_dotenv())

CacheExpiration = -1
AICache = RedisCache(
    defaultTTL=7 * 24 * 60 * 60,  # 24 hours
    prefix="ai",
    persist=False,
    verbose=False,
    isClassMethod=True,  # Fix: Used in OpenAIProvider class methods
    use_cloud_storage=False,
)


class AIResponse(BaseModel):
    response: Any | None = Field(None, description="The response from the AI model.")
    time: float = Field(0.0, description="The time it took to execute the request.")
    error: str | None = Field(None, description="The error message if the request failed.")
    text: str | None = Field(None, description="The text response from the AI model.")
    parsed: dict | None = Field(None, description="The JSON response from the AI model.")


class OpenAIProvider:
    """
    Async wrapper around GPT-5-Nano for structured text classification.
    Safe to use inside Firebase Gen 2 async handlers or other async runtimes.
    Uses lazy initialization to avoid httpx compatibility issues at import time.
    """

    def __init__(
        self,
        provider: str = "openai",
        model: str = "gpt-4o-mini",
        organization: str | None = None,
        verbose: bool = False,
    ):
        # Store config but don't create client yet (lazy initialization)
        self.provider = provider
        self.organization = organization
        self.model = model
        self.verbose = verbose
        self._client: AsyncOpenAI | None = None

    def _get_client(self, timeout: int = 10) -> AsyncOpenAI:
        """Lazy-load the OpenAI client to avoid import-time httpx issues."""
        if self._client is None:
            if self.provider == "openai":
                self._client = AsyncOpenAI(
                    api_key=os.getenv("OPENAI_API_KEY"),
                    organization=self.organization or os.getenv("OPENAI_ORGANIZATION"),
                    timeout=timeout,
                )
            elif self.provider == "cerebras":
                self._client = AsyncOpenAI(
                    api_key=os.getenv("CEREBRAS_API_KEY"),
                    base_url="https://api.cerebras.ai/v1",
                    timeout=timeout,
                )
            else:
                raise ValueError(f"Unknown provider: {self.provider}")
        return self._client

    @RedisCache.use_cache(AICache, prefix="openai")
    async def prompt_execute(
        self, input_data: str, retries: int = 3, timeout: int = 10, **kwargs
    ) -> AIResponse:
        """
        Asynchronously calls GPT-5-Nano and returns a validated ClassificationOutput.
        The input text is assumed to contain the full prompt.

        Args:
            input_data: User prompt content
            retries: Number of retry attempts
            timeout: Request timeout in seconds
            **kwargs: Additional parameters including:
                - system_prompt: Optional system prompt string
                - max_tokens: Maximum completion tokens (default: 1000)
                - temperature: Sampling temperature (default: 0.0)
        """
        try:
            t0 = time.perf_counter()
            response = AIResponse(
                response=None,
                time=0,
                error=None,
                text=None,
                parsed=None,
            )

            client = self._get_client(timeout)  # Lazy-load client

            # Build messages array
            messages: list[dict[str, str]] = []
            system_prompt = kwargs.get("system_prompt")
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": input_data})

            # Only use JSON response format if no system prompt (for backward compatibility)
            create_kwargs: dict[str, Any] = {
                "model": self.model,
                "messages": messages,
                "max_completion_tokens": kwargs.get("max_tokens", 1000),
                "temperature": kwargs.get("temperature", 0.0),
                "reasoning_effort": kwargs.get("reasoning_effort", "low"),
            }

            # Only set response_format for JSON when no system prompt
            if not system_prompt:
                create_kwargs["response_format"] = {"type": "json_object"}

            response.response = await client.chat.completions.create(
                **create_kwargs, timeout=float(timeout)
            )

            # Check if response structure is valid
            if not response.response or not response.response.choices:
                response.error = "Invalid response structure: no choices"
                response.text = None
                response.parsed = None
                return response

            if not response.response.choices[0].message:
                response.error = "Invalid response structure: no message"
                response.text = None
                response.parsed = None
                return response

            # The response content is in message.content
            choice = response.response.choices[0]
            response.text = choice.message.content

            # Check if response.text is None
            if response.text is None:
                finish = getattr(choice, "finish_reason", "unknown")
                logger.warning(
                    "Model returned None content (finish_reason=%s, model=%s)",
                    finish,
                    self.model,
                )
                response.error = f"Model returned None content (finish_reason={finish})"
                response.text = None
                response.parsed = None
                return response

            # Parse JSON only if no system prompt (for backward compatibility)
            # When system_prompt is used, response is plain text
            if not system_prompt:
                # Parse JSON safely
                try:
                    parsed = parse_json(response.text)
                except json.JSONDecodeError:
                    response.error = f"Model did not return valid JSON: {response.text}"
                    response.text = None
                    response.parsed = None
                    return response
                except AttributeError as e:
                    # Handle case where parse_json receives None or invalid input
                    response.error = f"Error parsing response: {str(e)}"
                    response.text = None
                    response.parsed = None
                    return response
                if not isinstance(parsed, dict):
                    response.error = f"Model did not return valid JSON: {response.text}"
                    response.text = None
                    response.parsed = None
                    return response
                response.parsed = parsed

            if self.verbose:
                logger.info(f"OpenAI response: {response.text}")
                logger.info(f"OpenAI JSON: {response.json}")

            t1 = time.perf_counter()
            if self.verbose:
                logger.info(f"OpenAI response time: {t1 - t0} seconds")
            response.time = t1 - t0
            return response

        except Exception as e:
            # Extract error details for better logging
            provider_name = "Cerebras" if self.provider == "cerebras" else "OpenAI"
            error_type = type(e).__name__
            error_message = str(e)
            status_code = None
            is_rate_limit = False
            is_timeout = False
            is_non_retryable_client_error = False

            # Check if it's a rate limit error (429)
            if (
                RateLimitError is not None
                and isinstance(e, RateLimitError)
                or hasattr(e, "status_code")
                and e.status_code == 429
                or "rate limit" in error_message.lower()
                or "429" in error_message
            ):
                is_rate_limit = True
                status_code = 429

            # Check if it's a timeout error
            if (
                APITimeoutError is not None
                and isinstance(e, APITimeoutError)
                or "timeout" in error_type.lower()
                or "timeout" in error_message.lower()
            ):
                is_timeout = True

            # Get status code if available
            if status_code is None:
                status_code = getattr(e, "status_code", None)
                if status_code is None and hasattr(e, "response"):
                    status_code = getattr(e.response, "status_code", None)

            if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                is_non_retryable_client_error = True

            # Log error details before retrying
            if is_rate_limit:
                logger.warning(
                    f"{provider_name} rate limit (429) encountered. Retrying after {2 ** (retries - 1)} seconds. "
                    f"{retries - 1} retries remaining. Error: {error_message}"
                )
            elif is_timeout:
                logger.warning(
                    f"{provider_name} API timeout encountered. Retrying after {2 ** (retries - 1)} seconds. "
                    f"{retries - 1} retries remaining. Error: {error_message}"
                )
            elif is_non_retryable_client_error:
                logger.warning(
                    "%s API error (HTTP %s): %s. Not retrying because this is a client/validation error. Error: %s",
                    provider_name,
                    status_code,
                    error_type,
                    error_message,
                )
            elif status_code:
                logger.warning(
                    f"{provider_name} API error (HTTP {status_code}): {error_type}. "
                    f"Retrying after {2 ** (retries - 1)} seconds. {retries - 1} retries remaining. "
                    f"Error: {error_message}"
                )
            else:
                logger.warning(
                    f"{provider_name} request failed: {error_type}. "
                    f"Retrying after {2 ** (retries - 1)} seconds. {retries - 1} retries remaining. "
                    f"Error: {error_message}"
                )

            if not is_non_retryable_client_error and retries > 0:
                # Use exponential backoff, but add extra delay for rate limits
                sleep_time = 2 ** (retries - 1)
                if is_rate_limit:
                    # Add extra delay for rate limits (up to 10 seconds)
                    sleep_time = min(sleep_time + 2, 10)

                await asyncio.sleep(sleep_time)
                result: AIResponse = await self.prompt_execute(
                    input_data, retries - 1, timeout, **kwargs
                )
                return result
            else:
                logger.error(
                    f"{provider_name} request failed after all retries exhausted. "
                    f"Error type: {error_type}, Status: {status_code}, "
                    f"Rate limit: {is_rate_limit}, Timeout: {is_timeout}, Error: {error_message}"
                )
                return AIResponse(
                    response=None,
                    time=0,
                    error=str(e) or "Unknown error",
                    text=None,
                    parsed=None,
                )

    async def prompt_execute_with_web_search(
        self, input_data: str, retries: int = 3, timeout: int = 10, **kwargs
    ) -> AIResponse:
        """Execute a prompt through OpenAI Responses with web search enabled."""
        try:
            t0 = time.perf_counter()
            response = AIResponse(
                response=None,
                time=0,
                error=None,
                text=None,
                parsed=None,
            )
            client = self._get_client(timeout)
            enable_web_search = kwargs.get("enable_web_search", True)
            create_kwargs: dict[str, Any] = {
                "model": self.model,
                "input": input_data,
                "max_output_tokens": kwargs.get("max_tokens", 1000),
                "temperature": kwargs.get("temperature", 1.0),
                "reasoning": {"effort": kwargs.get("reasoning_effort", "low")},
            }
            prompt_cache_key = kwargs.get("prompt_cache_key")
            if isinstance(prompt_cache_key, str) and prompt_cache_key:
                create_kwargs["prompt_cache_key"] = prompt_cache_key

            if enable_web_search:
                create_kwargs["tools"] = [
                    {
                        "type": "web_search",
                        "search_context_size": kwargs.get("search_context_size", "medium"),
                    }
                ]
            else:
                create_kwargs["text"] = {"format": {"type": "json_object"}}

            raw_response = await client.responses.create(**create_kwargs, timeout=float(timeout))
            response.response = raw_response
            output_text = getattr(raw_response, "output_text", None)
            if not isinstance(output_text, str) or not output_text:
                response.error = "Responses API returned no output text"
                response.text = None
                response.parsed = None
                return response

            response.text = output_text
            try:
                parsed = parse_json(output_text)
            except json.JSONDecodeError:
                response.error = f"Model did not return valid JSON: {output_text}"
                response.text = None
                response.parsed = None
                return response
            except AttributeError as e:
                response.error = f"Error parsing response: {str(e)}"
                response.text = None
                response.parsed = None
                return response
            if not isinstance(parsed, dict):
                response.error = f"Model did not return valid JSON: {output_text}"
                response.text = None
                response.parsed = None
                return response
            response.parsed = parsed

            response.time = time.perf_counter() - t0
            return response
        except Exception as e:
            provider_name = "Cerebras" if self.provider == "cerebras" else "OpenAI"
            error_message = str(e)
            status_code = getattr(e, "status_code", None)
            is_non_retryable_client_error = (
                status_code is not None and 400 <= status_code < 500 and status_code != 429
            )
            if not is_non_retryable_client_error and retries > 0:
                logger.warning(
                    "%s Responses API web-search request failed. Retrying after %s seconds. "
                    "%s retries remaining. Error: %s",
                    provider_name,
                    2 ** (retries - 1),
                    retries - 1,
                    error_message,
                )
                await asyncio.sleep(2 ** (retries - 1))
                return await self.prompt_execute_with_web_search(
                    input_data, retries - 1, timeout, **kwargs
                )
            logger.error(
                "%s Responses API web-search request failed after retries: %s",
                provider_name,
                error_message,
            )
            return AIResponse(
                response=None,
                time=0,
                error=error_message or "Unknown error",
                text=None,
                parsed=None,
            )
