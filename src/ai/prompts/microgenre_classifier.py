from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Literal, cast

from pydantic import BaseModel, Field

from ai.prompts.microgenre_prompts import MICROGENRE_SCORER_PROMPT
from ai.prompts.microgenre_taxonomy import (
    LEAF_IDS,
    TAXONOMY,
    TAXONOMY_BLOCK,
    JsonDict,
    JsonValue,
)
from ai.providers.models import CerebrasModels, OpenAIModels
from ai.providers.openai import AIResponse, OpenAIProvider
from utils.get_logger import get_logger

logger = get_logger(__name__)

PROMPT_VERSION = "microgenre-classifier-v3"
PROMPT_HASH = hashlib.sha256(
    MICROGENRE_SCORER_PROMPT.format(
        taxonomy_block=TAXONOMY_BLOCK,
        title_context="{title_context}",
        score_threshold="{score_threshold}",
    ).encode("utf-8")
).hexdigest()
DEFAULT_MODEL: str = OpenAIModels.GPT_5_6_TERRA
DEFAULT_CEREBRAS_MODEL: str = CerebrasModels.GPT_OSS
SUMMARY_MAX_CHARS = 4_000
ENRICHMENT_MAX_CHARS = 8_000
LLM_TIMEOUT_SECONDS = 45
LLM_MAX_TOKENS = 5_000
LLM_TEMPERATURE = 1.0
DEFAULT_SCORE_THRESHOLD = 0.1
MODEL_OUTPUT_MAX_ATTEMPTS = 3

MicroGenreProvider = Literal["openai", "cerebras"]
MicroGenreFailureType = Literal[
    "api_error",
    "empty_response",
    "invalid_json",
    "invalid_contract",
    "runtime_exception",
]
MicroGenreUnknownReason = Literal[
    "insufficient_evidence",
    "ambiguous_title",
    "conflicting_evidence",
]

_providers: dict[tuple[MicroGenreProvider, str], OpenAIProvider] = {}


def _default_model_for(provider: MicroGenreProvider) -> str:
    if provider == "cerebras":
        return DEFAULT_CEREBRAS_MODEL
    return DEFAULT_MODEL


def _get_provider(
    provider: MicroGenreProvider = "openai",
    model: str | None = None,
) -> OpenAIProvider:
    """Lazily construct (and cache) the micro-genre classification provider."""
    resolved_model = model or _default_model_for(provider)
    cache_key = (provider, resolved_model)
    existing = _providers.get(cache_key)
    if existing is not None:
        return existing
    created = OpenAIProvider(
        provider=provider,
        model=resolved_model,
        verbose=False,
    )
    _providers[cache_key] = created
    return created


class MicroGenreClassifyInput(BaseModel):
    """Title context supplied to the micro-genre classifier."""

    title: str = Field(..., description="Movie or TV series title.")
    media_type: Literal["tv", "movie"] = Field(..., description="Media kind.")
    year: int | None = Field(default=None, description="Release or first-air year.")
    summary: str | None = Field(default=None, description="Known plot/overview summary.")
    genres: list[str] | None = Field(default=None, description="Existing genre labels.")
    keywords: list[str] | None = Field(default=None, description="Known keywords or tags.")
    tmdb_id: str | None = Field(default=None, description="TMDB title id, when known.")
    id_imdb: str | None = Field(default=None, description="IMDb id, when known.")
    first_air_date: str | None = Field(default=None, description="TV first-air date, when known.")
    release_date: str | None = Field(default=None, description="Movie release date, when known.")
    enrichment_text: str | None = Field(
        default=None,
        description="Deterministic enrichment text fetched by the caller.",
    )
    enable_web_search: bool = Field(
        default=False,
        description="Whether the OpenAI Responses web_search tool should be enabled.",
    )
    score_threshold: float = Field(
        default=DEFAULT_SCORE_THRESHOLD,
        ge=0.0,
        le=1.0,
        description="Minimum score the model should emit; omitted labels are implicit zero.",
    )


class MicroGenreScoreResult(BaseModel):
    """Sparse canonical micro-genre score vector for a title."""

    microgenre_scores: dict[str, float]
    top_ids: list[str] = Field(default_factory=list)
    confidence: float
    rationale: str
    unknown: bool
    unknown_reason: MicroGenreUnknownReason | None = None
    score_threshold: float
    taxonomy_version: str
    taxonomy_hash: str
    prompt_version: str
    prompt_hash: str


class MicroGenreClassifyResponse(BaseModel):
    """Classifier wrapper matching the existing prompt module response pattern."""

    text: str
    result: MicroGenreScoreResult | None
    error: str | None = None
    error_type: MicroGenreFailureType | None = None
    error_detail: str | None = None
    raw_response_excerpt: str | None = None
    execution_time: float = 0.0


async def score_microgenres(
    input_data: MicroGenreClassifyInput,
    *,
    provider: MicroGenreProvider = "openai",
    model: str | None = None,
) -> MicroGenreClassifyResponse:
    """Score a title against every canonical taste-profile micro-genre.

    ``provider="openai"`` uses the Responses API path (optional web_search).
    On OpenAI API/empty failures (429, billing, timeouts, etc.) the call falls
    back once to Cerebras ``gpt-oss-120b``.

    ``provider="cerebras"`` uses chat completions only (no OpenAI attempt).
    """
    prompt = MICROGENRE_SCORER_PROMPT.format(
        taxonomy_block=TAXONOMY_BLOCK,
        title_context=_format_title_context(input_data),
        score_threshold=input_data.score_threshold,
    )
    enable_web_search = input_data.enable_web_search
    if provider == "cerebras" and enable_web_search:
        logger.warning(
            "web_search is not supported for provider=%s; continuing without it",
            provider,
        )
        enable_web_search = False

    t0 = time.time()
    primary = await _score_with_output_retries(
        prompt=prompt,
        score_threshold=input_data.score_threshold,
        provider=provider,
        model=model,
        enable_web_search=enable_web_search,
    )
    if primary.result is not None or provider != "openai":
        primary.execution_time = time.time() - t0
        return primary

    if primary.error_type not in ("api_error", "empty_response"):
        # Contract/JSON failures stay on the OpenAI attempt; do not burn Cerebras.
        primary.execution_time = time.time() - t0
        return primary

    logger.warning(
        "OpenAI microgenre classify failed (%s: %s); falling back to Cerebras %s",
        primary.error_type,
        primary.error,
        DEFAULT_CEREBRAS_MODEL,
    )
    fallback = await _score_with_output_retries(
        prompt=prompt,
        score_threshold=input_data.score_threshold,
        provider="cerebras",
        model=None,
        enable_web_search=False,
    )
    execution_time = time.time() - t0
    if fallback.result is not None:
        fallback.execution_time = execution_time
        return fallback

    fallback_error_type = fallback.error_type
    if fallback_error_type is None:
        fallback_error_type = "api_error"
    return _failure_response(
        text=fallback.text or primary.text,
        error_type=fallback_error_type,
        error=(f"OpenAI failed ({primary.error}); Cerebras fallback failed ({fallback.error})"),
        error_detail=(
            f"primary={primary.error_detail or primary.error}; "
            f"fallback={fallback.error_detail or fallback.error}"
        ),
        execution_time=execution_time,
    )


async def _score_with_output_retries(
    *,
    prompt: str,
    score_threshold: float,
    provider: MicroGenreProvider,
    model: str | None,
    enable_web_search: bool,
) -> MicroGenreClassifyResponse:
    """Retry malformed JSON/contract responses for either LLM provider."""
    response: MicroGenreClassifyResponse | None = None
    for attempt in range(1, MODEL_OUTPUT_MAX_ATTEMPTS + 1):
        response = await _score_with_provider(
            prompt=prompt,
            score_threshold=score_threshold,
            provider=provider,
            model=model,
            enable_web_search=enable_web_search,
        )
        if response.result is not None:
            return response
        if response.error_type not in ("invalid_json", "invalid_contract"):
            return response
        if attempt < MODEL_OUTPUT_MAX_ATTEMPTS:
            logger.warning(
                "%s microgenre output failed validation (%s; attempt %d/%d); retrying",
                provider,
                response.error_type,
                attempt,
                MODEL_OUTPUT_MAX_ATTEMPTS,
            )

    if response is None:
        raise RuntimeError("Microgenre output retry loop produced no response")
    return response


async def _score_with_provider(
    *,
    prompt: str,
    score_threshold: float,
    provider: MicroGenreProvider,
    model: str | None,
    enable_web_search: bool,
) -> MicroGenreClassifyResponse:
    llm = _get_provider(provider=provider, model=model)
    t0 = time.time()
    if provider == "cerebras":
        # Title-unique prompts have near-zero cache hit rate; at backfill
        # concurrency the AI RedisCache read/lock path exhausts the pool.
        response = await llm.prompt_execute(
            prompt,
            temperature=LLM_TEMPERATURE,
            timeout=LLM_TIMEOUT_SECONDS,
            max_tokens=LLM_MAX_TOKENS,
            no_cache=True,
        )
    else:
        response = await llm.prompt_execute_with_web_search(
            prompt,
            temperature=LLM_TEMPERATURE,
            timeout=LLM_TIMEOUT_SECONDS,
            max_tokens=LLM_MAX_TOKENS,
            search_context_size="medium",
            enable_web_search=enable_web_search,
            prompt_cache_key=f"mgc:{PROMPT_HASH[:32]}",
        )
    execution_time = time.time() - t0
    return _response_to_classify_result(
        response=response,
        provider=provider,
        score_threshold=score_threshold,
        execution_time=execution_time,
    )


def _response_to_classify_result(
    *,
    response: AIResponse | None,
    provider: MicroGenreProvider,
    score_threshold: float,
    execution_time: float,
) -> MicroGenreClassifyResponse:
    if response is None or response.error:
        error = response.error if response else f"{provider} provider returned no response."
        error_type: MicroGenreFailureType = "api_error"
        if response is not None and response.error:
            normalized_error = response.error.lower()
            if "valid json" in normalized_error or "parsing response" in normalized_error:
                error_type = "invalid_json"
            elif "no output text" in normalized_error or "empty response" in normalized_error:
                error_type = "empty_response"
        return _failure_response(
            text=response.text if response and response.text else "",
            error_type=error_type,
            error=error,
            error_detail=(
                response.error
                if response
                else f"No response object returned by {provider} provider."
            ),
            execution_time=execution_time,
        )

    if response.text is None:
        return _failure_response(
            text="",
            error_type="empty_response",
            error=f"{provider} returned an empty response.",
            error_detail="The provider response completed without text content.",
            execution_time=execution_time,
        )

    parsed = _extract_json_response(response.parsed, response.text)
    if parsed is None:
        logger.debug("Micro-genre classifier returned unparseable JSON: %.500s", response.text)
        return _failure_response(
            text=response.text,
            error_type="invalid_json",
            error="Model returned non-JSON output.",
            error_detail="The LLM call succeeded, but the model output was not parseable JSON.",
            execution_time=execution_time,
        )

    result, error = _build_result(parsed, score_threshold)
    if error is not None:
        logger.debug("Micro-genre classifier returned invalid contract response: %s", error)
        return _failure_response(
            text=response.text,
            error_type="invalid_contract",
            error="Model returned JSON that does not match the micro-genre classifier contract.",
            error_detail=error,
            execution_time=execution_time,
        )

    return MicroGenreClassifyResponse(
        text=response.text,
        result=result,
        error=None,
        execution_time=execution_time,
    )


def _failure_response(
    text: str,
    error_type: MicroGenreFailureType,
    error: str,
    error_detail: str,
    execution_time: float,
) -> MicroGenreClassifyResponse:
    return MicroGenreClassifyResponse(
        text=text,
        result=None,
        error=error,
        error_type=error_type,
        error_detail=error_detail,
        raw_response_excerpt=_raw_response_excerpt(text),
        execution_time=execution_time,
    )


def _format_title_context(input_data: MicroGenreClassifyInput) -> str:
    parts = [
        f"title: {input_data.title}",
        f"media_type: {input_data.media_type}",
    ]
    if input_data.year is not None:
        parts.append(f"year: {input_data.year}")
    if input_data.tmdb_id:
        parts.append(f"tmdb_id: {input_data.tmdb_id}")
    if input_data.id_imdb:
        parts.append(f"id_imdb: {input_data.id_imdb}")
    if input_data.first_air_date:
        parts.append(f"first_air_date: {input_data.first_air_date}")
    if input_data.release_date:
        parts.append(f"release_date: {input_data.release_date}")
    if input_data.genres:
        parts.append(f"genres: {', '.join(input_data.genres)}")
    if input_data.keywords:
        parts.append(f"keywords: {', '.join(input_data.keywords)}")
    if input_data.summary:
        parts.append(f"summary: {_truncate(input_data.summary, SUMMARY_MAX_CHARS)}")
    if input_data.enrichment_text:
        parts.append(
            f"enrichment_text: {_truncate(input_data.enrichment_text, ENRICHMENT_MAX_CHARS)}"
        )
    return "\n".join(parts)


def _truncate(value: str, max_chars: int) -> str:
    cleaned = value.strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return f"{cleaned[:max_chars]}...[truncated]"


def _extract_json_response(parsed: dict | None, text: str) -> JsonDict | None:
    if parsed and isinstance(parsed, dict):
        return cast(JsonDict, parsed)
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(loaded, dict):
        return None
    return cast(JsonDict, loaded)


def _build_result(
    parsed: JsonDict,
    score_threshold: float,
) -> tuple[MicroGenreScoreResult | None, str | None]:
    unknown = _bool_value(parsed.get("unknown"))
    confidence = 0.0 if unknown else _clamp_confidence(parsed.get("confidence"))

    if unknown:
        scores: dict[str, float] = {}
        top_ids: list[str] = []
        unknown_reason, unknown_reason_error = _unknown_reason(parsed.get("unknown_reason"))
        if unknown_reason_error is not None:
            return None, unknown_reason_error
    else:
        scores, error = _parse_sparse_scores(parsed, score_threshold)
        if error is not None:
            return None, error
        top_ids = _top_ids_from_scores(scores)
        unknown_reason = None

    return (
        MicroGenreScoreResult(
            microgenre_scores=scores,
            top_ids=top_ids,
            confidence=confidence,
            rationale=_optional_str(parsed.get("rationale")) or "",
            unknown=unknown,
            unknown_reason=unknown_reason,
            score_threshold=score_threshold,
            taxonomy_version=TAXONOMY.version,
            taxonomy_hash=TAXONOMY.taxonomy_hash,
            prompt_version=PROMPT_VERSION,
            prompt_hash=PROMPT_HASH,
        ),
        None,
    )


def _parse_sparse_scores(
    parsed: JsonDict,
    score_threshold: float,
) -> tuple[dict[str, float], str | None]:
    raw_scores = parsed.get("microgenre_scores")
    if raw_scores is None:
        raw_scores = parsed.get("scores")
    if not isinstance(raw_scores, dict):
        return (
            {},
            (
                "The model returned unknown=false but did not return the required "
                "microgenre_scores object. If the model could not classify the title, "
                "it must return unknown=true with microgenre_scores={}, top_ids=[], "
                "confidence=0.0, and unknown_reason."
            ),
        )

    invalid_ids = sorted(str(label_id) for label_id in raw_scores if label_id not in LEAF_IDS)
    if invalid_ids:
        return {}, f"Model returned unknown micro-genre ids: {invalid_ids[:5]}"

    scores: dict[str, float] = {}
    for leaf_id, raw_score in raw_scores.items():
        score = _score_value(raw_score)
        if score is None:
            return {}, f"Model returned non-numeric score for {leaf_id}."
        if score >= score_threshold:
            scores[str(leaf_id)] = score
    return _sort_scores_desc(scores), None


def _top_ids_from_scores(scores: dict[str, float]) -> list[str]:
    return list(_sort_scores_desc(scores))[:3]


def _sort_scores_desc(scores: dict[str, float]) -> dict[str, float]:
    return dict(sorted(scores.items(), key=lambda item: item[1], reverse=True))


def _optional_str(value: JsonValue | object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _unknown_reason(value: JsonValue | object) -> tuple[MicroGenreUnknownReason | None, str | None]:
    if value in ("insufficient_evidence", "ambiguous_title", "conflicting_evidence"):
        return cast(MicroGenreUnknownReason, value), None
    return (
        None,
        (
            "The model returned unknown=true but did not provide a valid unknown_reason. "
            "Expected one of: insufficient_evidence, ambiguous_title, conflicting_evidence."
        ),
    )


def _raw_response_excerpt(text: str) -> str | None:
    cleaned = text.strip()
    if not cleaned:
        return None
    return _truncate(cleaned, 1_000)


def _bool_value(value: JsonValue | object) -> bool:
    return value is True


def _string_list(value: JsonValue | object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _score_value(value: JsonValue | object) -> float | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    return max(0.0, min(1.0, float(value)))


def _clamp_confidence(value: JsonValue | object) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return 0.0
    return max(0.0, min(1.0, float(value)))


if __name__ == "__main__":

    async def _test() -> None:
        print("\n=== Micro-Genre Classifier Test ===\n")
        print("Enter: title | year | tv|movie | summary")
        raw = input().strip()
        title, year, media_type, summary = [part.strip() for part in raw.split("|", maxsplit=3)]
        result = await score_microgenres(
            MicroGenreClassifyInput(
                title=title,
                year=int(year) if year else None,
                media_type=cast(Literal["tv", "movie"], media_type),
                summary=summary or None,
            )
        )
        print("\n=== Raw Model Output ===\n")
        print(result.text)
        print("\n=== Parsed Micro-Genre Classification ===\n")
        print(result.model_dump())

    asyncio.run(_test())
