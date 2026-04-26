from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Literal, cast

from redis.asyncio import Redis

from ai.microgenre_batch_io import (
    append_sidecar_rows,
    checkpoint_path_for,
    completion_key,
    errors_path_for,
    load_completed_rows,
    resolve_batch_output_path,
    write_checkpoint,
    write_errors_file,
)
from ai.microgenre_batch_models import (
    WEB_SEARCH_CUTOFF_DATE,
    JsonDict,
    MediaType,
    MicroGenreBatchConfig,
    MicroGenreBatchInputRecord,
    MicroGenreBatchSidecarRecord,
    MicroGenreBatchStatus,
)
from ai.prompts.microgenre_classifier import (
    PROMPT_HASH,
    PROMPT_VERSION,
    MicroGenreClassifyInput,
    MicroGenreClassifyResponse,
    MicroGenreFailureType,
    MicroGenreScoreResult,
    score_microgenres,
)
from ai.prompts.microgenre_taxonomy import TAXONOMY
from utils.get_logger import get_logger

logger = get_logger(__name__)

ScorerFunc = Callable[[MicroGenreClassifyInput], Awaitable[MicroGenreClassifyResponse]]
StatusCallback = Callable[["MicroGenreBatchStatus"], None]

SCAN_COUNT = 1000
MGET_BATCH_SIZE = 500


async def collect_batch_records(
    redis: Redis,  # type: ignore[type-arg]
    config: MicroGenreBatchConfig,
) -> list[MicroGenreBatchInputRecord]:
    """Collect deterministic TV/Movie records from Redis media documents."""
    selected_types = _selected_media_types(config.media_type)
    records: list[MicroGenreBatchInputRecord] = []
    internal_idx = 0

    keys: list[str] = []
    async for raw_key in redis.scan_iter(match="media:*", count=SCAN_COUNT):
        keys.append(str(raw_key))

    for batch_start in range(0, len(keys), MGET_BATCH_SIZE):
        batch_keys = keys[batch_start : batch_start + MGET_BATCH_SIZE]
        raw_docs = await redis.execute_command("JSON.MGET", *batch_keys, "$")
        if not isinstance(raw_docs, list):
            continue
        for raw_doc in raw_docs:
            doc = _json_doc(raw_doc)
            if doc is None:
                continue
            media_type = _doc_media_type(doc)
            if media_type is None or media_type not in selected_types:
                continue
            title = _optional_str_value(doc.get("title") or doc.get("search_title"))
            mc_id = _optional_str_value(doc.get("mc_id") or doc.get("id"))
            if title is None or mc_id is None:
                continue

            record = MicroGenreBatchInputRecord(
                input_position=0,
                internal_idx=internal_idx,
                mc_id=mc_id,
                media_type=media_type,
                title=title,
                tmdb_id=_tmdb_id_from_doc(doc),
                media_doc=doc,
            )
            internal_idx += 1
            if config.rt_threshold is None or _passes_rt_threshold(record, config.rt_threshold):
                records.append(record)

    records.sort(key=lambda item: (item.media_type, item.mc_id))
    for position, record in enumerate(records):
        record.input_position = position

    ranged = records[config.skip :]
    if config.take is not None:
        ranged = ranged[: config.take]
    return ranged


async def run_microgenre_batch(
    redis: Redis,  # type: ignore[type-arg]
    config: MicroGenreBatchConfig,
    scorer: ScorerFunc = score_microgenres,
    status_callback: StatusCallback | None = None,
) -> MicroGenreBatchStatus:
    """Run sidecar-first batch classification with checkpointed recovery."""
    output_path = resolve_batch_output_path(config)
    checkpoint_path = checkpoint_path_for(output_path)
    errors_path = errors_path_for(output_path)
    status = MicroGenreBatchStatus(
        run_id=config.run_id,
        stage="collecting",
        media_type=config.media_type,
        output_path=str(output_path),
        checkpoint_path=str(checkpoint_path),
        errors_path=str(errors_path),
    )
    _emit_status(status, status_callback)

    selected_records = await collect_batch_records(redis, config)
    completed_rows = {} if config.force else load_completed_rows(output_path, config)
    completed_keys = set(completed_rows)
    carried_rows = [
        completed_rows[completion_key(record.mc_id, config)]
        for record in selected_records
        if completion_key(record.mc_id, config) in completed_keys
    ]
    pending_records = [
        record
        for record in selected_records
        if completion_key(record.mc_id, config) not in completed_keys
    ]

    status.total_selected = len(selected_records)
    status.skipped_existing = len(selected_records) - len(pending_records)
    status.planned = len(pending_records)
    status.total_batches = _batch_count(len(pending_records), config.batch_size)
    status.stage = "dry_run_complete" if config.dry_run else "running"
    status.updated_at = time.time()
    _emit_status(status, status_callback)

    if carried_rows and not output_path.exists() and not config.dry_run:
        append_sidecar_rows(output_path, carried_rows)
        write_errors_file(output_path, errors_path)

    if config.dry_run or not pending_records:
        status.stage = "complete"
        status.completed_at = time.time()
        status.updated_at = status.completed_at
        if carried_rows and not config.dry_run:
            write_checkpoint(checkpoint_path, config, status, carried_rows)
        _emit_status(status, status_callback)
        return status

    output_path.parent.mkdir(parents=True, exist_ok=True)
    execution_times: list[float] = []

    for batch_index, batch in enumerate(_chunks(pending_records, config.batch_size), start=1):
        status.current_batch = batch_index
        status.stage = "running"
        status.updated_at = time.time()
        _emit_status(status, status_callback)

        rows = await _process_batch(batch, config, scorer)
        append_sidecar_rows(output_path, rows)

        status.processed += len(rows)
        status.succeeded += sum(1 for row in rows if row.error is None)
        status.failed += sum(1 for row in rows if row.error is not None)
        execution_times.extend(row.execution_time for row in rows if row.execution_time > 0)
        status.average_execution_time = (
            sum(execution_times) / len(execution_times) if execution_times else 0.0
        )
        status.updated_at = time.time()

        if batch_index % config.checkpoint_every == 0 or batch_index == status.total_batches:
            write_checkpoint(checkpoint_path, config, status, rows)
            write_errors_file(output_path, errors_path)
        _emit_status(status, status_callback)

    status.stage = "complete"
    status.completed_at = time.time()
    status.updated_at = status.completed_at
    _emit_status(status, status_callback)
    return status


def build_microgenre_input_from_document(
    media_doc: JsonDict,
    media_type: Literal["tv", "movie"],
    score_threshold: float,
) -> MicroGenreClassifyInput:
    """Build scorer input from the Redis media document shape."""
    title = _optional_str_value(media_doc.get("title") or media_doc.get("search_title"))
    if title is None:
        title = _optional_str_value(media_doc.get("mc_id") or media_doc.get("id")) or "Unknown"
    first_air_date = _optional_str_value(media_doc.get("first_air_date"))
    release_date = _optional_str_value(media_doc.get("release_date"))

    return MicroGenreClassifyInput(
        title=title,
        media_type=media_type,
        year=_extract_year(media_doc),
        first_air_date=first_air_date,
        release_date=release_date,
        summary=_optional_str_value(media_doc.get("overview")),
        genres=_metadata_str_list(media_doc.get("genres")),
        keywords=_metadata_str_list(media_doc.get("keywords")),
        tmdb_id=_tmdb_id_from_doc(media_doc),
        id_imdb=_imdb_id_from_doc(media_doc),
        enrichment_text=None,
        enable_web_search=_should_enable_web_search(media_type, first_air_date, release_date),
        score_threshold=score_threshold,
    )


def build_microgenre_input_from_record(
    record: MicroGenreBatchInputRecord,
    score_threshold: float,
) -> MicroGenreClassifyInput:
    """Build scorer input from a selected batch record."""
    return build_microgenre_input_from_document(
        record.media_doc,
        record.media_type,
        score_threshold,
    )


def _selected_media_types(media_type: MediaType) -> tuple[Literal["tv", "movie"], ...]:
    if media_type == "tv":
        return ("tv",)
    if media_type == "movie":
        return ("movie",)
    return ("tv", "movie")


def _doc_media_type(doc: JsonDict) -> Literal["tv", "movie"] | None:
    value = doc.get("mc_type")
    if value == "tv":
        return "tv"
    if value == "movie":
        return "movie"
    return None


def _passes_rt_threshold(record: MicroGenreBatchInputRecord, rt_threshold: float) -> bool:
    if record.media_type != "movie":
        return True

    audience_score = _normalized_score(record.media_doc.get("rt_audience_score"))
    critics_score = _normalized_score(record.media_doc.get("rt_critics_score"))
    return (
        audience_score is not None
        and audience_score > rt_threshold
        or critics_score is not None
        and critics_score > rt_threshold
    )


def _normalized_score(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        score = float(value)
    elif isinstance(value, str):
        try:
            score = float(value.strip().removesuffix("%"))
        except ValueError:
            return None
    else:
        return None

    if score > 1.0:
        score /= 100.0
    return max(0.0, min(1.0, score))


def _should_enable_web_search(
    media_type: Literal["tv", "movie"],
    first_air_date: str | None,
    release_date: str | None,
) -> bool:
    date_value = first_air_date if media_type == "tv" else release_date
    parsed_date = _date_value(date_value)
    return parsed_date is not None and parsed_date >= WEB_SEARCH_CUTOFF_DATE


def _date_value(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


async def _process_batch(
    batch: list[MicroGenreBatchInputRecord],
    config: MicroGenreBatchConfig,
    scorer: ScorerFunc,
) -> list[MicroGenreBatchSidecarRecord]:
    semaphore = asyncio.Semaphore(config.concurrency)

    async def _classify(record: MicroGenreBatchInputRecord) -> MicroGenreBatchSidecarRecord:
        async with semaphore:
            return await _classify_with_retries(record, config, scorer)

    return list(await asyncio.gather(*(_classify(record) for record in batch)))


async def _classify_with_retries(
    record: MicroGenreBatchInputRecord,
    config: MicroGenreBatchConfig,
    scorer: ScorerFunc,
) -> MicroGenreBatchSidecarRecord:
    last_error: str | None = None
    last_error_type: MicroGenreFailureType | None = None
    last_error_detail: str | None = None
    last_raw_response_excerpt: str | None = None
    for attempt in range(config.max_retries + 1):
        try:
            input_data = build_microgenre_input_from_record(record, config.score_threshold)
            response = await scorer(input_data)
            if response.error is None and response.result is not None:
                return _sidecar_row(
                    record,
                    config,
                    response.result,
                    None,
                    None,
                    None,
                    None,
                    response.execution_time,
                )
            last_error = response.error or "Classifier returned no result"
            last_error_type = response.error_type
            last_error_detail = response.error_detail
            last_raw_response_excerpt = response.raw_response_excerpt
        except Exception as exc:
            last_error = str(exc)
            last_error_type = "runtime_exception"
            last_error_detail = f"{type(exc).__name__}: {exc}"
            last_raw_response_excerpt = None

        if attempt < config.max_retries:
            await asyncio.sleep(config.retry_delay_seconds * (2**attempt))

    logger.warning(
        "Micro-genre classification failed after retries for %s (%s): %s",
        record.title,
        record.mc_id,
        last_error or "Classification failed",
    )
    return _sidecar_row(
        record,
        config,
        None,
        last_error or "Classification failed",
        last_error_type,
        last_error_detail,
        last_raw_response_excerpt,
        0.0,
    )


def _sidecar_row(
    record: MicroGenreBatchInputRecord,
    config: MicroGenreBatchConfig,
    result: MicroGenreScoreResult | None,
    error: str | None,
    error_type: MicroGenreFailureType | None,
    error_detail: str | None,
    raw_response_excerpt: str | None,
    execution_time: float,
) -> MicroGenreBatchSidecarRecord:
    return MicroGenreBatchSidecarRecord(
        run_id=config.run_id,
        input_position=record.input_position,
        internal_idx=record.internal_idx,
        mc_id=record.mc_id,
        media_type=record.media_type,
        title=record.title,
        tmdb_id=record.tmdb_id,
        score_threshold=config.score_threshold,
        taxonomy_version=TAXONOMY.version,
        taxonomy_hash=TAXONOMY.taxonomy_hash,
        prompt_version=PROMPT_VERSION,
        prompt_hash=PROMPT_HASH,
        classification=result,
        error=error,
        error_type=error_type,
        error_detail=error_detail,
        raw_response_excerpt=raw_response_excerpt,
        execution_time=execution_time,
        classified_at=time.time(),
    )


def _chunks(
    records: list[MicroGenreBatchInputRecord], batch_size: int
) -> list[list[MicroGenreBatchInputRecord]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]


def _batch_count(item_count: int, batch_size: int) -> int:
    if item_count == 0:
        return 0
    return (item_count + batch_size - 1) // batch_size


def _json_doc(value: object) -> JsonDict | None:
    if isinstance(value, str):
        try:
            return _json_doc(json.loads(value))
        except json.JSONDecodeError:
            return None
    if isinstance(value, list):
        if not value:
            return None
        return _json_doc(value[0])
    if isinstance(value, dict):
        return cast(JsonDict, value)
    return None


def _optional_str_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _metadata_str_list(value: object) -> list[str] | None:
    if isinstance(value, list):
        resolved = [_metadata_list_item_to_str(item) for item in value]
        resolved = [item for item in resolved if item]
        return resolved or None
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return None


def _metadata_list_item_to_str(item: object) -> str:
    if isinstance(item, dict):
        value = item.get("name") or item.get("title") or item.get("id")
        return str(value).strip() if value is not None else ""
    return str(item).strip()


def _extract_year(media_doc: JsonDict) -> int | None:
    for key in ("year", "maxYear"):
        value = media_doc.get(key)
        if value is not None:
            try:
                return int(str(value))
            except (TypeError, ValueError):
                pass

    for key in ("release_date", "first_air_date"):
        value = media_doc.get(key)
        if isinstance(value, str) and len(value) >= 4 and value[:4].isdigit():
            return int(value[:4])
    return None


def _tmdb_id_from_doc(media_doc: JsonDict) -> str | None:
    if media_doc.get("source") == "tmdb":
        return _optional_str_value(media_doc.get("source_id"))
    value = _optional_str_value(media_doc.get("tmdb_id"))
    if value is not None:
        return value
    mc_id = _optional_str_value(media_doc.get("mc_id") or media_doc.get("id"))
    if mc_id is None:
        return None
    for prefix in ("tmdb_movie_", "tmdb_tv_", "tmdb_"):
        if mc_id.startswith(prefix):
            return mc_id.removeprefix(prefix)
    return None


def _imdb_id_from_doc(media_doc: JsonDict) -> str | None:
    value = _optional_str_value(media_doc.get("id_imdb") or media_doc.get("imdb_id"))
    if value is not None:
        return value
    external_ids = media_doc.get("external_ids")
    if isinstance(external_ids, dict):
        return _optional_str_value(external_ids.get("imdb_id"))
    return None


def _emit_status(status: MicroGenreBatchStatus, callback: StatusCallback | None) -> None:
    if callback is not None:
        callback(status.model_copy(deep=True))
