from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, cast

from ai.microgenre_batch_models import (
    DEFAULT_OUTPUT_DIR,
    WEB_SEARCH_CUTOFF_DATE,
    MicroGenreBatchCheckpoint,
    MicroGenreBatchConfig,
    MicroGenreBatchErrorRecord,
    MicroGenreBatchSidecarRecord,
    MicroGenreBatchStatus,
)
from ai.prompts.microgenre_classifier import (
    PROMPT_HASH,
    PROMPT_VERSION,
    MicroGenreFailureType,
)
from ai.prompts.microgenre_taxonomy import TAXONOMY
from utils.get_logger import get_logger

logger = get_logger(__name__)


def resolve_batch_output_path(config: MicroGenreBatchConfig) -> Path:
    """Return the sidecar output path for a batch config."""
    if config.output_path is not None:
        return cast(Path, config.output_path)
    return cast(Path, DEFAULT_OUTPUT_DIR / f"microgenre-results-{config.run_id}.jsonl")


def checkpoint_path_for(output_path: Path) -> Path:
    """Return the checkpoint path adjacent to a sidecar JSONL path."""
    return output_path.with_suffix(".checkpoint.json")


def errors_path_for(output_path: Path) -> Path:
    """Return the errors artifact path adjacent to a sidecar JSONL path."""
    return output_path.with_name(f"{output_path.stem}.errors.json")


def load_completed_keys(output_path: Path, config: MicroGenreBatchConfig) -> set[str]:
    """Load completed classification keys from existing sidecar JSONL files."""
    return set(load_completed_rows(output_path, config))


def load_completed_rows(
    output_path: Path,
    config: MicroGenreBatchConfig,
) -> dict[str, MicroGenreBatchSidecarRecord]:
    """Load completed sidecar rows keyed by the current classifier contract."""
    sidecar_paths = _sidecar_paths_for_completion(output_path)
    if not sidecar_paths:
        if config.resume:
            logger.warning("Resume requested but no sidecar files exist near: %s", output_path)
        return {}

    completed: dict[str, MicroGenreBatchSidecarRecord] = {}
    for sidecar_path in sidecar_paths:
        with sidecar_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping invalid sidecar JSONL row in %s", sidecar_path)
                    continue
                key = _completion_key_from_row(row, config)
                if key is not None:
                    completed[key] = MicroGenreBatchSidecarRecord(**row)
    return completed


def append_sidecar_rows(output_path: Path, rows: list[MicroGenreBatchSidecarRecord]) -> None:
    """Append completed batch rows and flush them durably at the batch boundary."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def write_errors_file(output_path: Path, errors_path: Path) -> None:
    """Write compact errors JSON from all failed rows currently in the sidecar."""
    errors = load_error_records(output_path)
    if not errors:
        return
    errors_path.write_text(
        json.dumps([error.model_dump(mode="json") for error in errors], indent=2, sort_keys=True),
        encoding="utf-8",
    )


def load_error_records(output_path: Path) -> list[MicroGenreBatchErrorRecord]:
    """Load failed sidecar rows as compact error records."""
    if not output_path.exists():
        return []

    errors: list[MicroGenreBatchErrorRecord] = []
    with output_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping invalid sidecar JSONL row in %s", output_path)
                continue
            error = row.get("error")
            if not isinstance(error, str) or not error:
                continue
            errors.append(
                MicroGenreBatchErrorRecord(
                    run_id=str(row.get("run_id", "")),
                    input_position=int(row.get("input_position", 0)),
                    internal_idx=int(row.get("internal_idx", 0)),
                    mc_id=str(row.get("mc_id", "")),
                    media_type="tv" if row.get("media_type") == "tv" else "movie",
                    title=str(row.get("title", "")),
                    tmdb_id=str(row["tmdb_id"]) if row.get("tmdb_id") is not None else None,
                    error=error,
                    error_type=_error_type(row.get("error_type")),
                    error_detail=_optional_str(row.get("error_detail")),
                    raw_response_excerpt=_optional_str(row.get("raw_response_excerpt")),
                    score_threshold=float(row.get("score_threshold", 0.0)),
                    taxonomy_version=str(row.get("taxonomy_version", "")),
                    taxonomy_hash=str(row.get("taxonomy_hash", "")),
                    prompt_version=str(row.get("prompt_version", "")),
                    prompt_hash=str(row.get("prompt_hash", "")),
                    classified_at=float(row.get("classified_at", 0.0)),
                )
            )
    return errors


def _error_type(value: object) -> MicroGenreFailureType | None:
    if value in (
        "api_error",
        "empty_response",
        "invalid_json",
        "invalid_contract",
        "runtime_exception",
    ):
        return cast(MicroGenreFailureType, value)
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def write_checkpoint(
    checkpoint_path: Path,
    config: MicroGenreBatchConfig,
    status: MicroGenreBatchStatus,
    rows: list[MicroGenreBatchSidecarRecord],
) -> None:
    """Write a compact checkpoint file after a completed batch."""
    last_position = max((row.input_position for row in rows), default=None)
    checkpoint = MicroGenreBatchCheckpoint(
        run_id=config.run_id,
        media_type=config.media_type,
        skip=config.skip,
        take=config.take,
        batch_size=config.batch_size,
        concurrency=config.concurrency,
        score_threshold=config.score_threshold,
        rt_threshold=config.rt_threshold,
        retry_errors=config.retry_errors,
        web_search_cutoff_date=WEB_SEARCH_CUTOFF_DATE.isoformat(),
        last_completed_position=last_position,
        processed=status.processed,
        succeeded=status.succeeded,
        failed=status.failed,
        skipped_existing=status.skipped_existing,
        output_path=status.output_path,
        errors_path=status.errors_path,
        taxonomy_hash=TAXONOMY.taxonomy_hash,
        prompt_version=PROMPT_VERSION,
        prompt_hash=PROMPT_HASH,
        updated_at=time.time(),
    )
    checkpoint_path.write_text(
        json.dumps(checkpoint.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def completion_key(media_id: str, config: MicroGenreBatchConfig) -> str:
    """Return the resume key for one title under the current classifier contract."""
    return f"{media_id}|{PROMPT_HASH}|{config.score_threshold:.6f}"


def _sidecar_paths_for_completion(output_path: Path) -> list[Path]:
    if not output_path.parent.exists():
        return [output_path] if output_path.exists() else []

    paths = {path for path in output_path.parent.glob("*.jsonl") if path.is_file()}
    if output_path.exists():
        paths.add(output_path)
    return sorted(paths)


def _completion_key_from_row(row: dict[str, Any], config: MicroGenreBatchConfig) -> str | None:
    mc_id = row.get("mc_id")
    row_threshold = row.get("score_threshold")
    has_error = isinstance(row.get("error"), str) and bool(row.get("error"))
    has_classification = isinstance(row.get("classification"), dict)
    if has_error and config.retry_errors:
        return None
    if not has_error and not has_classification:
        return None
    if not isinstance(mc_id, str) or not isinstance(row_threshold, int | float):
        return None
    if abs(float(row_threshold) - config.score_threshold) > 0.000001:
        return None
    if row.get("prompt_hash") != PROMPT_HASH:
        return None
    return completion_key(mc_id, config)
