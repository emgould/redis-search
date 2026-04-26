from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field

from ai.prompts.microgenre_classifier import (
    DEFAULT_SCORE_THRESHOLD,
    MicroGenreFailureType,
    MicroGenreScoreResult,
)

MAX_BATCH_CONCURRENCY = 175
DEFAULT_BATCH_SIZE = 150
DEFAULT_BATCH_CONCURRENCY = 150
DEFAULT_RT_THRESHOLD = 0.7
DEFAULT_OUTPUT_DIR = Path("data/microgenre-classifications")
WEB_SEARCH_CUTOFF_DATE = date(2025, 6, 1)

MediaType = Literal["tv", "movie", "both"]
JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonDict = dict[str, JsonValue]


class MicroGenreBatchConfig(BaseModel):
    """Configuration for sidecar-first batch micro-genre classification."""

    media_type: MediaType = "both"
    batch_size: int = Field(default=DEFAULT_BATCH_SIZE, ge=1)
    concurrency: int = Field(default=DEFAULT_BATCH_CONCURRENCY, ge=1, le=MAX_BATCH_CONCURRENCY)
    score_threshold: float = Field(default=DEFAULT_SCORE_THRESHOLD, ge=0.0, le=1.0)
    rt_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    skip: int = Field(default=0, ge=0)
    take: int | None = Field(default=None, ge=1)
    force: bool = False
    retry_errors: bool = False
    dry_run: bool = False
    resume: bool = False
    checkpoint_every: int = Field(default=1, ge=1)
    max_retries: int = Field(default=2, ge=0)
    retry_delay_seconds: float = Field(default=1.0, ge=0.0)
    output_path: Path | None = None
    run_id: str = Field(default_factory=lambda: uuid4().hex[:12])


class MicroGenreBatchInputRecord(BaseModel):
    """A deterministic Redis media document selected for batch classification."""

    input_position: int
    internal_idx: int
    mc_id: str
    media_type: Literal["tv", "movie"]
    title: str
    tmdb_id: str | None = None
    media_doc: dict[str, object] = Field(exclude=True)


class MicroGenreBatchSidecarRecord(BaseModel):
    """One durable JSONL result row for a classified title."""

    run_id: str
    input_position: int
    internal_idx: int
    mc_id: str
    media_type: Literal["tv", "movie"]
    title: str
    tmdb_id: str | None = None
    score_threshold: float
    taxonomy_version: str
    taxonomy_hash: str
    prompt_version: str
    prompt_hash: str
    classification: MicroGenreScoreResult | None = None
    error: str | None = None
    error_type: MicroGenreFailureType | None = None
    error_detail: str | None = None
    raw_response_excerpt: str | None = None
    execution_time: float = 0.0
    classified_at: float


class MicroGenreBatchErrorRecord(BaseModel):
    """Compact error artifact row for failed batch classifications."""

    run_id: str
    input_position: int
    internal_idx: int
    mc_id: str
    media_type: Literal["tv", "movie"]
    title: str
    tmdb_id: str | None = None
    error: str
    error_type: MicroGenreFailureType | None = None
    error_detail: str | None = None
    raw_response_excerpt: str | None = None
    score_threshold: float
    taxonomy_version: str
    taxonomy_hash: str
    prompt_version: str
    prompt_hash: str
    classified_at: float


class MicroGenreBatchStatus(BaseModel):
    """Progress status for a batch classification run."""

    run_id: str
    stage: str = "queued"
    media_type: MediaType
    output_path: str
    checkpoint_path: str
    errors_path: str
    total_selected: int = 0
    planned: int = 0
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_existing: int = 0
    current_batch: int = 0
    total_batches: int = 0
    average_execution_time: float = 0.0
    started_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    completed_at: float | None = None
    error: str | None = None


class MicroGenreBatchCheckpoint(BaseModel):
    """Small recovery/status artifact written at checkpoint boundaries."""

    run_id: str
    media_type: MediaType
    skip: int
    take: int | None
    batch_size: int
    concurrency: int
    score_threshold: float
    rt_threshold: float | None
    retry_errors: bool
    web_search_cutoff_date: str
    last_completed_position: int | None
    processed: int
    succeeded: int
    failed: int
    skipped_existing: int
    output_path: str
    errors_path: str
    taxonomy_hash: str
    prompt_version: str
    prompt_hash: str
    updated_at: float
