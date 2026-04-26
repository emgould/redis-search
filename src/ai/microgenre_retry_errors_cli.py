from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from redis.asyncio import Redis

from adapters.config import load_env
from ai.microgenre_batch import (
    _classify_with_retries,
    collect_batch_records,
)
from ai.microgenre_batch_io import write_errors_file
from ai.microgenre_batch_models import (
    DEFAULT_BATCH_CONCURRENCY,
    MicroGenreBatchConfig,
    MicroGenreBatchErrorRecord,
    MicroGenreBatchInputRecord,
    MicroGenreBatchSidecarRecord,
)
from ai.prompts.microgenre_classifier import score_microgenres


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Retry a micro-genre errors.json artifact and write a new merged sidecar. "
            "The input sidecar and errors file are never modified."
        )
    )
    parser.add_argument("--errors-path", type=Path, required=True)
    parser.add_argument("--base-path", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_BATCH_CONCURRENCY)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-delay-seconds", type=float, default=1.0)
    parser.add_argument(
        "--overwrite-output",
        action="store_true",
        help="Allow replacing an existing output sidecar path.",
    )
    return parser.parse_args()


async def main() -> int:
    load_env()
    args = parse_args()

    if args.output_path.exists() and not args.overwrite_output:
        print(f"Output path already exists: {args.output_path}")
        print("Use --overwrite-output or choose a new --output-path.")
        return 2

    errors = _load_errors(args.errors_path)
    if not errors:
        print(f"No error rows found in {args.errors_path}")
        return 0

    base_rows = _load_sidecar_rows(args.base_path)
    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    try:
        records_by_key = await _records_by_key(redis)

        retry_rows: list[MicroGenreBatchSidecarRecord] = []
        for error in errors:
            record = records_by_key.get((error.media_type, error.mc_id))
            if record is None:
                retry_rows.append(_missing_metadata_row(error, args.output_path.stem))
                continue

            config = MicroGenreBatchConfig(
                media_type=error.media_type,
                concurrency=args.concurrency,
                score_threshold=error.score_threshold,
                max_retries=args.max_retries,
                retry_delay_seconds=args.retry_delay_seconds,
                output_path=args.output_path,
                run_id=args.output_path.stem.removeprefix("microgenre-results-"),
                rt_threshold=None,
            )
            retry_rows.append(await _classify_with_retries(record, config, score_microgenres))
            print(
                f"retried {error.media_type}:{error.mc_id} "
                f"status={'error' if retry_rows[-1].error else 'ok'}",
                flush=True,
            )

        retry_keys = {(row.media_type, row.mc_id) for row in retry_rows}
        merged_rows = [
            row for row in base_rows if (row.media_type, row.mc_id) not in retry_keys
        ] + retry_rows
        merged_rows.sort(key=lambda row: (row.input_position, row.media_type, row.mc_id))

        _write_sidecar_rows(args.output_path, merged_rows)
        write_errors_file(
            args.output_path, args.output_path.with_name(f"{args.output_path.stem}.errors.json")
        )

        print(
            f"wrote {len(merged_rows)} rows to {args.output_path} "
            f"({sum(1 for row in retry_rows if row.error)} retry errors)",
            flush=True,
        )
        return 1 if any(row.error for row in retry_rows) else 0
    finally:
        await redis.aclose()


def _load_errors(errors_path: Path) -> list[MicroGenreBatchErrorRecord]:
    loaded = json.loads(errors_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        raise ValueError(f"Errors file must contain a JSON array: {errors_path}")
    return [MicroGenreBatchErrorRecord(**row) for row in loaded if isinstance(row, dict)]


def _load_sidecar_rows(base_path: Path) -> list[MicroGenreBatchSidecarRecord]:
    rows: list[MicroGenreBatchSidecarRecord] = []
    with base_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(MicroGenreBatchSidecarRecord(**json.loads(line)))
    return rows


async def _records_by_key(
    redis: Redis,  # type: ignore[type-arg]
) -> dict[tuple[str, str], MicroGenreBatchInputRecord]:
    config = MicroGenreBatchConfig(media_type="both", rt_threshold=None)
    records = await collect_batch_records(redis, config)
    return {
        (record.media_type, record.mc_id): record
        for record in records
    }


def _missing_metadata_row(
    error: MicroGenreBatchErrorRecord,
    run_id: str,
) -> MicroGenreBatchSidecarRecord:
    return MicroGenreBatchSidecarRecord(
        run_id=run_id.removeprefix("microgenre-results-"),
        input_position=error.input_position,
        internal_idx=error.internal_idx,
        mc_id=error.mc_id,
        media_type=error.media_type,
        title=error.title,
        tmdb_id=error.tmdb_id,
        score_threshold=error.score_threshold,
        taxonomy_version=error.taxonomy_version,
        taxonomy_hash=error.taxonomy_hash,
        prompt_version=error.prompt_version,
        prompt_hash=error.prompt_hash,
        classification=None,
        error="Unable to retry classification because metadata was not found in local indexes.",
        error_type="runtime_exception",
        error_detail=f"No metadata record found for {error.media_type}:{error.mc_id}",
        raw_response_excerpt=None,
        execution_time=0.0,
        classified_at=error.classified_at,
    )


def _write_sidecar_rows(output_path: Path, rows: list[MicroGenreBatchSidecarRecord]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
