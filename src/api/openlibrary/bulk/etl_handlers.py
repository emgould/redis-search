"""
OpenLibrary Bulk ETL Handlers

Provides functions to run OpenLibrary ETL operations that can be called
from the web app API endpoints.
"""

import asyncio
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)


@dataclass
class OpenLibraryETLStatus:
    """Status of an OpenLibrary ETL operation."""

    running: bool = False
    task_id: str = ""
    operation: str = ""  # "download", "extract", "load"
    started_at: str | None = None
    completed_at: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    result: dict[str, Any] | None = None


# Global status tracking
_ol_etl_status: dict[str, OpenLibraryETLStatus] = {}


def get_etl_status(task_id: str) -> OpenLibraryETLStatus | None:
    """Get status of an ETL task."""
    return _ol_etl_status.get(task_id)


def get_all_etl_status() -> dict[str, dict[str, Any]]:
    """Get status of all ETL tasks."""
    return {
        task_id: {
            "running": status.running,
            "operation": status.operation,
            "started_at": status.started_at,
            "completed_at": status.completed_at,
            "progress": status.progress,
            "error": status.error,
            "result": status.result,
        }
        for task_id, status in _ol_etl_status.items()
    }


async def download_dumps_task(
    task_id: str,
    data_dir: str,
    dumps: list[str] | None = None,
) -> None:
    """
    Background task to download OpenLibrary dumps.

    Args:
        task_id: Task identifier
        data_dir: Directory to save files
        dumps: Which dumps to download ("authors", "wikidata")
    """
    from api.openlibrary.bulk.download_dumps import download_dumps

    status = _ol_etl_status[task_id]

    try:
        status.progress = {"stage": "downloading", "dumps": dumps or ["authors", "wikidata"]}

        results = await download_dumps(
            data_dir=Path(data_dir),
            dumps=dumps,
            keep_gz=False,
        )

        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.result = {
            "success": all(results.values()),
            "dumps": results,
        }

    except Exception as e:
        logger.error(f"Download dumps failed: {e}")
        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.error = str(e)


async def bulk_load_task(
    task_id: str,
    wikidata_dump: str,
    authors_dump: str,
    output_file: str,
) -> None:
    """
    Background task to run bulk load pipeline.

    Args:
        task_id: Task identifier
        wikidata_dump: Path to wikidata dump
        authors_dump: Path to authors dump
        output_file: Output JSONL file
    """
    from api.openlibrary.bulk.bulk_load_openlibrary import run_pipeline

    status = _ol_etl_status[task_id]

    try:
        status.progress = {"stage": "extracting", "phase": "wikidata"}

        # Run the pipeline (blocking but fast)
        stats = run_pipeline(
            wikidata_dump=wikidata_dump,
            authors_dump=authors_dump,
            output_file=output_file,
        )

        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.result = {
            "success": stats["authors_found"] > 0,
            "stats": stats,
            "output_file": output_file,
        }

    except Exception as e:
        logger.error(f"Bulk load failed: {e}")
        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.error = str(e)


async def load_index_task(
    task_id: str,
    input_file: str,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    recreate_index: bool,
) -> None:
    """
    Background task to load authors into Redis index.

    Args:
        task_id: Task identifier
        input_file: Input JSONL file
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        recreate_index: Drop and recreate index
    """
    from api.openlibrary.bulk.load_author_index import load_authors_to_redis

    status = _ol_etl_status[task_id]

    try:
        status.progress = {"stage": "loading", "target": f"{redis_host}:{redis_port}"}

        stats = await load_authors_to_redis(
            input_file=input_file,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            recreate_index=recreate_index,
        )

        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.result = {
            "success": stats["loaded"] > 0,
            "stats": stats,
        }

    except Exception as e:
        logger.error(f"Load index failed: {e}")
        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.error = str(e)


def start_download_dumps(
    data_dir: str = "data/openlibrary",
    dumps: list[str] | None = None,
) -> str:
    """
    Start download dumps task in background.

    Returns:
        Task ID
    """
    task_id = f"ol_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="download",
        started_at=datetime.now().isoformat(),
    )

    thread = threading.Thread(
        target=lambda: asyncio.run(download_dumps_task(task_id, data_dir, dumps)),
        daemon=True,
    )
    thread.start()

    return task_id


def start_bulk_load(
    wikidata_dump: str = "data/openlibrary/ol_dump_wikidata_latest.txt",
    authors_dump: str = "data/openlibrary/ol_dump_authors_latest.txt",
    output_file: str = "data/openlibrary/mc_authors.jsonl",
) -> str:
    """
    Start bulk load task in background.

    Returns:
        Task ID
    """
    task_id = f"ol_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="extract",
        started_at=datetime.now().isoformat(),
    )

    thread = threading.Thread(
        target=lambda: asyncio.run(
            bulk_load_task(task_id, wikidata_dump, authors_dump, output_file)
        ),
        daemon=True,
    )
    thread.start()

    return task_id


def start_load_index(
    input_file: str = "data/openlibrary/mc_authors.jsonl",
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
    recreate_index: bool = False,
) -> str:
    """
    Start load index task in background.

    Returns:
        Task ID
    """
    task_id = f"ol_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    host = redis_host or os.getenv("REDIS_HOST", "localhost")
    port = redis_port or int(os.getenv("REDIS_PORT", "6380"))
    password = redis_password or os.getenv("REDIS_PASSWORD")

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="load",
        started_at=datetime.now().isoformat(),
    )

    thread = threading.Thread(
        target=lambda: asyncio.run(
            load_index_task(task_id, input_file, host, port, password, recreate_index)
        ),
        daemon=True,
    )
    thread.start()

    return task_id


async def bulk_load_books_task(
    task_id: str,
    authors_jsonl: str,
    works_dump: str,
    output_file: str,
    apply_quality_filter: bool = True,
) -> None:
    """
    Background task to run bulk load books pipeline.

    Args:
        task_id: Task identifier
        authors_jsonl: Path to mc_authors.jsonl
        works_dump: Path to works dump
        output_file: Output JSONL file
        apply_quality_filter: If True, only include works with cover AND (description OR subjects)
    """
    from api.openlibrary.bulk.bulk_load_books import run_pipeline

    status = _ol_etl_status[task_id]

    try:
        status.progress = {"stage": "extracting_books", "phase": "loading_authors"}

        # Run the pipeline (blocking but fast)
        stats = run_pipeline(
            authors_jsonl=authors_jsonl,
            works_dump=works_dump,
            output_file=output_file,
            apply_quality_filter=apply_quality_filter,
        )

        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.result = {
            "success": stats["works_found"] > 0,
            "stats": stats,
            "output_file": output_file,
        }

    except Exception as e:
        logger.error(f"Bulk load books failed: {e}")
        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.error = str(e)


async def load_book_index_task(
    task_id: str,
    input_file: str,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    recreate_index: bool,
) -> None:
    """
    Background task to load books into Redis index.

    Args:
        task_id: Task identifier
        input_file: Input JSONL file
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        recreate_index: Drop and recreate index
    """
    from api.openlibrary.bulk.load_book_index import load_books_to_redis

    status = _ol_etl_status[task_id]

    try:
        status.progress = {"stage": "loading_books", "target": f"{redis_host}:{redis_port}"}

        stats = await load_books_to_redis(
            input_file=input_file,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            recreate_index=recreate_index,
        )

        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.result = {
            "success": stats["loaded"] > 0,
            "stats": stats,
        }

    except Exception as e:
        logger.error(f"Load book index failed: {e}")
        status.running = False
        status.completed_at = datetime.now().isoformat()
        status.error = str(e)


def start_bulk_load_books(
    authors_jsonl: str = "data/openlibrary/mc_authors.jsonl",
    works_dump: str = "data/openlibrary/ol_dump_works_latest.txt",
    output_file: str = "data/openlibrary/mc_books.jsonl",
    apply_quality_filter: bool = True,
) -> str:
    """
    Start bulk load books task in background.

    Args:
        authors_jsonl: Path to mc_authors.jsonl
        works_dump: Path to works dump
        output_file: Output JSONL file
        apply_quality_filter: If True, only include works with cover AND (description OR subjects)

    Returns:
        Task ID
    """
    task_id = f"ol_extract_books_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="extract_books",
        started_at=datetime.now().isoformat(),
    )

    thread = threading.Thread(
        target=lambda: asyncio.run(
            bulk_load_books_task(
                task_id, authors_jsonl, works_dump, output_file, apply_quality_filter
            )
        ),
        daemon=True,
    )
    thread.start()

    return task_id


def start_load_book_index(
    input_file: str = "data/openlibrary/mc_books.jsonl",
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
    recreate_index: bool = False,
) -> str:
    """
    Start load book index task in background.

    Returns:
        Task ID
    """
    task_id = f"ol_load_books_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    host = redis_host or os.getenv("REDIS_HOST", "localhost")
    port = redis_port or int(os.getenv("REDIS_PORT", "6380"))
    password = redis_password or os.getenv("REDIS_PASSWORD")

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="load_books",
        started_at=datetime.now().isoformat(),
    )

    thread = threading.Thread(
        target=lambda: asyncio.run(
            load_book_index_task(task_id, input_file, host, port, password, recreate_index)
        ),
        daemon=True,
    )
    thread.start()

    return task_id


def start_full_pipeline(
    data_dir: str = "data/openlibrary",
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
    skip_download: bool = False,
    recreate_index: bool = False,
) -> str:
    """
    Start full ETL pipeline: download -> extract -> load.

    Returns:
        Task ID
    """
    task_id = f"ol_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    host = redis_host or os.getenv("REDIS_HOST", "localhost")
    port = redis_port or int(os.getenv("REDIS_PORT", "6380"))
    password = redis_password or os.getenv("REDIS_PASSWORD")

    _ol_etl_status[task_id] = OpenLibraryETLStatus(
        running=True,
        task_id=task_id,
        operation="full",
        started_at=datetime.now().isoformat(),
    )

    async def run_full_pipeline():
        status = _ol_etl_status[task_id]
        data_path = Path(data_dir)

        try:
            # Step 1: Download (optional)
            if not skip_download:
                from api.openlibrary.bulk.download_dumps import download_dumps

                status.progress = {"stage": "downloading"}
                results = await download_dumps(data_path)
                if not all(results.values()):
                    raise Exception(f"Download failed: {results}")

            # Step 2: Extract
            from api.openlibrary.bulk.bulk_load_openlibrary import run_pipeline

            status.progress = {"stage": "extracting"}
            wikidata_dump = str(data_path / "ol_dump_wikidata_latest.txt")
            authors_dump = str(data_path / "ol_dump_authors_latest.txt")
            output_file = str(data_path / "mc_authors.jsonl")

            stats = run_pipeline(
                wikidata_dump=wikidata_dump,
                authors_dump=authors_dump,
                output_file=output_file,
            )

            # Step 3: Load to Redis
            from api.openlibrary.bulk.load_author_index import load_authors_to_redis

            status.progress = {"stage": "loading"}
            load_stats = await load_authors_to_redis(
                input_file=output_file,
                redis_host=host,
                redis_port=port,
                redis_password=password,
                recreate_index=recreate_index,
            )

            status.running = False
            status.completed_at = datetime.now().isoformat()
            status.result = {
                "success": True,
                "extract_stats": stats,
                "load_stats": load_stats,
            }

        except Exception as e:
            logger.error(f"Full pipeline failed: {e}")
            status.running = False
            status.completed_at = datetime.now().isoformat()
            status.error = str(e)

    thread = threading.Thread(
        target=lambda: asyncio.run(run_full_pipeline()),
        daemon=True,
    )
    thread.start()

    return task_id
