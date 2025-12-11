"""
OpenLibrary ETL API Routes

Provides endpoints for:
- Downloading OpenLibrary dump files
- Extracting and processing author data
- Loading authors into Redis index
- Running the full pipeline
"""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from adapters.redis_manager import RedisManager
from api.openlibrary.bulk.etl_handlers import (
    get_all_etl_status,
    get_etl_status,
    start_bulk_load,
    start_bulk_load_books,
    start_download_dumps,
    start_full_pipeline,
    start_load_book_index,
    start_load_index,
)
from web.auth import require_api_key

router = APIRouter(prefix="/api/openlibrary", tags=["openlibrary"])


@router.get("/etl/status")
async def get_ol_etl_status(_: None = Depends(require_api_key)):
    """Get status of all OpenLibrary ETL tasks."""
    return JSONResponse(
        content={
            "success": True,
            "tasks": get_all_etl_status(),
        }
    )


@router.get("/etl/status/{task_id}")
async def get_ol_etl_task_status(task_id: str, _: None = Depends(require_api_key)):
    """Get status of a specific ETL task."""
    status = get_etl_status(task_id)
    if not status:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": f"Task not found: {task_id}"},
        )

    return JSONResponse(
        content={
            "success": True,
            "task_id": task_id,
            "running": status.running,
            "operation": status.operation,
            "started_at": status.started_at,
            "completed_at": status.completed_at,
            "progress": status.progress,
            "error": status.error,
            "result": status.result,
        }
    )


@router.post("/etl/download")
async def trigger_download_dumps(
    dumps: str = Query(
        default="authors,wikidata",
        description="Comma-separated list of dumps to download (authors, wikidata)",
    ),
    _: None = Depends(require_api_key),
):
    """
    Download OpenLibrary dump files.

    This downloads the latest dump files from OpenLibrary:
    - ol_dump_authors_latest.txt.gz
    - ol_dump_wikidata_latest.txt.gz
    """
    dump_list = [d.strip() for d in dumps.split(",") if d.strip()]

    task_id = start_download_dumps(
        data_dir="data/openlibrary",
        dumps=dump_list if dump_list else None,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": f"Started download for: {', '.join(dump_list or ['authors', 'wikidata'])}",
            "task_id": task_id,
        }
    )


@router.post("/etl/extract")
async def trigger_bulk_load(
    wikidata_dump: str = Query(
        default="data/openlibrary/ol_dump_wikidata_latest.txt",
        description="Path to wikidata dump file",
    ),
    authors_dump: str = Query(
        default="data/openlibrary/ol_dump_authors_latest.txt",
        description="Path to authors dump file",
    ),
    output: str = Query(
        default="data/openlibrary/mc_authors.jsonl",
        description="Output JSONL file",
    ),
    _: None = Depends(require_api_key),
):
    """
    Extract and process author data from dump files.

    This scans the dump files and outputs MCAuthorItem records.
    """
    task_id = start_bulk_load(
        wikidata_dump=wikidata_dump,
        authors_dump=authors_dump,
        output_file=output,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": "Started extraction from dump files",
            "task_id": task_id,
        }
    )


@router.post("/etl/load")
async def trigger_load_index(
    input_file: str = Query(
        default="data/openlibrary/mc_authors.jsonl",
        description="Input JSONL file",
    ),
    recreate_index: bool = Query(
        default=False,
        description="Drop and recreate the index",
    ),
    _: None = Depends(require_api_key),
):
    """
    Load authors into Redis index.

    Reads the JSONL file and loads MCAuthorItems into the idx:author index.
    """
    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    task_id = start_load_index(
        input_file=input_file,
        redis_host=config.host,
        redis_port=config.port,
        redis_password=config.password,
        recreate_index=recreate_index,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": f"Started loading to Redis ({config.name})",
            "task_id": task_id,
        }
    )


@router.post("/etl/full")
async def trigger_full_pipeline(
    skip_download: bool = Query(
        default=False,
        description="Skip downloading (use existing dump files)",
    ),
    recreate_index: bool = Query(
        default=False,
        description="Drop and recreate the index",
    ),
    _: None = Depends(require_api_key),
):
    """
    Run the full ETL pipeline: download -> extract -> load.

    This runs all three steps in sequence:
    1. Download latest dump files (unless skip_download=True)
    2. Extract and process author data
    3. Load authors into Redis index
    """
    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    task_id = start_full_pipeline(
        data_dir="data/openlibrary",
        redis_host=config.host,
        redis_port=config.port,
        redis_password=config.password,
        skip_download=skip_download,
        recreate_index=recreate_index,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": f"Started full pipeline -> {config.name}",
            "task_id": task_id,
            "steps": ["download", "extract", "load"] if not skip_download else ["extract", "load"],
        }
    )


# ============================================================================
# Book ETL Endpoints
# ============================================================================


@router.post("/etl/extract-books")
async def trigger_extract_books(
    authors_jsonl: str = Query(
        default="data/openlibrary/mc_authors.jsonl",
        description="Path to mc_authors.jsonl with known authors",
    ),
    works_dump: str = Query(
        default="data/openlibrary/ol_dump_works_latest.txt",
        description="Path to OpenLibrary works dump file",
    ),
    output: str = Query(
        default="data/openlibrary/mc_books.jsonl",
        description="Output JSONL file",
    ),
    apply_quality_filter: bool = Query(
        default=True,
        description="Apply quality filter: cover AND (description OR subjects)",
    ),
    _: None = Depends(require_api_key),
):
    """
    Extract books from OpenLibrary works dump for known authors.

    This scans the works dump and outputs MCBookItem records for works
    by authors in the mc_authors.jsonl file.

    Quality filter (default enabled): Only includes works with a cover
    AND either a description or subjects.
    """
    task_id = start_bulk_load_books(
        authors_jsonl=authors_jsonl,
        works_dump=works_dump,
        output_file=output,
        apply_quality_filter=apply_quality_filter,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": f"Started book extraction (quality_filter={apply_quality_filter})",
            "task_id": task_id,
        }
    )


@router.post("/etl/load-books")
async def trigger_load_book_index(
    input_file: str = Query(
        default="data/openlibrary/mc_books.jsonl",
        description="Input JSONL file",
    ),
    recreate_index: bool = Query(
        default=False,
        description="Drop and recreate the index",
    ),
    _: None = Depends(require_api_key),
):
    """
    Load books into Redis index.

    Reads the JSONL file and loads MCBookItems into the idx:book index.
    """
    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    task_id = start_load_book_index(
        input_file=input_file,
        redis_host=config.host,
        redis_port=config.port,
        redis_password=config.password,
        recreate_index=recreate_index,
    )

    return JSONResponse(
        content={
            "success": True,
            "message": f"Started loading books to Redis ({config.name})",
            "task_id": task_id,
        }
    )

