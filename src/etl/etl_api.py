"""
ETL API - HTTP endpoints for triggering and monitoring ETL jobs.

This module provides a FastAPI application for:
- Triggering the full nightly ETL via HTTP (for Cloud Scheduler)
- Triggering individual ETL jobs manually
- Monitoring ETL run status and history
- Viewing job configurations
"""

import os
from datetime import datetime
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.adapters.config import load_env
from src.etl.etl_metadata import ETLMetadataStore
from src.etl.etl_runner import ETLConfig, ETLRunner, run_single_etl
from src.utils.get_logger import get_logger

logger = get_logger(__name__)

# Load environment
load_env()

app = FastAPI(
    title="ETL API",
    description="HTTP endpoints for triggering and monitoring TMDB ETL jobs",
    version="1.0.0",
)

# CORS - needed for web app access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# Track running ETL task
_etl_task_status: dict[str, Any] = {
    "running": False,
    "run_id": None,
    "started_at": None,
    "progress": {},
    "error": None,
    "result": None,
}


# Track individual job runs
_job_task_status: dict[str, dict[str, Any]] = {}


# =============================================================================
# Pydantic Models
# =============================================================================


class TriggerETLRequest(BaseModel):
    """Request body for triggering ETL."""

    start_date: str | None = None  # YYYY-MM-DD
    end_date: str | None = None  # YYYY-MM-DD
    job_filter: list[str] | None = None  # Only run specific jobs


class TriggerJobRequest(BaseModel):
    """Request body for triggering a single job."""

    media_type: str  # tv, movie, person
    start_date: str | None = None
    end_date: str | None = None
    verbose: bool = False


class ETLStatusResponse(BaseModel):
    """Response model for ETL status."""

    running: bool
    run_id: str | None = None
    started_at: str | None = None
    progress: dict[str, Any] = {}
    error: str | None = None


# =============================================================================
# Background Task Functions
# =============================================================================


async def run_full_etl_task(
    start_date: str | None = None,
    end_date: str | None = None,
    job_filter: list[str] | None = None,
) -> None:
    """Background task to run full ETL."""
    global _etl_task_status

    try:
        config = ETLConfig.from_env()
        runner = ETLRunner(config)

        _etl_task_status["progress"] = {
            "total_jobs": sum(len(j.runs) for j in config.jobs if j.enabled),
            "jobs_completed": 0,
            "current_job": None,
        }

        result = await runner.run_all(
            start_date_override=start_date,
            end_date_override=end_date,
            job_filter=job_filter,
        )

        _etl_task_status["result"] = result.to_dict()
        _etl_task_status["progress"]["jobs_completed"] = result.jobs_completed

    except Exception as e:
        logger.error(f"ETL task failed: {e}")
        _etl_task_status["error"] = str(e)

    finally:
        _etl_task_status["running"] = False


async def run_single_job_task(
    task_id: str,
    media_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    verbose: bool = False,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
) -> None:
    """Background task to run a single ETL job."""
    global _job_task_status

    try:
        stats = await run_single_etl(
            media_type=media_type,
            start_date=start_date,
            end_date=end_date,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            verbose=verbose,
        )

        _job_task_status[task_id]["result"] = stats.to_dict()

    except Exception as e:
        logger.error(f"Job task {task_id} failed: {e}")
        _job_task_status[task_id]["error"] = str(e)

    finally:
        _job_task_status[task_id]["running"] = False
        _job_task_status[task_id]["completed_at"] = datetime.now().isoformat()


# =============================================================================
# HTTP Endpoints
# =============================================================================


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "etl-api"}


@app.get("/config")
async def get_config():
    """Get the current ETL configuration."""
    try:
        config = ETLConfig.from_env()
        return JSONResponse(content={
            "success": True,
            "config": config.to_dict(),
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.get("/jobs")
async def list_jobs():
    """List all configured ETL jobs."""
    try:
        config = ETLConfig.from_env()
        jobs = []
        for job in config.jobs:
            jobs.append({
                "name": job.name,
                "target": job.target,
                "enabled": job.enabled,
                "runs": [r.to_dict() for r in job.runs],
            })
        return JSONResponse(content={
            "success": True,
            "jobs": jobs,
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.post("/trigger")
async def trigger_full_etl(
    background_tasks: BackgroundTasks,
    request: TriggerETLRequest | None = None,
):
    """
    Trigger the full ETL run.

    This is the endpoint that Cloud Scheduler will call.
    It can also be called manually from the web UI.
    """
    global _etl_task_status

    if _etl_task_status["running"]:
        return JSONResponse(
            status_code=409,
            content={
                "success": False,
                "error": "An ETL run is already in progress",
                "run_id": _etl_task_status["run_id"],
            },
        )

    # Initialize task status
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    _etl_task_status = {
        "running": True,
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "progress": {},
        "error": None,
        "result": None,
    }

    # Parse request
    start_date = request.start_date if request else None
    end_date = request.end_date if request else None
    job_filter = request.job_filter if request else None

    # Start background task
    background_tasks.add_task(
        run_full_etl_task,
        start_date=start_date,
        end_date=end_date,
        job_filter=job_filter,
    )

    return JSONResponse(content={
        "success": True,
        "message": "ETL run started",
        "run_id": run_id,
    })


@app.get("/status")
async def get_etl_status():
    """Get the status of the current or most recent ETL run."""
    return JSONResponse(content={
        "running": _etl_task_status["running"],
        "run_id": _etl_task_status.get("run_id"),
        "started_at": _etl_task_status.get("started_at"),
        "progress": _etl_task_status.get("progress", {}),
        "error": _etl_task_status.get("error"),
        "result": _etl_task_status.get("result"),
    })


@app.post("/job/trigger")
async def trigger_single_job(
    background_tasks: BackgroundTasks,
    media_type: str = Query(..., description="Media type: tv, movie, or person"),
    start_date: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    verbose: bool = Query(False, description="Enable verbose logging"),
):
    """
    Trigger a single ETL job for a specific media type.

    This is useful for manual runs from the web UI.
    """
    global _job_task_status

    if media_type not in ["tv", "movie", "person"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid media_type: {media_type}"},
        )

    # Check if this job is already running
    for task_id, status in _job_task_status.items():
        if status.get("running") and status.get("media_type") == media_type:
            return JSONResponse(
                status_code=409,
                content={
                    "success": False,
                    "error": f"A {media_type} job is already running",
                    "task_id": task_id,
                },
            )

    # Create task ID
    task_id = f"{media_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Get Redis config
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6380"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Initialize task status
    _job_task_status[task_id] = {
        "running": True,
        "media_type": media_type,
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "error": None,
        "result": None,
    }

    # Start background task
    background_tasks.add_task(
        run_single_job_task,
        task_id=task_id,
        media_type=media_type,
        start_date=start_date,
        end_date=end_date,
        verbose=verbose,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
    )

    return JSONResponse(content={
        "success": True,
        "message": f"ETL job started for {media_type}",
        "task_id": task_id,
    })


@app.get("/job/status/{task_id}")
async def get_job_status(task_id: str):
    """Get the status of a specific job task."""
    if task_id not in _job_task_status:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": f"Task not found: {task_id}"},
        )

    return JSONResponse(content={
        "success": True,
        "task_id": task_id,
        **_job_task_status[task_id],
    })


@app.get("/job/status")
async def list_job_statuses():
    """List all job task statuses."""
    return JSONResponse(content={
        "success": True,
        "tasks": _job_task_status,
    })


@app.get("/runs")
async def list_runs(
    run_date: str | None = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit: int = Query(10, description="Maximum runs to return"),
):
    """List recent ETL runs from GCS metadata."""
    try:
        store = ETLMetadataStore()
        runs = store.list_runs(run_date=run_date, limit=limit)
        return JSONResponse(content={
            "success": True,
            "runs": runs,
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.get("/runs/{run_date}/{run_id}")
async def get_run(run_date: str, run_id: str):
    """Get details of a specific ETL run."""
    try:
        store = ETLMetadataStore()
        metadata = store.get_run_metadata(run_date, run_id)

        if not metadata:
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Run not found"},
            )

        return JSONResponse(content={
            "success": True,
            "run": metadata.to_dict(),
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.get("/job/state")
async def get_job_states():
    """Get the persistent state of all jobs (last run times, etc.)."""
    try:
        store = ETLMetadataStore()
        states = store.get_all_job_states()
        return JSONResponse(content={
            "success": True,
            "states": {name: state.to_dict() for name, state in states.items()},
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    """Run the ETL API server."""
    import uvicorn

    port = int(os.getenv("PORT", "8081"))
    uvicorn.run("src.etl.etl_api:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()

