import os
import re
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import cast

from fastapi import BackgroundTasks, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from redis.commands.search.field import Field, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from src.adapters.redis_client import get_redis
from src.adapters.redis_manager import RedisEnvironment, RedisManager
from src.adapters.redis_repository import RedisRepository
from src.services.search_service import autocomplete, reset_repo


def get_latest_person_ids_file() -> dict | None:
    """Get info about the latest person IDs file in data/person/."""
    project_root = Path(__file__).parent.parent
    person_dir = project_root / "data" / "person"

    if not person_dir.exists():
        return None

    # Find all person_ids_*.json files
    files = list(person_dir.glob("person_ids_*.json"))
    if not files:
        return None

    # Get the most recent file by modification time
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    stat = latest_file.stat()

    # Format the modification time
    mod_time = datetime.fromtimestamp(stat.st_mtime)

    return {
        "filename": latest_file.name,
        "path": str(latest_file),
        "size_mb": round(stat.st_size / (1024 * 1024), 1),
        "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
        "modified_date": mod_time.strftime("%Y-%m-%d"),
    }

app = FastAPI()
templates = Jinja2Templates(directory="web/templates")

# Track background task status (ETL)
_task_status = {"running": False, "output": "", "error": ""}

# Track promote task status (separate from ETL task)
_promote_status = {"running": False, "output": "", "error": ""}

# Track ETL task status
_etl_status = {"running": False, "output": "", "error": ""}

# Track TMDB Extract task status (for running the extract scripts)
_extract_status: dict[str, dict] = {
    "tv": {"running": False, "output": "", "error": "", "progress": {}},
    "movie": {"running": False, "output": "", "error": "", "progress": {}},
}

# Track Person ETL task status
_person_download_status = {"running": False, "output": "", "error": ""}
_person_extract_status = {"running": False, "output": "", "error": "", "progress": {}}
_person_load_status = {"running": False, "output": "", "error": ""}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    current_env = RedisManager.get_current_env()
    return templates.TemplateResponse("home.html", {
        "request": request,
        "current_env": current_env.value,
    })


@app.get("/api/autocomplete")
async def api_autocomplete(q: str = Query(default="")):
    """JSON API endpoint for autocomplete search."""
    if not q or len(q) < 2:
        return JSONResponse(content=[])
    results = await autocomplete(q)
    return JSONResponse(content=results)


@app.get("/autocomplete_test", response_class=HTMLResponse)
async def autocomplete_test(request: Request, q: str = ""):
    results = await autocomplete(q) if q else []
    return templates.TemplateResponse("autocomplete.html",
                                      {"request": request, "query": q, "results": results})

@app.get("/management", response_class=HTMLResponse)
async def management(request: Request):
    """Management dashboard with Redis environment switcher and data loading."""
    current_env = RedisManager.get_current_env()

    # Test both connections
    local_status = await RedisManager.test_connection(RedisEnvironment.LOCAL)
    public_status = await RedisManager.test_connection(RedisEnvironment.PUBLIC)

    # Get stats from current connection
    try:
        repo = RedisRepository()
        stats = await repo.stats()
    except Exception as e:
        stats = {"error": str(e)}

    # Get latest person IDs file info
    person_ids_info = get_latest_person_ids_file()

    return templates.TemplateResponse("management.html", {
        "request": request,
        "current_env": current_env.value,
        "local_status": local_status,
        "public_status": public_status,
        "stats": stats,
        "task_status": _task_status,
        "person_ids_info": person_ids_info,
    })


@app.post("/api/switch-redis")
async def switch_redis(env: str = Query(...)):
    """Switch Redis environment (local or public)."""
    try:
        new_env = RedisEnvironment(env)
        RedisManager.set_current_env(new_env)
        reset_repo()  # Reset search service repository

        # Test new connection
        status = await RedisManager.test_connection(new_env)
        return JSONResponse(content={
            "success": True,
            "env": new_env.value,
            "status": status,
        })
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid environment: {env}"}
        )


@app.get("/api/redis-status")
async def redis_status():
    """Get status of both Redis environments."""
    current_env = RedisManager.get_current_env()
    local_status = await RedisManager.test_connection(RedisEnvironment.LOCAL)
    public_status = await RedisManager.test_connection(RedisEnvironment.PUBLIC)

    return JSONResponse(content={
        "current_env": current_env.value,
        "local": local_status,
        "public": public_status,
    })


@app.get("/api/redis-stats")
async def redis_stats():
    """Get current Redis stats for the active connection."""
    try:
        repo = RedisRepository()
        stats = await repo.stats()
        return JSONResponse(content={
            "success": True,
            "num_docs": stats.get("num_docs", 0),
            "dbsize": stats.get("dbsize", 0),
            "cache_breakdown": stats.get("cache_breakdown", {}),
            "memory_used": stats.get("info", {}).get("used_memory", 0),
            "memory_peak": stats.get("info", {}).get("used_memory_peak", 0),
            "index_stats": stats.get("index_stats", {}),
            "people_num_docs": stats.get("people_num_docs", 0),
            "people_index_stats": stats.get("people_index_stats", {}),
        })
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)})


def run_gcs_load_task(media_type: str, redis_host: str, redis_port: int, redis_password: str | None):
    """Run GCS metadata load in background with specified Redis connection."""
    global _task_status
    _task_status = {"running": True, "output": "", "error": ""}

    try:
        # Set up environment with the selected Redis connection
        env = os.environ.copy()
        env["REDIS_HOST"] = redis_host
        env["REDIS_PORT"] = str(redis_port)
        env["REDIS_PASSWORD"] = redis_password or ""
        # Force unbuffered Python output for real-time progress
        env["PYTHONUNBUFFERED"] = "1"

        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            [
                sys.executable,
                "-u",  # Unbuffered output
                "scripts/load_gcs_metadata.py",
                "--type", media_type,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # Read output in real-time
        output_lines = []
        assert process.stdout is not None  # For type checker
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                _task_status["output"] = "".join(output_lines)
            elif process.poll() is not None:
                break

        # Get any remaining output and errors
        remaining_stdout, stderr = process.communicate()
        if remaining_stdout:
            output_lines.append(remaining_stdout)
            _task_status["output"] = "".join(output_lines)
        if stderr:
            _task_status["error"] = stderr

    except Exception as e:
        _task_status["error"] = str(e)
    finally:
        _task_status["running"] = False


@app.post("/api/load-gcs-metadata")
async def load_gcs_metadata(
    background_tasks: BackgroundTasks,
    media_type: str = Query(default="all"),
):
    """Trigger GCS metadata load in background into the currently selected Redis."""
    global _task_status

    if _task_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "A load task is already running"}
        )

    if media_type not in ["movie", "tv", "all"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid media type: {media_type}"}
        )

    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    background_tasks.add_task(
        run_gcs_load_task,
        media_type,
        config.host,
        config.port,
        config.password,
    )

    return JSONResponse(content={
        "success": True,
        "message": f"Started loading {media_type} metadata from GCS into {config.name}",
    })


@app.get("/api/task-status")
async def task_status():
    """Get status of background task."""
    return JSONResponse(content=_task_status)


def run_promote_task():
    """Run promote to dev in background."""
    global _promote_status
    _promote_status = {"running": True, "output": "", "error": ""}

    try:
        # Set up environment
        env = os.environ.copy()
        # Force unbuffered Python output for real-time progress
        env["PYTHONUNBUFFERED"] = "1"

        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            [
                sys.executable,
                "-u",  # Unbuffered output
                "scripts/promote_to_dev.py",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # Read output in real-time
        output_lines = []
        assert process.stdout is not None  # For type checker
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                _promote_status["output"] = "".join(output_lines)
            elif process.poll() is not None:
                break

        # Get any remaining output and errors
        remaining_stdout, stderr = process.communicate()
        if remaining_stdout:
            output_lines.append(remaining_stdout)
            _promote_status["output"] = "".join(output_lines)
        if stderr:
            _promote_status["error"] = stderr

    except Exception as e:
        _promote_status["error"] = str(e)
    finally:
        _promote_status["running"] = False


@app.post("/api/promote-to-dev")
async def promote_to_dev(background_tasks: BackgroundTasks):
    """Promote local Redis documents to public Redis."""
    global _promote_status

    if _promote_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "A promote task is already running"}
        )

    background_tasks.add_task(run_promote_task)

    return JSONResponse(content={
        "success": True,
        "message": "Started promoting local Redis to dev",
    })


@app.get("/api/promote-status")
async def promote_status():
    """Get status of promote background task."""
    return JSONResponse(content=_promote_status)


def run_etl_task(
    media_type: str,
    load_all: bool = False,
    year: int | None = None,
    month: int | None = None,
    year_gte: int | None = None,
    year_lte: int | None = None,
    skip_gcs: bool = False,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
):
    """Run ETL in background with specified parameters."""
    global _etl_status
    _etl_status = {"running": True, "output": "", "error": ""}

    try:
        # Build command args
        cmd = [
            sys.executable,
            "-u",  # Unbuffered output
            "-m", "src.etl.run_etl",
            "--type", media_type,
        ]

        if load_all:
            cmd.append("--all")
        else:
            if year:
                cmd.extend(["--year", str(year)])
            if month:
                cmd.extend(["--month", str(month)])
            if year_gte:
                cmd.extend(["--year-gte", str(year_gte)])
            if year_lte:
                cmd.extend(["--year-lte", str(year_lte)])

        if skip_gcs:
            cmd.append("--no-gcs")

        # Set up environment with the selected Redis connection
        env = os.environ.copy()
        env["REDIS_HOST"] = redis_host
        env["REDIS_PORT"] = str(redis_port)
        env["REDIS_PASSWORD"] = redis_password or ""
        env["PYTHONUNBUFFERED"] = "1"

        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # Read output in real-time
        output_lines = []
        assert process.stdout is not None
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                _etl_status["output"] = "".join(output_lines)
            elif process.poll() is not None:
                break

        # Get any remaining output and errors
        remaining_stdout, stderr = process.communicate()
        if remaining_stdout:
            output_lines.append(remaining_stdout)
            _etl_status["output"] = "".join(output_lines)
        if stderr:
            _etl_status["error"] = stderr

    except Exception as e:
        _etl_status["error"] = str(e)
    finally:
        _etl_status["running"] = False


@app.post("/api/run-etl")
async def api_run_etl(
    background_tasks: BackgroundTasks,
    media_type: str = Query(...),
    load_all: bool = Query(default=False),
    year: int | None = Query(default=None),
    month: int | None = Query(default=None),
    year_gte: int | None = Query(default=None),
    year_lte: int | None = Query(default=None),
    skip_gcs: bool = Query(default=False),
):
    """Run ETL to load data into the currently selected Redis."""
    global _etl_status

    if _etl_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "An ETL task is already running"}
        )

    if media_type not in ["movie", "tv"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid media type: {media_type}"}
        )

    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    background_tasks.add_task(
        run_etl_task,
        media_type,
        load_all,
        year,
        month,
        year_gte,
        year_lte,
        skip_gcs,
        config.host,
        config.port,
        config.password,
    )

    # Build description of what's being loaded
    desc_parts = [media_type]
    if load_all:
        desc_parts.append("all files")
    elif year:
        desc_parts.append(f"year {year}")
        if month:
            desc_parts.append(f"month {month}")
    elif year_gte or year_lte:
        if year_gte and year_lte:
            desc_parts.append(f"years {year_gte}-{year_lte}")
        elif year_gte:
            desc_parts.append(f"year >= {year_gte}")
        else:
            desc_parts.append(f"year <= {year_lte}")

    return JSONResponse(content={
        "success": True,
        "message": f"Started ETL for {' '.join(desc_parts)} into {config.name}",
    })


@app.get("/api/etl-status")
async def etl_status():
    """Get status of ETL background task."""
    return JSONResponse(content=_etl_status)


def parse_extract_progress(output: str) -> dict[str, int | str]:
    """Parse ETL script output to extract progress information."""
    progress: dict[str, int | str] = {
        "month_current": 0,
        "month_total": 0,
        "batch_current": 0,
        "batch_total": 0,
        "current_month_label": "",
        "status_message": "",
    }

    # Find all month progress lines: "Processing month X/Y: YYYY-MM"
    month_matches = list(re.finditer(
        r"Processing month (\d+)/(\d+): (\d{4}-\d{2})",
        output
    ))
    if month_matches:
        last_match = month_matches[-1]
        progress["month_current"] = int(last_match.group(1))
        progress["month_total"] = int(last_match.group(2))
        progress["current_month_label"] = last_match.group(3)

    # Find all batch progress lines: "Processing batch X/Y (N items)"
    batch_matches = list(re.finditer(
        r"Processing batch (\d+)/(\d+)",
        output
    ))
    if batch_matches:
        last_match = batch_matches[-1]
        progress["batch_current"] = int(last_match.group(1))
        progress["batch_total"] = int(last_match.group(2))

    # Determine status message based on output
    month_current = cast(int, progress["month_current"])
    month_total = cast(int, progress["month_total"])
    batch_current = cast(int, progress["batch_current"])
    batch_total = cast(int, progress["batch_total"])
    current_month_label = cast(str, progress["current_month_label"])

    if "ETL process complete!" in output:
        progress["status_message"] = "Complete"
    elif month_current > 0:
        if batch_current > 0:
            progress["status_message"] = (
                f"Month {month_current}/{month_total}: "
                f"{current_month_label} - "
                f"Batch {batch_current}/{batch_total}"
            )
        else:
            progress["status_message"] = (
                f"Discovering shows for {current_month_label}..."
            )
    elif "Starting" in output:
        progress["status_message"] = "Starting extraction..."
    else:
        progress["status_message"] = "Initializing..."

    return progress


def run_extract_task(
    media_type: str,
    start_date: str,
    months_back: int,
):
    """Run TMDB extract ETL script in background."""
    global _extract_status
    _extract_status[media_type] = {
        "running": True,
        "output": "",
        "error": "",
        "progress": {},
    }

    try:
        # Get project root directory (parent of web/)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Determine which script to run
        script_name = f"run_tmdb_{media_type}_etl.sh"
        script_path = os.path.join(project_root, "scripts", script_name)

        # Set up environment
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        # Build command - the script takes: start_date, months_back, [output_dir]
        cmd = ["bash", script_path, start_date, str(months_back)]

        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            env=env,
            bufsize=1,  # Line buffered
            cwd=project_root,  # Run from project root
        )

        # Read output in real-time
        output_lines = []
        assert process.stdout is not None
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                output = "".join(output_lines)
                _extract_status[media_type]["output"] = output
                _extract_status[media_type]["progress"] = parse_extract_progress(output)
            elif process.poll() is not None:
                break

        # Get any remaining output
        remaining = process.stdout.read()
        if remaining:
            output_lines.append(remaining)
            output = "".join(output_lines)
            _extract_status[media_type]["output"] = output
            _extract_status[media_type]["progress"] = parse_extract_progress(output)

        # Check for errors
        if process.returncode != 0:
            _extract_status[media_type]["error"] = f"Process exited with code {process.returncode}"

    except Exception as e:
        _extract_status[media_type]["error"] = str(e)
    finally:
        _extract_status[media_type]["running"] = False
        # Final progress parse
        _extract_status[media_type]["progress"] = parse_extract_progress(
            _extract_status[media_type]["output"]
        )


@app.post("/api/run-extract")
async def api_run_extract(
    media_type: str = Query(...),
    start_date: str = Query(...),
    months_back: int = Query(...),
):
    """Run TMDB extract ETL to discover and enrich media from TMDB API."""
    global _extract_status

    if media_type not in ["movie", "tv"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid media type: {media_type}"}
        )

    if _extract_status[media_type]["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": f"An extract task for {media_type} is already running"}
        )

    # Validate start_date format (YYYY-MM)
    if not re.match(r"^\d{4}-\d{2}$", start_date):
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Invalid start_date format. Expected YYYY-MM"}
        )

    if months_back < 1 or months_back > 60:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "months_back must be between 1 and 60"}
        )

    # Use a dedicated thread instead of BackgroundTasks to avoid blocking the server
    thread = threading.Thread(
        target=run_extract_task,
        args=(media_type, start_date, months_back),
        daemon=True,
    )
    thread.start()

    return JSONResponse(content={
        "success": True,
        "message": f"Started {media_type} extraction from {start_date} for {months_back} month(s)",
    })


@app.get("/api/extract-status/{media_type}")
async def extract_status(media_type: str):
    """Get status of TMDB extract background task."""
    if media_type not in ["movie", "tv"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Invalid media type: {media_type}"}
        )
    return JSONResponse(content=_extract_status[media_type])


# ============================================================
# Person ETL Endpoints
# ============================================================

def run_person_download_task(date_str: str | None = None):
    """Run person ID bulk download in background."""
    global _person_download_status
    _person_download_status = {"running": True, "output": "", "error": ""}

    try:
        # Build command
        cmd = [sys.executable, "-u", "scripts/download_tmdb_person_ids.py"]
        if date_str:
            cmd.extend(["--date", date_str])

        # Set up environment
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        output_lines = []
        assert process.stdout is not None
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                _person_download_status["output"] = "".join(output_lines)
            elif process.poll() is not None:
                break

        remaining = process.stdout.read()
        if remaining:
            output_lines.append(remaining)
            _person_download_status["output"] = "".join(output_lines)

        if process.returncode != 0:
            _person_download_status["error"] = f"Process exited with code {process.returncode}"

    except Exception as e:
        _person_download_status["error"] = str(e)
    finally:
        _person_download_status["running"] = False


@app.post("/api/person/download")
async def api_person_download(
    background_tasks: BackgroundTasks,
    date: str | None = Query(default=None),
):
    """Download person IDs from TMDB daily export."""
    global _person_download_status

    if _person_download_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "A download task is already running"}
        )

    background_tasks.add_task(run_person_download_task, date)

    return JSONResponse(content={
        "success": True,
        "message": f"Started person ID download{' for ' + date if date else ''}",
    })


@app.get("/api/person/download-status")
async def person_download_status():
    """Get status of person download task."""
    return JSONResponse(content=_person_download_status)


def run_person_extract_task(source_file: str | None, limit: int | None):
    """Run person extract (enrich) in background."""
    global _person_extract_status
    _person_extract_status = {"running": True, "output": "", "error": "", "progress": {}}

    try:
        # Build command
        cmd = [sys.executable, "-u", "-c", f"""
import asyncio
import sys
sys.path.insert(0, '.')
from src.services.person_etl_service import run_person_extract

async def main():
    source = {source_file!r} if {source_file!r} else None
    limit = {limit!r}
    await run_person_extract(source_file=source, limit=limit)

asyncio.run(main())
"""]

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        output_lines = []
        assert process.stdout is not None
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                output = "".join(output_lines)
                _person_extract_status["output"] = output
                # Parse progress from output
                _person_extract_status["progress"] = parse_person_extract_progress(output)
            elif process.poll() is not None:
                break

        remaining = process.stdout.read()
        if remaining:
            output_lines.append(remaining)
            output = "".join(output_lines)
            _person_extract_status["output"] = output
            _person_extract_status["progress"] = parse_person_extract_progress(output)

        if process.returncode != 0:
            _person_extract_status["error"] = f"Process exited with code {process.returncode}"

    except Exception as e:
        _person_extract_status["error"] = str(e)
    finally:
        _person_extract_status["running"] = False


def parse_person_extract_progress(output: str) -> dict:
    """Parse person extract output to extract progress information."""
    progress: dict = {
        "batch_current": 0,
        "batch_total": 0,
        "status_message": "",
    }

    # Find batch progress: "Processing batch X/Y"
    batch_matches = list(re.finditer(r"Processing batch (\d+)/(\d+)", output))
    if batch_matches:
        last_match = batch_matches[-1]
        progress["batch_current"] = int(last_match.group(1))
        progress["batch_total"] = int(last_match.group(2))
        progress["status_message"] = f"Batch {progress['batch_current']}/{progress['batch_total']}"

    if "Extract Summary" in output:
        progress["status_message"] = "Complete"

    return progress


@app.post("/api/person/extract")
async def api_person_extract(
    background_tasks: BackgroundTasks,
    source_file: str | None = Query(default=None),
    limit: int | None = Query(default=None),
):
    """Extract and enrich person data from TMDB."""
    global _person_extract_status

    if _person_extract_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "An extract task is already running"}
        )

    background_tasks.add_task(run_person_extract_task, source_file, limit)

    return JSONResponse(content={
        "success": True,
        "message": "Started person extract and enrich",
    })


@app.get("/api/person/extract-status")
async def person_extract_status():
    """Get status of person extract task."""
    return JSONResponse(content=_person_extract_status)


def run_person_load_task(
    source_file: str | None,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
):
    """Run person load into Redis in background."""
    global _person_load_status
    _person_load_status = {"running": True, "output": "", "error": ""}

    try:
        # Build command
        cmd = [sys.executable, "-u", "-c", f"""
import asyncio
import sys
sys.path.insert(0, '.')
from src.services.person_etl_service import run_person_load

async def main():
    source = {source_file!r} if {source_file!r} else None
    await run_person_load(
        source_file=source,
        redis_host={redis_host!r},
        redis_port={redis_port!r},
        redis_password={redis_password!r},
    )

asyncio.run(main())
"""]

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        output_lines = []
        assert process.stdout is not None
        while True:
            line = process.stdout.readline()
            if line:
                output_lines.append(line)
                _person_load_status["output"] = "".join(output_lines)
            elif process.poll() is not None:
                break

        remaining = process.stdout.read()
        if remaining:
            output_lines.append(remaining)
            _person_load_status["output"] = "".join(output_lines)

        if process.returncode != 0:
            _person_load_status["error"] = f"Process exited with code {process.returncode}"

    except Exception as e:
        _person_load_status["error"] = str(e)
    finally:
        _person_load_status["running"] = False


@app.post("/api/person/load")
async def api_person_load(
    background_tasks: BackgroundTasks,
    source_file: str | None = Query(default=None),
):
    """Load enriched person data into Redis."""
    global _person_load_status

    if _person_load_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": "A load task is already running"}
        )

    # Get the currently selected Redis connection
    current_env = RedisManager.get_current_env()
    config = RedisManager.get_config(current_env)

    background_tasks.add_task(
        run_person_load_task,
        source_file,
        config.host,
        config.port,
        config.password,
    )

    return JSONResponse(content={
        "success": True,
        "message": f"Started person load into {config.name}",
    })


@app.get("/api/person/load-status")
async def person_load_status():
    """Get status of person load task."""
    return JSONResponse(content=_person_load_status)


@app.get("/api/person/files")
async def list_person_files():
    """List available person data files."""
    from pathlib import Path

    data_dir = Path("data/person")
    if not data_dir.exists():
        return JSONResponse(content={"id_files": [], "enriched_files": []})

    id_files = sorted(data_dir.glob("person_ids_*.json"), reverse=True)
    enriched_files = sorted(data_dir.glob("enriched_person_*.json"), reverse=True)

    return JSONResponse(content={
        "id_files": [{"name": f.name, "size": f.stat().st_size} for f in id_files],
        "enriched_files": [{"name": f.name, "size": f.stat().st_size} for f in enriched_files],
    })


@app.get("/api/media/{media_id}")
async def get_media(media_id: str):
    """Get a single media item by ID."""
    redis = get_redis()
    try:
        # Try with media: prefix first
        key = media_id if media_id.startswith("media:") else f"media:{media_id}"
        data = await redis.json().get(key)  # type: ignore[misc]
        if data:
            return JSONResponse(content={"id": key, **data})
        return JSONResponse(status_code=404, content={"error": "Media not found"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# Index configurations - maps index name to (redis_index_name, prefix, schema)
INDEX_CONFIGS = {
    "media": {
        "redis_name": "idx:media",
        "prefix": "media:",
        "schema": (
            # Primary search field with high weight
            TextField("$.search_title", as_name="search_title", weight=5.0),
            # Content type filters (MCType and MCSubType)
            TagField("$.mc_type", as_name="mc_type"),
            TagField("$.mc_subtype", as_name="mc_subtype"),
            # Source filter
            TagField("$.source", as_name="source"),
            # Sortable numeric fields for ranking
            NumericField("$.popularity", as_name="popularity", sortable=True),
            NumericField("$.rating", as_name="rating", sortable=True),
            NumericField("$.year", as_name="year", sortable=True),
        ),
    },
    "people": {
        "redis_name": "idx:people",
        "prefix": "person:",
        "schema": (
            # Primary search field (name) with high weight
            TextField("$.search_title", as_name="search_title", weight=5.0),
            # Also known as (alternate names) - searchable
            TextField("$.also_known_as", as_name="also_known_as", weight=3.0),
            # Content type filters (MCType and MCSubType)
            TagField("$.mc_type", as_name="mc_type"),
            TagField("$.mc_subtype", as_name="mc_subtype"),
            # Source filter
            TagField("$.source", as_name="source"),
            # Sortable numeric fields for ranking
            NumericField("$.popularity", as_name="popularity", sortable=True),
        ),
    },
}


@app.delete("/api/index/{index_name}")
async def delete_index(index_name: str):
    """Delete a Redis search index (keeps the data documents)."""
    if index_name not in INDEX_CONFIGS:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Unknown index: {index_name}"}
        )

    config = INDEX_CONFIGS[index_name]
    redis_index_name = config["redis_name"]
    redis = get_redis()

    try:
        await redis.ft(redis_index_name).dropindex(delete_documents=False)
        return JSONResponse(content={
            "success": True,
            "message": f"Index '{redis_index_name}' deleted successfully",
        })
    except Exception as e:
        error_msg = str(e)
        if "Unknown index name" in error_msg or "Unknown Index name" in error_msg:
            return JSONResponse(content={
                "success": True,
                "message": f"Index '{redis_index_name}' does not exist (already deleted)",
            })
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": error_msg}
        )


@app.post("/api/index/{index_name}")
async def create_index(index_name: str):
    """Create a Redis search index."""
    if index_name not in INDEX_CONFIGS:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Unknown index: {index_name}"}
        )

    config = INDEX_CONFIGS[index_name]
    redis_index_name = config["redis_name"]
    prefix = config["prefix"]
    schema: list[Field] = cast(list[Field], list(config["schema"]))
    redis = get_redis()

    definition = IndexDefinition(prefix=[prefix], index_type=IndexType.JSON)

    try:
        await redis.ft(redis_index_name).create_index(schema, definition=definition)
        return JSONResponse(content={
            "success": True,
            "message": f"Index '{redis_index_name}' created successfully",
        })
    except Exception as e:
        error_msg = str(e)
        if "Index already exists" in error_msg:
            return JSONResponse(content={
                "success": True,
                "message": f"Index '{redis_index_name}' already exists",
            })
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": error_msg}
        )


@app.get("/admin/index_info", response_class=HTMLResponse)
async def index_info(request: Request):
    redis = get_redis()
    try:
        raw = await redis.ft("idx:media").info()
        info = {}
        for i in range(0, len(raw), 2):
            key = raw[i]
            val = raw[i+1]
            info[key] = val
    except Exception as e:
        info = {"error": str(e)}

    return templates.TemplateResponse("admin_index.html",
                                      {"request": request, "info": info})
