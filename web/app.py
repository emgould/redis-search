import os
import subprocess
import sys

from fastapi import BackgroundTasks, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.adapters.redis_client import get_redis
from src.adapters.redis_manager import RedisEnvironment, RedisManager
from src.adapters.redis_repository import RedisRepository
from src.services.search_service import autocomplete, reset_repo

app = FastAPI()
templates = Jinja2Templates(directory="web/templates")

# Track background task status
_task_status = {"running": False, "output": "", "error": ""}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


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

    return templates.TemplateResponse("management.html", {
        "request": request,
        "current_env": current_env.value,
        "local_status": local_status,
        "public_status": public_status,
        "stats": stats,
        "task_status": _task_status,
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
