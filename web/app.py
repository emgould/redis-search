from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.adapters.redis_client import get_redis
from src.adapters.redis_repository import RedisRepository
from src.services.search_service import autocomplete

app = FastAPI()
templates = Jinja2Templates(directory="web/templates")


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
    repo = RedisRepository()
    stats = await repo.stats()
    return templates.TemplateResponse("management.html",
                                      {"request": request, "stats": stats})



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
