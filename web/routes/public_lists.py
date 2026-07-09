"""
Public List discovery API routes.

Read endpoints are public (all indexed content is public by
definition). Write endpoints are guarded by the public-list shared
secret because this index is runtime-owned by the MediaCircle backend
projection pipeline.
"""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from core.public_list_queries import PublicListQueryError
from core.public_lists import PUBLIC_LIST_INDEX, PublicListUpsertRequest
from services.public_list_service import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    delete_public_list,
    ensure_public_list_index,
    get_public_list,
    search_public_lists,
    upsert_public_list,
)
from utils.get_logger import get_logger
from web.auth import require_public_list_write_key

logger = get_logger(__name__)

router = APIRouter(prefix="/api/public-lists", tags=["public-lists"])


@router.get("/search")
async def api_search_public_lists(
    q: str | None = Query(default=None, description="Free text; '@name' searches owners"),
    owner_username: str | None = Query(default=None, description="Owner username filter"),
    owner_id: str | None = Query(default=None, description="Exact owner id filter"),
    topic: str | None = Query(default=None, description="Topic keyword/label filter"),
    item_mc_id: str | None = Query(
        default=None, description="Exact contained-item mc_id (e.g. tmdb_238)"
    ),
    item_title: str | None = Query(
        default=None, description="Contained-item title (fallback when mc_id unknown)"
    ),
    sort: str = Query(
        default="relevance",
        description="Sort: relevance, followers, engagement, recent, created",
    ),
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0),
):
    """
    Search public Lists.

    Notes:
    - MediaCircle system-owned Lists are included in text/topic/title
      results but excluded from ``followers`` and ``engagement`` sorts.
    - ``total`` is the full match count for pagination.
    """
    try:
        payload = await search_public_lists(
            q=q,
            owner_username=owner_username,
            owner_id=owner_id,
            topic=topic,
            item_mc_id=item_mc_id,
            item_title=item_title,
            sort=sort,
            limit=limit,
            offset=offset,
        )
    except PublicListQueryError as exc:
        return JSONResponse(status_code=400, content={"success": False, "error": str(exc)})
    except Exception as exc:  # noqa: BLE001 - surface redis errors as 500 JSON
        if "no such index" in str(exc).lower():
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": f"{PUBLIC_LIST_INDEX} does not exist; create it first",
                },
            )
        logger.error(f"public-lists search failed: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(exc)})

    return JSONResponse(content={"success": True, **payload})


@router.get("/{list_id}")
async def api_get_public_list(list_id: str):
    """Fetch one public List document by List id."""
    doc = await get_public_list(list_id)
    if doc is None:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": f"Public list not found: {list_id}"},
        )
    return JSONResponse(content={"success": True, "list": doc})


@router.put("/{list_id}")
async def api_upsert_public_list(
    list_id: str,
    request: PublicListUpsertRequest,
    _: None = Depends(require_public_list_write_key),
):
    """
    Upsert one public List document (MediaCircle projection writes only).

    The path ``list_id`` must match the payload ``list_id``.
    """
    if request.list_id != list_id:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "error": f"Path list_id '{list_id}' does not match payload "
                f"list_id '{request.list_id}'",
            },
        )
    try:
        doc = await upsert_public_list(request)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"public-lists upsert failed for {list_id}: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(exc)})
    return JSONResponse(content={"success": True, "list": doc})


@router.delete("/{list_id}")
async def api_delete_public_list(
    list_id: str,
    _: None = Depends(require_public_list_write_key),
):
    """Delete one public List document (MediaCircle projection deletes only)."""
    try:
        removed = await delete_public_list(list_id)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"public-lists delete failed for {list_id}: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(exc)})
    return JSONResponse(content={"success": True, "deleted": removed, "list_id": list_id})


@router.post("/index")
async def api_create_public_list_index(
    _: None = Depends(require_public_list_write_key),
):
    """Create ``idx:public_lists`` if it does not exist."""
    try:
        created = await ensure_public_list_index()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"public-lists index create failed: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(exc)})
    return JSONResponse(
        content={
            "success": True,
            "created": created,
            "message": (
                f"Index '{PUBLIC_LIST_INDEX}' created"
                if created
                else f"Index '{PUBLIC_LIST_INDEX}' already exists"
            ),
        }
    )
