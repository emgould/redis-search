"""
TVDB Wrappers - Async wrapper functions for compatibility with existing patterns.
Provides backward-compatible interface for existing code.
"""

from api.subapi._tvdb.core import TVDBService


async def search_async(api_key: str, query: str, **kwargs) -> tuple[dict, int | None]:
    """
    Async wrapper for searching TVDB shows.

    Args:
        api_key: TVDB API key
        query: Search query string
        **kwargs: Additional arguments passed to search

    Returns:
        Tuple of (search results dict, error code or None)
    """
    if not query:
        return {"error": "Search query is required"}, 400

    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)
        limit = kwargs.get("limit", 10)
        results = service.search(query, limit=limit)
        return {"shows": results, "total_count": len(results), "query": query}, None

    except Exception as e:
        return {"error": str(e)}, 500


async def search_tvdb_images_async(
    api_key: str,
    query: str,
    limit: int = 10,
    image_types: list[str] | None = None,
    lang: str = "eng",
    tvdb_id: int | None = None,
    tmdb_token: str | None = None,
    **kwargs,
) -> tuple[dict, int | None]:
    """
    Async wrapper for searching TVDB shows and getting their images.

    Args:
        api_key: TVDB API key
        query: Search query string
        limit: Maximum number of results
        image_types: List of image types to fetch
        lang: Language preference
        tvdb_id: Specific TVDB ID (bypasses search)
        tmdb_token: TMDB API token for cast data
        **kwargs: Additional arguments

    Returns:
        Tuple of (results dict, error code or None)
    """
    if not query and not tvdb_id:
        return {"error": "Search query or TVDB ID is required"}, 400

    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)

        if tvdb_id:
            # Get images for specific show
            show_results = service.get_show_images(
                query="", tvdb_id=tvdb_id, image_types=image_types, lang=lang
            )
            return {"show": show_results, "query": query, "tvdb_id": tvdb_id}, None
        else:
            # Search and get images for all results
            # Note: search_with_images not implemented in core yet, using basic search
            search_results = service.search(query, limit=limit)
            return {
                "shows": search_results,
                "total_count": len(search_results),
                "query": query,
            }, None

    except Exception as e:
        return {"error": str(e)}, 500


async def get_show_details_extended_async(
    api_key: str,
    tvdb_id: int,
    extended: bool = True,
    tmdb_token: str | None = None,
    **kwargs,
) -> tuple[dict, int | None]:
    """
    Async wrapper for getting detailed show information.

    Args:
        api_key: TVDB API key
        tvdb_id: TVDB ID of the show
        extended: Whether to fetch extended information
        tmdb_token: TMDB API token for cast data
        **kwargs: Additional arguments

    Returns:
        Tuple of (show details dict, error code or None)
    """
    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)
        result = service.get_show_details(tvdb_id, extended=extended)

        if result:
            return {"show": result, "tvdb_id": tvdb_id}, None
        else:
            return {"error": f"Show with ID {tvdb_id} not found"}, 404

    except Exception as e:
        return {"error": str(e)}, 500


async def get_show_complete_data_async(
    api_key: str, tvdb_id: int, lang: str = "eng", tmdb_token: str | None = None, **kwargs
) -> tuple[dict, int | None]:
    """
    Async wrapper for getting complete show information.

    Args:
        api_key: TVDB API key
        tvdb_id: TVDB ID of the show
        lang: Language preference
        tmdb_token: TMDB API token for enhanced data
        **kwargs: Additional arguments

    Returns:
        Tuple of (complete show data dict, error code or None)
    """
    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)
        result = service.get_show_complete(tvdb_id, lang=lang)

        if result:
            return {"show": result, "tvdb_id": tvdb_id}, None
        else:
            return {"error": f"Show with ID {tvdb_id} not found"}, 404

    except Exception as e:
        return {"error": str(e)}, 500


async def search_by_external_id_async(
    api_key: str, external_id: str, source: str = "imdb", **kwargs
) -> tuple[dict, int | None]:
    """
    Async wrapper for searching by external ID.

    Args:
        api_key: TVDB API key
        external_id: External ID (e.g., IMDB ID)
        source: Source of the ID
        **kwargs: Additional arguments

    Returns:
        Tuple of (show info dict, error code or None)
    """
    if not external_id:
        return {"error": "External ID is required"}, 400

    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)
        result = service.search_by_external_id(external_id, source=source)

        if result:
            return {"show": result, "external_id": external_id, "source": source}, None
        else:
            return {"error": f"Show with external ID {external_id} not found"}, 404

    except Exception as e:
        return {"error": str(e)}, 500


async def get_all_images_async(
    api_key: str, tvdb_id: int, lang: str = "eng", **kwargs
) -> tuple[dict, int | None]:
    """
    Async wrapper for getting all images for a show.

    Args:
        api_key: TVDB API key
        tvdb_id: TVDB ID of the show
        lang: Language preference
        **kwargs: Additional arguments

    Returns:
        Tuple of (images dict, error code or None)
    """
    if not api_key:
        return {"error": "API key is required"}, 400

    try:
        service = TVDBService(api_key)
        result = service.get_all_images(tvdb_id, lang=lang)

        return {"images": result, "tvdb_id": tvdb_id, "total_types": len(result)}, None

    except Exception as e:
        return {"error": str(e)}, 500


async def search_tmdb_multi_async(
    query: str,
    tmdb_token: str,
    page: int = 1,
    limit: int = 20,
    tvdb_api_key: str | None = None,
    **kwargs,
) -> tuple[dict, int | None]:
    """
    Async wrapper for TMDB multi search.

    Args:
        query: Search query
        tmdb_token: TMDB API token
        page: Page number
        limit: Results per page
        tvdb_api_key: TVDB API key (optional)
        **kwargs: Additional arguments

    Returns:
        Tuple of (search results dict, error code or None)
    """
    if not query.strip():
        return {"error": "Search query is required"}, 400

    if not tmdb_token:
        return {"error": "TMDB API token is required"}, 400

    try:
        # Use dummy key if not provided
        tvdb_key_to_use = tvdb_api_key if tvdb_api_key else "dummy"
        service = TVDBService(tvdb_key_to_use)

        result = service.search_tmdb_multi(
            query=query, tmdb_token=tmdb_token, page=page, limit=limit
        )

        if result.get("error"):
            return result, 500

        return result, None

    except Exception as e:
        return {"error": str(e)}, 500
