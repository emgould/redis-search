"""
Watchmode-related Firebase Functions

This module contains all Watchmode-related Firebase function handlers.
Handlers exclusively call wrapper methods - no auth or business logic here.
"""

import json
import logging
from typing import Any

from firebase_functions import https_fn

# Import the Watchmode wrapper instance
from api.watchmode.wrappers import watchmode_wrapper
from utils.async_runner import run_async

# Configure logging
logger = logging.getLogger(__name__)


def _safe_model_payload(value: object) -> object:
    """
    Safely serialize pydantic-heavy payloads for HTTP responses.

    In emulator hot-reload flows, some models can transiently carry mocked
    serializer internals, which may raise when calling model_dump().
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list | tuple):
        return [_safe_model_payload(item) for item in value]

    if isinstance(value, dict):
        return {str(key): _safe_model_payload(item) for key, item in value.items()}

    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()  # type: ignore[attr-defined]
        except Exception as exc:
            logger.warning(
                "model_dump failed, falling back to attribute serialization: %s",
                exc,
            )

    if hasattr(value, "__dict__"):
        return {
            key: _safe_model_payload(item)
            for key, item in vars(value).items()
            if not key.startswith("__pydantic_")
        }

    return str(value)


class WatchmodeHandler:
    """Class containing all Watchmode-related Firebase Functions."""

    def __init__(self):
        """Initialize Watchmode handler. All auth handled by wrapper."""
        logger.info("WatchmodeHandler initialized")

    def get_whats_new(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get what's new this week on streaming services using Watchmode API.

        This function demonstrates:
        - Secure secret management with Firebase Functions
        - External API integration with Watchmode
        - Async processing in Python functions

        Usage: GET /get_whats_new?region=US&limit=50
        """
        try:
            # Parse request parameters
            region = req.args.get("region", "US")
            limit = int(req.args.get("limit", 50))

            # Validate limit
            if limit < 1 or limit > 250:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 250"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Create and run the async function
            async def run_get_whats_new():
                return await watchmode_wrapper.get_whats_new(region=region, limit=limit)

            # Run the async function safely (handles event loop issues in deployed environments)
            response = run_async(run_get_whats_new())

            # Check for errors
            status_code = response.status_code
            if status_code != 200 or response.error:
                return https_fn.Response(
                    json.dumps(_safe_model_payload(response), default=str),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Defensive check: ensure response has results attribute
            if not hasattr(response, "results"):
                logger.error(
                    f"Response object missing 'results' attribute. "
                    f"Available attributes: {dir(response)}, "
                    f"Type: {type(response)}, "
                    f"Model dump: {_safe_model_payload(response)}"
                )
                return https_fn.Response(
                    json.dumps({"error": "Invalid response structure from wrapper"}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

            releases_count = len(response.results)
            logger.info(
                f"Successfully fetched what's new for region: {region} - {response.total_results} total, {releases_count} releases in array"
            )

            # Log sample release if available
            if releases_count > 0:
                sample_release = response.results[0]
                logger.info(
                    f"Sample release: tmdb_id={getattr(sample_release, 'tmdb_id', None)}, "
                    f"name={getattr(sample_release, 'name', None) or getattr(sample_release, 'title', None)}, "
                    f"content_type={getattr(sample_release, 'content_type', None)}, "
                    f"has_poster={bool(getattr(sample_release, 'poster_path', None))}"
                )
            else:
                logger.warning(
                    f"No releases in response array! Total results: {response.total_results}"
                )

            return https_fn.Response(
                json.dumps(_safe_model_payload(response), default=str),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as ve:
            logger.error(f"Validation error in get_whats_new: {str(ve)}")
            return https_fn.Response(
                json.dumps({"error": str(ve)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Error in get_whats_new function: {str(e)}", exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    def get_watchmode_title_details(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get detailed information for a specific title from Watchmode.

        Usage: GET /get_watchmode_title_details?watchmode_id=12345
        """
        try:
            # Parse request parameters
            watchmode_id = req.args.get("watchmode_id")

            if not watchmode_id:
                return https_fn.Response(
                    json.dumps({"error": "watchmode_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                watchmode_id_int = int(watchmode_id)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "watchmode_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Create and run the async function
            async def run_get_details():
                return await watchmode_wrapper.get_watchmode_title_details(
                    watchmode_id=watchmode_id_int
                )

            # Run the async function safely (handles event loop issues in deployed environments)
            result = run_async(run_get_details())

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched details for Watchmode ID: {watchmode_id_int}")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as ve:
            logger.error(f"Validation error in get_watchmode_title_details: {str(ve)}")
            return https_fn.Response(
                json.dumps({"error": str(ve)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Error in get_watchmode_title_details function: {str(e)}", exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    def search_titles(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for titles by name using Watchmode API.

        Returns watchmode IDs that can be used with get_watchmode_title_details.

        Usage: GET /search_watchmode_titles?query=inception&types=movie,tv
        """
        try:
            # Parse request parameters
            query = req.args.get("query")
            types = req.args.get("types", "movie,tv")

            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate types parameter
            valid_types = ["movie", "tv", "person"]
            type_list = [t.strip() for t in types.split(",")]
            if not all(t in valid_types for t in type_list):
                return https_fn.Response(
                    json.dumps(
                        {
                            "error": f"Invalid types parameter. Valid values: {', '.join(valid_types)}"
                        }
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Create and run the async function
            async def run_search():
                return await watchmode_wrapper.search_titles(query=query, types=types)

            # Run the async function safely (handles event loop issues in deployed environments)
            result = run_async(run_search())

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched for '{query}' - found {result.total_results} results"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as ve:
            logger.error(f"Validation error in search_titles: {str(ve)}")
            return https_fn.Response(
                json.dumps({"error": str(ve)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Error in search_titles function: {str(e)}", exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    def get_sports(self, req: https_fn.Request) -> https_fn.Response:
        """GET sports frontend API. Bind using existing auth/App Check deployment pattern.

        action=events|event|sources|leagues|teams|coverage. No cache bypass,
        refresh or upstream continuation controls are exposed to callers.
        """
        if req.method != "GET":
            return https_fn.Response(json.dumps({"error": "GET required"}), status=405,
                headers={"Content-Type": "application/json", "Allow": "GET"})
        try:
            allowed = {"action", "region", "date", "timezone", "mode", "league_ids", "team_ids",
                       "followed_league_ids", "followed_team_ids", "search", "page",
                       "limit", "version", "event_id", "league_id", "team_league_hints"}
            if set(req.args) - allowed:
                raise ValueError("Unknown sports query parameter")
            params: dict[str, Any] = dict(req.args)
            if "timezone" in params:
                params["timezone_name"] = params.pop("timezone")
            for key in ("page", "limit", "event_id", "league_id"):
                if key in params:
                    params[key] = int(params[key])
            result = run_async(watchmode_wrapper.get_sports(**params))
            return https_fn.Response(json.dumps(_safe_model_payload(result), default=str),
                status=result.status_code, headers={"Content-Type": "application/json", "Cache-Control": "private, no-store"})
        except ValueError as exc:
            return https_fn.Response(json.dumps({"error": str(exc), "code": "INVALID_QUERY"}),
                status=400, headers={"Content-Type": "application/json"})
        except Exception:
            logger.exception("Watchmode sports handler failed")
            return https_fn.Response(json.dumps({"error": "Sports temporarily unavailable"}),
                status=503, headers={"Content-Type": "application/json"})


# Create a global instance of the handler
watchmode_handler = WatchmodeHandler()
