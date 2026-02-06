from __future__ import annotations

import asyncio
import re

import aiohttp

from api.youtube.models import DynamicYouTubeVideo, YouTubeCreator, YouTubeVideo
from contracts.models import MCImage, MCUrlType
from utils.get_logger import get_logger

logger = get_logger(__name__)
_YOUTUBE_KEY_CACHE: str | None = None

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}


# ----------------------------------------
# Utility: dynamically get YouTube API key
# ----------------------------------------
async def get_api_key() -> str:
    """
    Fetches the current INNERTUBE_API_KEY directly from YouTube's homepage or embedded player script.
    Caches it in memory to avoid repeated lookups.
    """
    global _YOUTUBE_KEY_CACHE
    if _YOUTUBE_KEY_CACHE:
        return _YOUTUBE_KEY_CACHE

    url = "https://www.youtube.com"
    async with aiohttp.ClientSession() as session, session.get(url, headers=HEADERS) as resp:
        html = await resp.text()

    # Try to find the key using a regex YouTube uses internally
    match = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
    if not match:
        # fallback: check alternate player embed script
        async with aiohttp.ClientSession() as session:
            async with session.get("https://www.youtube.com/iframe_api", headers=HEADERS) as resp:
                alt_html = await resp.text()
            match = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', alt_html)

    if not match:
        raise RuntimeError("Unable to extract INNERTUBE_API_KEY from YouTube")

    _YOUTUBE_KEY_CACHE = match.group(1)
    return _YOUTUBE_KEY_CACHE or ""


def images_from_thumbnails(thumbnails: list[dict]) -> list[MCImage]:
    images = []
    for thumb in thumbnails:
        url = thumb.get("url")
        width = thumb.get("width", 0)

        if not url:
            continue

        # Add https: prefix if missing (YouTube API sometimes returns URLs starting with //)
        if url.startswith("//"):
            url = f"https:{url}"

        # Determine key based on width
        # YouTube channel thumbnails are typically: 88x88, 176x176, 800x800
        if width <= 88:
            key = "small"
        elif width <= 176:
            key = "medium"
        elif width <= 800:
            key = "large"
        else:
            key = "original"

        images.append(
            MCImage(
                url=url,
                key=key,
                type=MCUrlType.URL,
                description="channel_avatar",
            )
        )

    return images


# ----------------------------------------
# Async helper to get video details
# ----------------------------------------
async def get_video_details(
    session: aiohttp.ClientSession, video_id: str, thumbnails: list[dict] | None = None
) -> dict:
    try:
        url = f"https://www.youtube.com/youtubei/v1/player?key={_YOUTUBE_KEY_CACHE}"

        payload = {
            "context": {
                "client": {
                    "hl": "en",
                    "gl": "US",
                    "clientName": "WEB",
                    "clientVersion": "2.20241107.00.00",
                }
            },
            "videoId": video_id,
        }

        async with session.post(url, json=payload, headers=HEADERS) as resp:
            data = await resp.json()

        vd = data.get("videoDetails", {})
        mf = data.get("microformat", {}).get("playerMicroformatRenderer", {})

        # Process thumbnails into MCImage objects
        images = []
        if thumbnails:
            # YouTube typically provides thumbnails in ascending size order
            # Common sizes: default (120x90), medium (320x180), high (480x360), standard (640x480), maxres (1280x720)
            for thumb in thumbnails:
                thumb_url: str | None = thumb.get("url")
                width = thumb.get("width", 0)

                if not thumb_url:
                    continue

                # Add https: prefix if missing (YouTube API sometimes returns URLs starting with //)
                if thumb_url.startswith("//"):
                    thumb_url = f"https:{thumb_url}"

                # Determine key based on width
                if width <= 120:
                    key = "small"
                elif width <= 320:
                    key = "medium"
                elif width <= 480:
                    key = "large"
                else:
                    key = "original"

                images.append(
                    MCImage(
                        url=thumb_url,
                        key=key,
                        type=MCUrlType.URL,
                        description="thumbnail",
                    )
                )

        return {
            "description": vd.get("shortDescription"),
            "duration_seconds": int(vd.get("lengthSeconds", 0))
            if vd.get("lengthSeconds")
            else None,
            "category": mf.get("category"),
            "tags": mf.get("tags", []),
            "publish_date": mf.get("publishDate"),
            "is_live": vd.get("isLiveContent", False),
            "images": images,
            "error": None,
            "status_code": 200,
        }
    except Exception as e:
        return {"error": str(e), "status_code": 500}


# ----------------------------------------
# Helper: parse YouTube count strings
# ----------------------------------------
def parse_youtube_count(count_str: str | None) -> int:
    """
    Parse YouTube count strings like '1.2M subscribers', '523 videos', etc.
    Returns 0 if parsing fails.

    Examples:
        '1.2M subscribers' -> 1200000
        '523 videos' -> 523
        '10K subscribers' -> 10000
        None -> 0
    """
    if not count_str:
        return 0

    # Remove common suffixes and clean the string
    count_str = count_str.lower().strip()
    for suffix in ["subscribers", "subscriber", "videos", "video"]:
        count_str = count_str.replace(suffix, "").strip()

    # Handle K, M, B multipliers
    multiplier = 1
    if "k" in count_str:
        multiplier = 1_000
        count_str = count_str.replace("k", "")
    elif "m" in count_str:
        multiplier = 1_000_000
        count_str = count_str.replace("m", "")
    elif "b" in count_str:
        multiplier = 1_000_000_000
        count_str = count_str.replace("b", "")

    try:
        # Parse the number and apply multiplier
        number = float(count_str.strip())
        return int(number * multiplier)
    except (ValueError, AttributeError):
        return 0


# ----------------------------------------
# Core function
# ----------------------------------------


# ----------------------------------------
# get_channel_videos
# ----------------------------------------
async def get_channel_videos(channel_id: str, limit: int = 10) -> list[YouTubeVideo]:
    """
    Retrieve a list of videos from a YouTube channel using its channel_id.
    Uses the internal youtubei/v1/browse endpoint (no Data API key).
    """
    await get_api_key()
    url = f"https://www.youtube.com/youtubei/v1/browse?key={_YOUTUBE_KEY_CACHE}"

    payload = {
        "context": {
            "client": {
                "hl": "en",
                "gl": "US",
                "clientName": "WEB",
                "clientVersion": "2.20241107.00.00",
            }
        },
        "browseId": channel_id,
        "params": "EgZ2aWRlb3M%3D",  # "videos" tab
    }

    async with aiohttp.ClientSession().post(url, headers=HEADERS, json=payload) as resp:
        data = await resp.json()

    # Navigate to videoRenderers
    try:
        tabs = data["contents"]["twoColumnBrowseResultsRenderer"]["tabs"]
        video_tab = next(
            t for t in tabs if "tabRenderer" in t and t["tabRenderer"].get("title") == "Videos"
        )
        grid_items = video_tab["tabRenderer"]["content"]["richGridRenderer"]["contents"]
    except Exception:
        grid_items = []

    videos: list[YouTubeVideo] = []
    for item in grid_items:
        vid = item.get("richItemRenderer", {}).get("content", {}).get("videoRenderer")
        if not vid:
            continue

        video_id = vid.get("videoId")
        title = vid.get("title", {}).get("runs", [{}])[0].get("text")
        thumbs = vid.get("thumbnail", {}).get("thumbnails", [])
        thumb_url = thumbs[-1]["url"] if thumbs else ""
        views = vid.get("viewCountText", {}).get("simpleText")
        published = vid.get("publishedTimeText", {}).get("simpleText")
        images = images_from_thumbnails(thumbs)

        # Extract short description (if present)
        desc_runs = (
            vid.get("detailedMetadataSnippets", [{}])[0].get("snippetText", {}).get("runs", [])
        )
        description = "".join([r.get("text", "") for r in desc_runs]) if desc_runs else None

        videos.append(
            YouTubeVideo(
                id=video_id or "",
                video_id=video_id or "",
                title=title or "",
                url=f"https://www.youtube.com/watch?v={video_id}",
                thumbnail_url=thumb_url,
                view_count=views,
                published_at=published or "",
                images=images,
                source_id=video_id or "",
                description=description or "",
            )
        )

        if len(videos) >= limit:
            break

    return videos


async def get_person_details(source_id: str) -> YouTubeCreator:
    """
    Fetch full details for a YouTube channel using its channel_id.
    Uses the internal youtubei/v1/browse endpoint (no API key registration needed).
    """
    try:
        api_key = await get_api_key()
        url = f"https://www.youtube.com/youtubei/v1/browse?key={api_key}"

        payload = {
            "context": {
                "client": {
                    "hl": "en",
                    "gl": "US",
                    "clientName": "WEB",
                    "clientVersion": "2.20241107.00.00",
                }
            },
            "browseId": source_id,  # channel_id
        }

        async with aiohttp.ClientSession().post(url, headers=HEADERS, json=payload) as resp:
            data = await resp.json()

        # Root shortcut
        meta = data.get("header", {}).get("c4TabbedHeaderRenderer", {})

        title = meta.get("title")
        description = data.get("metadata", {}).get("channelMetadataRenderer", {}).get("description")
        avatar = None
        thumbs = []
        if "avatar" in meta:
            thumbs = meta["avatar"].get("thumbnails", [])
            avatar = thumbs[-1]["url"] if thumbs else None

        images = images_from_thumbnails(thumbs)
        banner = None
        if "banner" in meta:
            thumbs = meta["banner"].get("thumbnails", [])
            banner = thumbs[-1]["url"] if thumbs else None

        subs = meta.get("subscriberCountText", {}).get("simpleText")
        vids = meta.get("videosCountText", {}).get("runs", [{}])[0].get("text")
        country = data.get("metadata", {}).get("channelMetadataRenderer", {}).get("country")
        joined = meta.get("joinedDateText", {}).get("runs", [{}])[0].get("text")

        return YouTubeCreator(
            id=source_id,
            title=title,
            images=images,
            description=description,
            url=f"https://www.youtube.com/channel/{source_id}",
            avatar=avatar,
            banner=banner,
            subscriber_count=subs,
            video_count=vids,
            country=country,
            joined_date=joined,
            source_id=source_id,
        )
    except Exception as e:
        logger.error(f"YOUTUBE Dynamic: Error in get_person_details: {str(e)}")
        return YouTubeCreator(
            id=source_id,
            title=source_id,
            url=f"https://www.youtube.com/channel/{source_id}",
            description=f"Error fetching details for {source_id}",
            error=str(e),
            status_code=500,
        )


# ----------------------------------------
# get_person (search creator)
# ----------------------------------------
async def get_person(query: str, limit: int = 1) -> list[YouTubeCreator]:
    """
    Searches YouTube for a channel/creator by name or keyword.
    Uses the internal youtubei/v1/search endpoint (no API key needed).

    Args:
        query: Name of the person or channel to search.
        limit: Number of creators to return (default: 1).

    Returns:
        List of YouTubeCreator models.
    """
    try:
        api_key = await get_api_key()
        url = f"https://www.youtube.com/youtubei/v1/search?key={api_key}"

        payload = {
            "context": {
                "client": {
                    "hl": "en",
                    "gl": "US",
                    "clientName": "WEB",
                    "clientVersion": "2.20241107.00.00",
                }
            },
            "query": query,
            "params": "EgIQAg%3D%3D",  # Filter: channels only
        }

        async with aiohttp.ClientSession().post(url, headers=HEADERS, json=payload) as resp:
            data = await resp.json()

        try:
            items = data["contents"]["twoColumnSearchResultsRenderer"]["primaryContents"][
                "sectionListRenderer"
            ]["contents"][0]["itemSectionRenderer"]["contents"]
        except KeyError:
            items = []

        creators: list[YouTubeCreator] = []
        for item in items:
            channel = item.get("channelRenderer")
            if not channel:
                continue

            channel_id = channel.get("channelId")
            title = channel.get("title", {}).get("simpleText", "")
            thumbs = channel.get("thumbnail", {}).get("thumbnails", [])
            thumb_url = thumbs[-1]["url"] if thumbs else None
            # Add https: prefix if missing
            if thumb_url and thumb_url.startswith("//"):
                thumb_url = f"https:{thumb_url}"
            subs_text = channel.get("subscriberCountText", {}).get("simpleText")
            vids_text = channel.get("videoCountText", {}).get("runs", [{}])[0].get("text")
            desc_runs = (
                channel.get("descriptionSnippet", {}).get("runs", [])
                if "descriptionSnippet" in channel
                else []
            )
            desc = "".join(r.get("text", "") for r in desc_runs)

            # Parse counts to integers
            subscriber_count = parse_youtube_count(subs_text)
            video_count = parse_youtube_count(vids_text)

            # Process thumbnails into MCImage objects
            images = images_from_thumbnails(thumbs)

            creators.append(
                YouTubeCreator(
                    id=channel_id,
                    title=title,
                    url=f"https://www.youtube.com/channel/{channel_id}",
                    thumbnail=thumb_url,
                    subscriber_count=subscriber_count,
                    video_count=video_count,
                    description=desc,
                    source_id=channel_id,
                    images=images,
                )
            )

            if len(creators) >= limit:
                break

        return creators
    except Exception as e:
        logger.error(f"YOUTUBE Dynamic: Error in get_person: {str(e)}")
        return []


# -------------------------------
# Core async search function
# -------------------------------


async def search_videos_async(
    query: str, limit: int = 10, enrich: bool = True
) -> list[YouTubeVideo]:
    """
    Search YouTube videos using the unofficial INNERTUBE API.

    Args:
        query: Search query string
        limit: Maximum number of results to return
        enrich: If True, fetch additional details for each video (slower but more data).
                If False, return basic info only (faster, good for autocomplete).
    """
    try:
        api_key = await get_api_key()
        search_url = f"https://www.youtube.com/youtubei/v1/search?key={api_key}"

        payload = {
            "context": {
                "client": {
                    "hl": "en",
                    "gl": "US",
                    "clientName": "WEB",
                    "clientVersion": "2.20241107.00.00",
                }
            },
            "query": query,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(search_url, headers=HEADERS, json=payload) as resp:
                data = await resp.json()

            # Navigate into the deeply nested YouTube JSON structure
            try:
                items = data["contents"]["twoColumnSearchResultsRenderer"]["primaryContents"][
                    "sectionListRenderer"
                ]["contents"][0]["itemSectionRenderer"]["contents"]
            except KeyError:
                items = []

            results: list[DynamicYouTubeVideo] = []
            thumbnails_map: dict[str, list[dict]] = {}  # Store thumbnails for each video_id

            for item in items:
                video = item.get("videoRenderer")
                if not video:
                    continue

                vid = video.get("videoId")
                title = (
                    video.get("title", {}).get("runs", [{}])[0].get("text")
                    if "title" in video
                    else "Unknown"
                )
                channel = video.get("ownerText", {}).get("runs", [{}])[0].get("text")
                published_time = video.get("publishedTimeText", {}).get("simpleText")
                view_count = video.get("viewCountText", {}).get("simpleText")
                thumbs = video.get("thumbnail", {}).get("thumbnails", [])
                thumb_url = thumbs[-1]["url"] if thumbs else None
                # Add https: prefix if missing
                if thumb_url and thumb_url.startswith("//"):
                    thumb_url = f"https:{thumb_url}"

                # Store thumbnails for enrichment
                thumbnails_map[vid] = thumbs

                # Process thumbnails to MCImage objects for basic results
                images = images_from_thumbnails(thumbs) if thumbs else []

                results.append(
                    DynamicYouTubeVideo(
                        video_id=vid,
                        title=title,
                        channel=channel,
                        published_time=published_time,
                        view_count=view_count,
                        thumbnail_url=thumb_url,
                        url=f"https://www.youtube.com/watch?v={vid}",
                        images=images,  # Include basic thumbnails
                    )
                )

                if len(results) >= limit:
                    break

            # ----------------------------------------
            # Enrich each video concurrently (optional)
            # ----------------------------------------
            if enrich and results:
                tasks = [
                    get_video_details(session, v.video_id, thumbnails_map.get(v.video_id))
                    for v in results
                ]
                details_list = await asyncio.gather(*tasks, return_exceptions=True)

                # Merge data back into results
                for v, d in zip(results, details_list, strict=False):
                    if isinstance(d, dict) and not d.get("error"):
                        for key, val in d.items():
                            setattr(v, key, val)

        # Convert DynamicYouTubeVideo objects to YouTubeVideo objects
        # Filter out any videos that have errors (check error attribute, not dict.get)
        youtube_videos = [
            YouTubeVideo.from_dynamic(v) for v in results if not (hasattr(v, "error") and v.error)
        ]
        return youtube_videos
    except Exception as e:
        logger.error(f"YOUTUBE Dynamic: Error in search_videos_async: {str(e)}")
        return []
