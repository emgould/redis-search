"""
RSS Feed Parser - Async parser for podcast RSS feeds.
Extracts episode data from various RSS feed formats (RSS 2.0, Atom, iTunes extensions).
"""

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime

import aiohttp

from api.podcast.models import RSSEpisode, RSSFeedResult
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache configuration - 30 minutes for RSS feed data
# Feeds don't update that frequently, so we can cache aggressively
RSSFeedCache = RedisCache(
    defaultTTL=60 * 30,  # 30 minutes
    prefix="rss_feed",
    verbose=False,
    isClassMethod=False,
)

# Common XML namespaces used in podcast RSS feeds
NAMESPACES = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom": "http://www.w3.org/2005/Atom",
    "media": "http://search.yahoo.com/mrss/",
    "podcast": "https://podcastindex.org/namespace/1.0",
}


def _parse_duration(duration_str: str | None) -> int | None:
    """
    Parse duration string to seconds.
    Handles formats: HH:MM:SS, MM:SS, seconds, or text like "1 hour 30 minutes".

    Args:
        duration_str: Duration string from RSS feed

    Returns:
        Duration in seconds, or None if unparseable
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip()

    # Try pure integer (seconds)
    if duration_str.isdigit():
        return int(duration_str)

    # Try HH:MM:SS or MM:SS format
    time_match = re.match(r"^(\d+):(\d{2}):(\d{2})$", duration_str)
    if time_match:
        hours, minutes, seconds = map(int, time_match.groups())
        return hours * 3600 + minutes * 60 + seconds

    time_match = re.match(r"^(\d+):(\d{2})$", duration_str)
    if time_match:
        minutes, seconds = map(int, time_match.groups())
        return minutes * 60 + seconds

    # Try text format like "1 hour 30 minutes"
    total_seconds = 0
    hour_match = re.search(r"(\d+)\s*(?:hour|hr)", duration_str, re.IGNORECASE)
    if hour_match:
        total_seconds += int(hour_match.group(1)) * 3600

    min_match = re.search(r"(\d+)\s*(?:minute|min)", duration_str, re.IGNORECASE)
    if min_match:
        total_seconds += int(min_match.group(1)) * 60

    sec_match = re.search(r"(\d+)\s*(?:second|sec)", duration_str, re.IGNORECASE)
    if sec_match:
        total_seconds += int(sec_match.group(1))

    return total_seconds if total_seconds > 0 else None


def _parse_pub_date(date_str: str | None) -> str | None:
    """
    Parse publication date to ISO format.
    Handles RFC 2822 (standard RSS) and various other formats.

    Args:
        date_str: Date string from RSS feed

    Returns:
        ISO format date string, or None if unparseable
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try RFC 2822 format (standard for RSS)
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except (ValueError, TypeError):
        pass

    # Try ISO format directly
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except ValueError:
        pass

    # Try common date formats
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%d %b %Y",
        "%d %B %Y",
        "%B %d, %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue

    logger.debug(f"Could not parse date: {date_str}")
    return None


def _clean_html(text: str | None) -> str | None:
    """
    Clean HTML from description text.
    Preserves basic structure but removes tags.

    Args:
        text: Text that may contain HTML

    Returns:
        Cleaned text with HTML entities decoded
    """
    if not text:
        return None

    # Decode HTML entities
    text = html.unescape(text)

    # Remove CDATA wrappers
    text = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", text, flags=re.DOTALL)

    # Replace common block elements with newlines
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Clean up whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    return text if text else None


def _get_element_text(element: ET.Element | None, default: str | None = None) -> str | None:
    """Safely get text from an XML element."""
    if element is not None and element.text:
        return element.text.strip()
    return default


def _find_with_ns(element: ET.Element, tag: str, ns_key: str) -> ET.Element | None:
    """Find element with namespace prefix."""
    ns = NAMESPACES.get(ns_key, "")
    return element.find(f"{{{ns}}}{tag}") if ns else None


def _parse_item(item: ET.Element) -> RSSEpisode:
    """
    Parse a single RSS item/entry into an RSSEpisode.

    Args:
        item: XML element representing an RSS item

    Returns:
        RSSEpisode with parsed data
    """
    # Title - try standard RSS, then Atom
    title = _get_element_text(item.find("title"))
    if not title:
        title = "Untitled Episode"

    # Description - try multiple sources
    description = None
    # Try content:encoded first (usually has full content)
    content_encoded = _find_with_ns(item, "encoded", "content")
    if content_encoded is not None:
        description = _get_element_text(content_encoded)

    # Fall back to description
    if not description:
        description = _get_element_text(item.find("description"))

    # Fall back to itunes:summary
    if not description:
        itunes_summary = _find_with_ns(item, "summary", "itunes")
        description = _get_element_text(itunes_summary)

    description = _clean_html(description)

    # Audio URL from enclosure
    audio_url = None
    enclosure = item.find("enclosure")
    if enclosure is not None:
        audio_url = enclosure.get("url")
        # Validate it's an audio file
        enc_type = enclosure.get("type", "")
        if audio_url and not enc_type.startswith("audio/"):
            # Still use it, but log for debugging
            logger.debug(f"Enclosure type is {enc_type}, not audio/*")

    # Publication date
    pub_date = _parse_pub_date(_get_element_text(item.find("pubDate")))

    # Duration - try itunes:duration first
    duration_seconds = None
    itunes_duration = _find_with_ns(item, "duration", "itunes")
    if itunes_duration is not None:
        duration_seconds = _parse_duration(_get_element_text(itunes_duration))

    # GUID
    guid = _get_element_text(item.find("guid"))

    # Link
    link = _get_element_text(item.find("link"))

    # Episode image
    image = None
    itunes_image = _find_with_ns(item, "image", "itunes")
    if itunes_image is not None:
        image = itunes_image.get("href")
    # Try media:thumbnail
    if not image:
        media_thumb = _find_with_ns(item, "thumbnail", "media")
        if media_thumb is not None:
            image = media_thumb.get("url")

    return RSSEpisode(
        title=title,
        description=description,
        audio_url=audio_url,
        pub_date=pub_date,
        duration_seconds=duration_seconds,
        guid=guid,
        link=link,
        image=image,
    )


def _parse_rss_xml(xml_content: str) -> RSSFeedResult:
    """
    Parse RSS XML content into structured data.

    Args:
        xml_content: Raw XML string

    Returns:
        RSSFeedResult with parsed episodes
    """
    try:
        # Parse XML
        root = ET.fromstring(xml_content)

        # Determine feed type and find channel/feed element
        channel = None
        items: list[ET.Element] = []

        # RSS 2.0 format
        if root.tag == "rss":
            channel = root.find("channel")
            if channel is not None:
                items = channel.findall("item")
        # Atom format
        elif root.tag == f"{{{NAMESPACES['atom']}}}feed" or root.tag == "feed":
            channel = root
            items = root.findall(f"{{{NAMESPACES['atom']}}}entry")
            if not items:
                items = root.findall("entry")
        # RDF format (RSS 1.0)
        elif "rdf" in root.tag.lower():
            channel = root.find("{http://purl.org/rss/1.0/}channel")
            items = root.findall("{http://purl.org/rss/1.0/}item")
        else:
            # Try to find channel anyway
            channel = root.find("channel")
            if channel is not None:
                items = channel.findall("item")

        if not items:
            return RSSFeedResult(
                error="No episodes found in feed",
                status_code=200,
                total_episodes=0,
            )

        # Parse feed metadata
        feed_title = None
        feed_description = None
        if channel is not None:
            feed_title = _get_element_text(channel.find("title"))
            feed_description = _clean_html(_get_element_text(channel.find("description")))

        # Parse episodes
        episodes: list[RSSEpisode] = []
        for item in items:
            try:
                episode = _parse_item(item)
                episodes.append(episode)
            except Exception as e:
                logger.warning(f"Failed to parse episode: {e}")
                continue

        return RSSFeedResult(
            episodes=episodes,
            total_episodes=len(episodes),
            feed_title=feed_title,
            feed_description=feed_description,
            status_code=200,
        )

    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return RSSFeedResult(
            error=f"Invalid XML: {e}",
            status_code=400,
        )
    except Exception as e:
        logger.error(f"Error parsing RSS feed: {e}")
        return RSSFeedResult(
            error=f"Parse error: {e}",
            status_code=500,
        )


@RSSFeedCache.use_cache(RSSFeedCache, prefix="parse_feed")
async def parse_rss_feed(
    feed_url: str,
    max_episodes: int = 25,
    timeout_seconds: int = 15,
) -> RSSFeedResult:
    """
    Fetch and parse an RSS feed URL.

    Args:
        feed_url: URL of the RSS feed
        max_episodes: Maximum number of episodes to return (default 25)
        timeout_seconds: HTTP request timeout (default 15s)

    Returns:
        RSSFeedResult with parsed episodes or error information
    """
    if not feed_url:
        return RSSFeedResult(
            error="No feed URL provided",
            status_code=400,
        )

    logger.info(f"Fetching RSS feed: {feed_url}")

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        headers = {
            "User-Agent": "MediaCircle/1.0 (podcast aggregator)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }

        async with (
            aiohttp.ClientSession() as session,
            session.get(feed_url, headers=headers, timeout=timeout) as response,
        ):
                if response.status != 200:
                    logger.warning(f"RSS feed returned status {response.status}: {feed_url}")
                    return RSSFeedResult(
                        error=f"Feed returned HTTP {response.status}",
                        status_code=response.status,
                    )

                # Read content
                content = await response.text()

                if not content or len(content) < 50:
                    return RSSFeedResult(
                        error="Empty or invalid feed response",
                        status_code=400,
                    )

        # Parse the XML content
        result = _parse_rss_xml(content)

        # Limit episodes
        if result.episodes and len(result.episodes) > max_episodes:
            result.episodes = result.episodes[:max_episodes]

        logger.info(f"Parsed {len(result.episodes)} episodes from feed")
        return result

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error fetching RSS feed: {e}")
        return RSSFeedResult(
            error=f"Failed to fetch feed: {e}",
            status_code=502,
        )
    except TimeoutError:
        logger.error(f"Timeout fetching RSS feed: {feed_url}")
        return RSSFeedResult(
            error="Feed request timed out",
            status_code=504,
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching RSS feed: {e}")
        return RSSFeedResult(
            error=f"Unexpected error: {e}",
            status_code=500,
        )
