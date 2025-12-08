import json
from enum import Enum
from hashlib import md5
from typing import Any

from pydantic import Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods

"""
These are the known types and are a contract for expressing types of media with the frontend.
"""


class MCSources(str, Enum):
    TMDB = "tmdb"
    LASTFM = "lastfm"
    PODCASTINDEX = "podcastindex"
    OPENLIBRARY = "openlibrary"
    GOOGLE_BOOKS = "google_books"
    NEWSAPI = "newsapi"
    NEWSAI = "newsai"
    YOUTUBE = "youtube"
    NYTIMES = "nytimes"
    SPOTIFY = "spotify"
    FLIXPATROL = "flixpatrol"
    COMSCORE = "comscore"
    WATCHMODE = "watchmode"


class MCType(str, Enum):
    """
    MediaCircle content type enum.
    Defines all possible content types that can be returned by the backend.
    """

    # Primary content types (returned to frontend)
    MOVIE = "movie"
    TV_SERIES = "tv"
    PODCAST = "podcast"
    PODCAST_EPISODE = "podcast_episode"
    BOOK = "book"
    PERSON = "person"
    NEWS_ARTICLE = "news_article"
    VIDEO = "youtube_video"
    MUSIC = "music"
    MUSIC_ALBUM = "music_album"
    MUSIC_ALBUM_METADATA = "music_album_metadata"
    MUSIC_PLAYLIST = "music_playlist"
    MUSIC_TRACK = "music_track"
    BOOK_COVER = "book_cover"

    # Metadata and utility types
    MIXED = "mixed"
    CREDITS = "credits"
    KEYWORD = "keyword"
    PROVIDER = "provider"
    PROVIDERS_LIST = "providers_list"
    SEARCH = "search"
    SEARCH_RESULT = "search_result"
    GENRE = "genre"

    # Internal-only types (used for data aggregation, never returned to frontend)
    FLIXPATROL = "flixpatrol"  # Internal: FlixPatrol trending data aggregation
    COMSCORE = "comscore"  # Internal: Comscore box office data aggregation


class MCSubType(str, Enum):
    """
    MediaCircle person type enum.
    Defines all possible person types that can be returned by the backend.
    """

    ACTOR = "actor"
    MUSICIAN = "musician"
    POLITICIAN = "politician"
    ATHLETE = "athlete"
    AUTHOR = "author"
    PODCASTER = "podcaster"
    ARTIST = "artist"
    MUSIC_ARTIST = "music_artist"
    PERSON = "person"
    WRITER = "writer"
    DIRECTOR = "director"
    PRODUCER = "producer"
    YOUTUBE_CREATOR = "youtube_creator"
    CHARACTER = "character"


VALID_MC_TYPES = [
    MCType.MOVIE,
    MCType.TV_SERIES,
    MCType.PODCAST,
    MCType.PODCAST_EPISODE,
    MCType.BOOK,
    MCType.PERSON,
    MCType.NEWS_ARTICLE,
    MCType.VIDEO,
    MCType.MUSIC_ALBUM,
    MCType.MUSIC_PLAYLIST,
    MCType.MUSIC_TRACK,
    MCType.PROVIDER,
    MCType.CREDITS,
    MCType.KEYWORD,
]


class MCUrlType(str, Enum):
    """Model for a MediaCircle link type."""

    URL = "url"  # full url
    PATH = "path"  # partial path that must be used to derive url
    DEEP_LINK = "deep_link"  # deep link to the item


class MCImage(BaseModelWithMethods):
    """Model for a MediaCircle image."""

    url: str = ""
    key: str = ""
    type: MCUrlType | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Override to_dict to create a dictionary keyed by the 'key' field."""
        data = self.model_dump(exclude={"key"})
        return {self.key: data}


class MCLink(BaseModelWithMethods):
    """Model for a MediaCircle image."""

    url: str = ""
    key: str = ""
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Override to_dict to create a dictionary keyed by the 'key' field."""
        data = self.model_dump(exclude={"key"})
        return {self.key: data}


class MCBaseItem(BaseModelWithMethods):
    """Model for MediaCircle base item data."""

    mc_type: MCType

    # MediaCircle standardized fields
    mc_id: str = Field(
        default=""
    )  # Guaranteed to be unique, always set (either provided or generated via hash)
    mc_subtype: MCSubType | None = None
    source_id: str | None = None
    source: MCSources | None = None

    links: list[MCLink] = Field(default_factory=list)
    images: list[MCImage] = Field(default_factory=list)
    metrics: dict[str, Any] = {}
    external_ids: dict[str, Any] = {}

    error: str | None = None  # If there was an error instantiating the item
    status_code: int = 200

    # Search/sorting metadata - normalized score for cross-type comparison
    sort_order: float = 0.0

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "MCBaseItem":
        """Auto-generate mc_id if not provided. Always ensures mc_id is set."""
        if not self.mc_id:
            # Handle different item types - TMDB items use tmdb_id, others use id
            item_dict = {}
            if hasattr(self, "tmdb_id") and self.tmdb_id is not None:
                item_dict["tmdb_id"] = self.tmdb_id
                item_dict["id"] = self.tmdb_id  # Also include id for generate_mc_id compatibility
            elif hasattr(self, "id") and self.id is not None:
                item_dict["id"] = self.id

            # Include mc_subtype if present (needed for music artist detection)
            if hasattr(self, "mc_subtype") and self.mc_subtype is not None:
                item_dict["mc_subtype"] = self.mc_subtype

            # Generate mc_id from item data (always returns a value, uses hash fallback if needed)
            self.mc_id = generate_mc_id(item_dict, self.mc_type)

        # Ensure mc_id is never empty after validation
        if not self.mc_id:
            # Last resort: generate hash from model data
            model_dict = self.model_dump(exclude={"mc_id"})
            self.mc_id = generate_mc_id(model_dict, self.mc_type)

        return self


class MCSearchResponse(BaseModelWithMethods):
    """Base class for All API search responses."""

    results: list[MCBaseItem] = Field(default_factory=list)
    total_results: int = 0
    query: str | None = None
    data_source: str | None = None
    data_type: MCType | None = None
    page: int = 1
    total_pages: int = 1
    error: str | None = None
    status_code: int = 200
    metrics: dict[str, Any] = Field(default_factory=dict)


ApiWrapperResponse = MCSearchResponse


def generate_mc_id(item: dict[str, Any], mc_type: MCType) -> str:
    """
    Generate a deterministic unique identifier for any content item.

    Args:
        item: The item data dictionary
        mc_type: The MediaCircle content type

    Returns:
        str: The unique identifier (always returns a value, uses hash fallback if needed)

    Rules by type:
        - movie: "tmdb_movie_{tmdb_id}"
        - tv: "tmdb_tv_{tmdb_id}"
        - podcast: "podcast_{id}"
        - podcast_episode: "episode_{id}"
        - book: "book_{openlibrary_key|isbn13|isbn10}"
        - person: "person_{tmdb_id}"
        - news_article: "news_{url_hash or id}"
        - youtube_video: "youtube_{video_id}"
        - music_album: "album_{mbid or artist_album_hash}"
        - fallback: hash-based ID generated from item data and mc_type
    """
    if mc_type == MCType.MOVIE:
        tmdb_id = item.get("tmdb_id") or item.get("id")
        if tmdb_id:
            return f"tmdb_movie_{tmdb_id}"

    elif mc_type == MCType.TV_SERIES:
        tmdb_id = item.get("tmdb_id") or item.get("id")
        if tmdb_id:
            return f"tmdb_tv_{tmdb_id}"

    elif mc_type == MCType.PODCAST:
        podcast_id = item.get("id")
        if podcast_id:
            return f"podcast_{podcast_id}"

    elif mc_type == MCType.PODCAST_EPISODE:
        episode_id = item.get("id")
        if episode_id:
            return f"episode_{episode_id}"

    elif mc_type == MCType.BOOK:
        # Priority: openlibrary_key > isbn13 > isbn10
        openlibrary_key = item.get("openlibrary_key")
        if openlibrary_key:
            # Clean the key (remove /works/ or /books/ prefix if present)
            clean_key = openlibrary_key.replace("/works/", "").replace("/books/", "")
            return f"book_{clean_key}"

        isbn13 = item.get("primary_isbn13") or item.get("isbn13") or item.get("google_isbn13")
        if isbn13:
            return f"book_{isbn13}"

        isbn10 = item.get("primary_isbn10") or item.get("isbn10") or item.get("google_isbn10")
        if isbn10:
            return f"book_{isbn10}"

    elif mc_type == MCType.PERSON:
        # Check if this is a music artist (subtype MUSIC_ARTIST)
        mc_subtype = item.get("mc_subtype")
        if mc_subtype == MCSubType.MUSIC_ARTIST.value or mc_subtype == MCSubType.MUSIC_ARTIST:
            # For Spotify artists, use Spotify ID
            spotify_id = item.get("id")
            if spotify_id:
                return f"spotify_artist_{spotify_id}"
        # For other person types, use person_id
        person_id = item.get("id")
        if person_id:
            return f"person_{person_id}"

    elif mc_type == MCType.NEWS_ARTICLE:
        # For news, use URL hash or article ID if available
        article_id = item.get("id") or item.get("article_id")
        if article_id:
            return f"news_{article_id}"
        url = item.get("url")
        if url:
            # Create a simple hash from URL
            return f"news_{hash(url) & 0x7FFFFFFF}"  # Positive 32-bit hash

    elif mc_type == MCType.VIDEO:
        video_id = item.get("video_id") or item.get("id")
        if video_id:
            return f"youtube_{video_id}"

    elif mc_type == MCType.MUSIC_ALBUM:
        # For music albums, use MusicBrainz ID if available
        mbid = item.get("mbid")
        if mbid:
            return f"album_{mbid}"

        # For Spotify albums, use Spotify ID if available
        spotify_id = item.get("id")
        if spotify_id:
            return f"album_{spotify_id}"

        # Otherwise create hash from artist + album title
        artist = item.get("artist")
        title = item.get("title")
        if artist and title:
            unique_str = f"{artist.lower()}_{title.lower()}"
            return f"album_{hash(unique_str) & 0x7FFFFFFF}"
    elif mc_type == MCType.MUSIC_PLAYLIST:
        # For Spotify playlists, use Spotify ID
        spotify_id = item.get("id")
        if spotify_id:
            return f"spotify_playlist_{spotify_id}"

    # Fallback: Generate hash-based ID from item data and mc_type
    # Create a deterministic hash from sorted item data and mc_type
    # Sort item keys for deterministic hashing
    sorted_item = json.dumps(item, sort_keys=True, default=str)
    hash_input = f"{mc_type.value}_{sorted_item}"
    hash_value = md5(hash_input.encode()).hexdigest()
    return f"{mc_type.value}_hash_{hash_value}"


class MCPersonSearchRequest(BaseModelWithMethods):
    """Model for a MediaCircle person search request."""

    source_id: str
    source: MCSources
    mc_type: MCType
    mc_id: str
    mc_subtype: MCSubType | None = None
    name: str

    @model_validator(mode="after")
    def convert_string_enums(self) -> "MCPersonSearchRequest":
        """Convert string inputs to enum types if needed."""
        if isinstance(self.mc_type, str):
            self.mc_type = MCType(self.mc_type)
        if isinstance(self.mc_subtype, str):
            self.mc_subtype = MCSubType(self.mc_subtype)
        if isinstance(self.source, str):
            self.source = MCSources(self.source)
        return self


class MCPersonSearchResponse(BaseModelWithMethods):
    """Model for a MediaCircle person search request."""

    input: MCPersonSearchRequest

    details: MCBaseItem | None = None  # This is any details about the person
    works: list[MCBaseItem] = Field(default_factory=list)  # This is a list of works by the person
    related: list[MCBaseItem] = Field(
        default_factory=list
    )  # This is a list of works across media related to
    error: str | None = None
    status_code: int = 200


def convert_media_type_to_mctype(media_type: str) -> MCType | None:
    """Convert a media type string to a MCType enum."""
    media_type_map = {
        "movie": MCType.MOVIE,
        "tv": MCType.TV_SERIES,
        "podcast": MCType.PODCAST,
        "book": MCType.BOOK,
        "news": MCType.NEWS_ARTICLE,
        "video": MCType.VIDEO,
        "music": MCType.MUSIC,
        "music_album": MCType.MUSIC_ALBUM,
        "music_playlist": MCType.MUSIC_PLAYLIST,
        "music_track": MCType.MUSIC_TRACK,
    }
    return media_type_map.get(media_type)
