"""
Build the Redis Search indexes for the media search application.

Four indexes:
1. idx:media - Movies and TV shows
   Schema matches the SearchDocument dataclass from src/core/normalize.py:
   - Indexed fields: search_title, mc_type, mc_subtype, year, popularity, rating, source
   - Display fields (stored, not indexed): image, cast, overview

2. idx:people - Person/actor data
   - Indexed fields: search_title (name), also_known_as, mc_type, mc_subtype, popularity, source
   - Display fields (stored, not indexed): image, overview (biography)

3. idx:podcasts - Podcast data for autocomplete
   - Indexed fields: search_title, author, mc_type, source, language, categories, popularity, episode_count
   - Display fields (stored, not indexed): image, description, url, site, etc.

4. idx:book - OpenLibrary books
   - TEXT fields: search_title, title, author, description, subjects_search
   - TAG fields: author_normalized, subjects (normalized), mc_type, source
   - Numeric fields: popularity_score, edition_count, first_publish_year
"""

import asyncio
import os

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Load env file (defaults to local.env for local development)
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

# Index name constants
INDEX_NAME = "idx:media"
PEOPLE_INDEX_NAME = "idx:people"
PODCASTS_INDEX_NAME = "idx:podcasts"
BOOKS_INDEX_NAME = "idx:book"


async def build_index():
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    schema = (
        # Primary search field with high weight
        TextField("$.search_title", as_name="search_title", weight=5.0),
        # Content type filters (MCType and MCSubType)
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.mc_subtype", as_name="mc_subtype"),
        # Source filter
        TagField("$.source", as_name="source"),
        # Genre filtering (arrays) - normalized (lowercase, underscores)
        TagField("$.genre_ids[*]", as_name="genre_ids"),
        TagField("$.genres[*]", as_name="genres"),
        # Cast filtering (arrays) - cast_names normalized
        TagField("$.cast_ids[*]", as_name="cast_ids"),
        TagField("$.cast_names[*]", as_name="cast_names"),
        # Director object (JSONPath into nested dict)
        TagField("$.director.id", as_name="director_id"),
        TagField("$.director.name_normalized", as_name="director_name"),
        # Keywords (IPTC expanded, normalized)
        TagField("$.keywords[*]", as_name="keywords"),
        # Origin country (normalized ISO codes)
        TagField("$.origin_country[*]", as_name="origin_country"),
        # watch_providers indexed subfields
        TagField("$.watch_providers.watch_region", as_name="watch_region"),
        TagField("$.watch_providers.primary_provider_type", as_name="primary_provider_type"),
        TagField("$.watch_providers.streaming_platform_ids[*]", as_name="streaming_platform_ids"),
        TagField("$.watch_providers.on_demand_platform_ids[*]", as_name="on_demand_platform_ids"),
        NumericField("$.watch_providers.primary_provider_id", as_name="primary_provider_id", sortable=True),
        # Sortable numeric fields for ranking
        NumericField("$.popularity", as_name="popularity", sortable=True),
        NumericField("$.rating", as_name="rating", sortable=True),
        NumericField("$.year", as_name="year", sortable=True),
        # Document lifecycle timestamps
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField("$.modified_at", as_name="modified_at", sortable=True),
    )

    definition = IndexDefinition(prefix=["media:"], index_type=IndexType.JSON)

    try:
        await r.ft(INDEX_NAME).create_index(schema, definition=definition)
        print(f"Index '{INDEX_NAME}' created successfully")
        print("Indexed fields: search_title, mc_type, mc_subtype, source, popularity, rating, year")
        print("Display fields (stored, not indexed): image, cast, overview")
    except Exception as e:
        if "Index already exists" in str(e):
            print(f"Index '{INDEX_NAME}' already exists")
        else:
            print("Index creation failed:", e)
    finally:
        await r.aclose()


async def drop_index():
    """Drop the existing index (useful for rebuilding)."""
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await r.ft(INDEX_NAME).dropindex(delete_documents=False)
        print(f"Index '{INDEX_NAME}' dropped successfully")
    except Exception as e:
        print(f"Could not drop index: {e}")
    finally:
        await r.aclose()


async def build_people_index():
    """Build the people search index for person/actor data."""
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    schema = (
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
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField("$.modified_at", as_name="modified_at", sortable=True),
    )

    definition = IndexDefinition(prefix=["person:"], index_type=IndexType.JSON)

    try:
        await r.ft(PEOPLE_INDEX_NAME).create_index(schema, definition=definition)
        print(f"Index '{PEOPLE_INDEX_NAME}' created successfully")
        print("Indexed fields: search_title (name), also_known_as, mc_type, mc_subtype, source, popularity")
        print("Display fields (stored, not indexed): image, overview")
    except Exception as e:
        if "Index already exists" in str(e):
            print(f"Index '{PEOPLE_INDEX_NAME}' already exists")
        else:
            print("Index creation failed:", e)
    finally:
        await r.aclose()


async def drop_people_index():
    """Drop the existing people index (useful for rebuilding)."""
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await r.ft(PEOPLE_INDEX_NAME).dropindex(delete_documents=False)
        print(f"Index '{PEOPLE_INDEX_NAME}' dropped successfully")
    except Exception as e:
        print(f"Could not drop people index: {e}")
    finally:
        await r.aclose()


async def build_podcasts_index():
    """Build the podcasts search index for podcast autocomplete.

    Uses the normalization paradigm with SearchDocument format:
    - Indexed fields follow SearchDocument structure from document_to_redis()
    - Display fields include podcast-specific metadata
    """
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    schema = (
        # Primary search field (from SearchDocument.search_title) with high weight
        TextField("$.search_title", as_name="search_title", weight=5.0),
        # Author/creator name - searchable (full-text)
        TextField("$.author", as_name="author", weight=3.0),
        # Author normalized for exact TAG matching
        TagField("$.author_normalized", as_name="author_normalized"),
        # SearchDocument standard fields
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        TagField("$.id", as_name="id"),  # mc_id from SearchDocument.id
        # Language filter (normalized)
        TagField("$.language", as_name="language"),
        # Categories array (normalized, IPTC-expanded)
        TagField("$.categories[*]", as_name="categories"),
        # Sortable numeric fields for ranking (from SearchDocument)
        NumericField("$.popularity", as_name="popularity", sortable=True),
        # Episode count (podcast display field, useful for sorting)
        NumericField("$.episode_count", as_name="episode_count", sortable=True),
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField("$.modified_at", as_name="modified_at", sortable=True),
    )

    definition = IndexDefinition(prefix=["podcast:"], index_type=IndexType.JSON)

    try:
        await r.ft(PODCASTS_INDEX_NAME).create_index(schema, definition=definition)
        print(f"Index '{PODCASTS_INDEX_NAME}' created successfully")
        print("Indexed fields: search_title, author, id (mc_id), mc_type, source, language, category1, category2, popularity, episode_count")
        print("Display fields (stored, not indexed): image, overview, title, url, site, etc.")
    except Exception as e:
        if "Index already exists" in str(e):
            print(f"Index '{PODCASTS_INDEX_NAME}' already exists")
        else:
            print("Index creation failed:", e)
    finally:
        await r.aclose()


async def drop_podcasts_index():
    """Drop the existing podcasts index (useful for rebuilding)."""
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await r.ft(PODCASTS_INDEX_NAME).dropindex(delete_documents=False)
        print(f"Index '{PODCASTS_INDEX_NAME}' dropped successfully")
    except Exception as e:
        print(f"Could not drop podcasts index: {e}")
    finally:
        await r.aclose()


async def build_books_index():
    """Build the books search index for OpenLibrary books.

    Includes:
    - TEXT fields for full-text search: title, author, description, subjects
    - TAG fields for exact matching: author_normalized, subjects_normalized
    - Numeric fields for sorting: popularity_score, edition_count
    """
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    schema = (
        # Primary search fields (TEXT)
        TextField("$.search_title", as_name="search_title", weight=5.0),
        TextField("$.title", as_name="title", weight=4.0),
        TextField("$.author_search", as_name="author_search", weight=3.0),
        TextField("$.author", as_name="author", weight=2.0),
        TextField("$.description", as_name="description", weight=1.0),
        TextField("$.subjects_search", as_name="subjects_search", weight=1.0),
        # Type/source filters
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        # External IDs (exact match)
        TagField("$.openlibrary_key", as_name="openlibrary_key"),
        TagField("$.primary_isbn13", as_name="primary_isbn13"),
        TagField("$.primary_isbn10", as_name="primary_isbn10"),
        TagField("$.author_olids[*]", as_name="author_olid"),
        TagField("$.cover_available", as_name="cover_available"),
        # Normalized TAG fields for exact matching
        TagField("$.author_normalized", as_name="author_normalized"),
        TagField("$.subjects_normalized[*]", as_name="subjects"),
        # Sortable numeric fields
        NumericField("$.first_publish_year", as_name="first_publish_year", sortable=True),
        NumericField("$.ratings_average", as_name="ratings_average", sortable=True),
        NumericField("$.ratings_count", as_name="ratings_count", sortable=True),
        NumericField("$.readinglog_count", as_name="readinglog_count", sortable=True),
        NumericField("$.number_of_pages", as_name="number_of_pages", sortable=True),
        NumericField("$.popularity_score", as_name="popularity_score", sortable=True),
        NumericField("$.edition_count", as_name="edition_count", sortable=True),
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField("$.modified_at", as_name="modified_at", sortable=True),
    )

    definition = IndexDefinition(prefix=["book:"], index_type=IndexType.JSON)

    try:
        await r.ft(BOOKS_INDEX_NAME).create_index(schema, definition=definition)
        print(f"Index '{BOOKS_INDEX_NAME}' created successfully")
        print("TEXT fields: search_title, title, author, description, subjects_search")
        print("TAG fields: mc_type, source, author_normalized, subjects (normalized)")
        print("Numeric fields: popularity_score, edition_count, first_publish_year")
    except Exception as e:
        if "Index already exists" in str(e):
            print(f"Index '{BOOKS_INDEX_NAME}' already exists")
        else:
            print("Index creation failed:", e)
    finally:
        await r.aclose()


async def drop_books_index():
    """Drop the existing books index (useful for rebuilding)."""
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await r.ft(BOOKS_INDEX_NAME).dropindex(delete_documents=False)
        print(f"Index '{BOOKS_INDEX_NAME}' dropped successfully")
    except Exception as e:
        print(f"Could not drop books index: {e}")
    finally:
        await r.aclose()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--drop":
        asyncio.run(drop_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        asyncio.run(drop_index())
        asyncio.run(build_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--people":
        asyncio.run(build_people_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--drop-people":
        asyncio.run(drop_people_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild-people":
        asyncio.run(drop_people_index())
        asyncio.run(build_people_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--podcasts":
        asyncio.run(build_podcasts_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--drop-podcasts":
        asyncio.run(drop_podcasts_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild-podcasts":
        asyncio.run(drop_podcasts_index())
        asyncio.run(build_podcasts_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--books":
        asyncio.run(build_books_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--drop-books":
        asyncio.run(drop_books_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild-books":
        asyncio.run(drop_books_index())
        asyncio.run(build_books_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--all":
        asyncio.run(build_index())
        asyncio.run(build_people_index())
        asyncio.run(build_podcasts_index())
        asyncio.run(build_books_index())
    else:
        asyncio.run(build_index())
