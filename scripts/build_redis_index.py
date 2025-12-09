"""
Build the Redis Search indexes for the media search application.

Three indexes:
1. idx:media - Movies and TV shows
   Schema matches the SearchDocument dataclass from src/core/normalize.py:
   - Indexed fields: search_title, mc_type, mc_subtype, year, popularity, rating, source
   - Display fields (stored, not indexed): image, cast, overview

2. idx:people - Person/actor data
   - Indexed fields: search_title (name), also_known_as, mc_type, mc_subtype, popularity, source
   - Display fields (stored, not indexed): image, overview (biography)

3. idx:podcasts - Podcast data for autocomplete
   - Indexed fields: search_title, author, mc_type, source, language, category1, category2, popularity, episode_count
   - Display fields (stored, not indexed): image, description, url, site, etc.
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
        # Sortable numeric fields for ranking
        NumericField("$.popularity", as_name="popularity", sortable=True),
        NumericField("$.rating", as_name="rating", sortable=True),
        NumericField("$.year", as_name="year", sortable=True),
        # Note: image, cast, overview are stored in JSON but NOT indexed
        # They are display-only fields returned with search results
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
        # Note: image, overview are stored in JSON but NOT indexed
        # They are display-only fields returned with search results
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
        # Author/creator name - searchable (podcast display field)
        TextField("$.author", as_name="author", weight=3.0),
        # SearchDocument standard fields
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        TagField("$.id", as_name="id"),  # mc_id from SearchDocument.id
        # Language filter (podcast display field)
        TagField("$.language", as_name="language"),
        # Category filters (podcast display field)
        TagField("$.categories.1", as_name="category1"),
        TagField("$.categories.2", as_name="category2"),
        # Sortable numeric fields for ranking (from SearchDocument)
        NumericField("$.popularity", as_name="popularity", sortable=True),
        # Episode count (podcast display field, useful for sorting)
        NumericField("$.episode_count", as_name="episode_count", sortable=True),
        # Note: image, overview, and other fields are stored but NOT indexed
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
    elif len(sys.argv) > 1 and sys.argv[1] == "--all":
        asyncio.run(build_index())
        asyncio.run(build_people_index())
        asyncio.run(build_podcasts_index())
    else:
        asyncio.run(build_index())
