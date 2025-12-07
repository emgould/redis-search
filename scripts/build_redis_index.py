"""
Build the Redis Search index for the media search application.

Schema matches the SearchDocument dataclass from src/core/normalize.py:
- Indexed fields: search_title, mc_type, mc_subtype, year, popularity, rating, source
- Display fields (stored, not indexed): image, cast, overview
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

# Index name constant
INDEX_NAME = "idx:media"


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


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--drop":
        asyncio.run(drop_index())
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        asyncio.run(drop_index())
        asyncio.run(build_index())
    else:
        asyncio.run(build_index())
