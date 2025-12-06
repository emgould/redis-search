
import os

import httpx


async def fetch_all_media():
    url = os.getenv("API_SOURCE_URL")
    if not url:
        raise ValueError("API_SOURCE_URL environment variable is not set")
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.json()
