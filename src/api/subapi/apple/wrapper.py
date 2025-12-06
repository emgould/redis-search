import asyncio
import re
import urllib.parse
from typing import Any, cast

import aiohttp

from api.subapi.apple.auth import AppleAuth
from api.subapi.apple.models import AppleMusicAlbum


class AppleMusicAPI(AppleAuth):
    """
    Derived AppleAuth service for Apple Music API requests.
    Provides search functionality for albums, songs, etc.
    """

    BASE_URL = "https://api.music.apple.com/v1/catalog"

    def __init__(self, storefront: str = "us"):
        super().__init__()
        self.storefront = storefront

    async def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """
        Internal helper to perform authorized GET requests to Apple Music API.
        """
        token = await self.get_developer_token()
        if not token:
            raise ValueError("Missing or invalid developer token.")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        url = f"{self.BASE_URL}/{self.storefront}/{endpoint}"

        async with (
            aiohttp.ClientSession() as session,
            session.get(url, headers=headers, params=params) as response,
        ):
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Apple API error {response.status}: {text}")
            result = await response.json()
            return cast(dict[str, Any], result)

    async def search_album(self, term: str, limit: int = 1) -> list[AppleMusicAlbum] | None:
        """
        Search Apple Music for an album and return key metadata.
        Includes native deep links for Apple Music and YouTube.
        """
        params = {"term": term, "types": "albums", "limit": str(limit)}

        data = await self._request("search", params)
        if not data:
            return None

        albums_data = data.get("results", {}).get("albums", {}).get("data", [])
        if not albums_data:
            return None

        albums = [AppleMusicAlbum.model_validate(album) for album in albums_data]

        # --- ✅ Generate Apple Music native deep links (no web URLs)
        for album in albums:
            name = getattr(album.attributes, "name", "").strip()
            slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "album"
            album_id = album.id
            # musics:// works best for iOS deep linking (Music app)
            album.deeplink = f"musics://music.apple.com/{self.storefront}/album/{urllib.parse.quote(slug)}/{album_id}"

        # --- ✅ Generate YouTube deep links (iOS, Android, Web)
        youtube_tasks = [self.get_youtube_video_id(album.attributes.name) for album in albums]
        youtube_video_ids = await asyncio.gather(*youtube_tasks, return_exceptions=True)

        for album, youtube_video_id in zip(albums, youtube_video_ids, strict=False):
            if youtube_video_id and not isinstance(youtube_video_id, Exception):
                video_id = youtube_video_id
                # YouTube Music deeplinks (not regular YouTube)
                album.youtube_ios_link = (
                    f"https://music.youtube.com/watch?v={video_id}"  # iOS - uses universal link
                )
                album.youtube_android_link = (
                    f"https://music.youtube.com/watch?v={video_id}"  # Android - uses universal link
                )
                album.youtube_web_fallback = (
                    f"https://music.youtube.com/watch?v={video_id}"  # fallback
                )

        return albums

    async def get_youtube_video_id(self, query: str) -> str | None:
        """
        Fetch a YouTube video ID by scraping the search results page.
        """
        search_url = f"https://www.youtube.com/results?search_query={urllib.parse.quote(query)}"
        async with aiohttp.ClientSession() as session, session.get(search_url) as resp:
            html = await resp.text()
            match = re.search(r"watch\?v=([A-Za-z0-9_-]{11})", html)
            if match:
                return match.group(1)
        return None


# Singleton instance for use across the application
apple_wrapper = AppleMusicAPI(storefront="us")


# --- ✅ Example usage
if __name__ == "__main__":

    async def main():
        apple = AppleMusicAPI(storefront="us")
        albums = await apple.search_album("Midnights Taylor Swift")

        if albums:
            for album in albums:
                print(f"Album: {album.attributes.name}")
                print(f"Apple deeplink: {album.deeplink}")
                print(f"YouTube (iOS): {album.youtube_ios_link}")
                print(f"YouTube (Android): {album.youtube_android_link}")
                print(f"YouTube (Web): {album.youtube_web_fallback}")
                print("-" * 40)
        else:
            print("No albums found.")

    asyncio.run(main())
