"""
Deprecated LastFM Models - Legacy models kept for potential future use.

DEPRECATED: These models are no longer actively used in the codebase.
They are preserved here in case they become useful in the future.

Current Status:
- OdesliPlatformLinks: Replaced by direct Apple Music API integration
  See: api.subapi.apple.wrapper.AppleMusicAPI
"""

from pydantic import BaseModel, Field


class OdesliPlatformLinks(BaseModel):
    """
    DEPRECATED: Model for Odesli/Songlink platform links.

    This model was used when we relied on the Odesli API to expand Spotify URLs
    to other streaming platforms. We have since migrated to direct API integrations
    (Apple Music API, YouTube scraping) for better control and reliability.

    Kept for reference in case we need to reintegrate Odesli or a similar service.
    """

    spotify: str | None = None
    apple_music: str | None = Field(default=None, alias="applemusic")
    youtube_music: str | None = Field(default=None, alias="youtubemusic")
    amazon_music: str | None = Field(default=None, alias="amazonmusic")
    tidal: str | None = None
    deezer: str | None = None

    model_config = {"populate_by_name": True}
