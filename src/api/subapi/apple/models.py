from pydantic import BaseModel, Field, HttpUrl


class Artwork(BaseModel):
    """Album artwork metadata."""

    bg_color: str | None = Field(None, alias="bgColor", description="Background color hex")
    height: int = Field(..., description="Artwork height in pixels")
    width: int = Field(..., description="Artwork width in pixels")
    text_color1: str | None = Field(None, alias="textColor1", description="Primary text color hex")
    text_color2: str | None = Field(
        None, alias="textColor2", description="Secondary text color hex"
    )
    text_color3: str | None = Field(None, alias="textColor3", description="Tertiary text color hex")
    text_color4: str | None = Field(
        None, alias="textColor4", description="Quaternary text color hex"
    )
    url: str = Field(..., description="Artwork URL template with {w}x{h} placeholders")

    class Config:
        populate_by_name = True


class EditorialNotes(BaseModel):
    """Editorial notes for the album."""

    short: str | None = Field(None, description="Short editorial note")
    standard: str | None = Field(None, description="Standard editorial note")
    tagline: str | None = Field(None, description="Editorial tagline")

    class Config:
        populate_by_name = True


class PlayParams(BaseModel):
    """Play parameters for the album."""

    id: str = Field(..., description="Album ID for playback")
    kind: str = Field(..., description="Resource kind (e.g., 'album')")

    class Config:
        populate_by_name = True


class AlbumAttributes(BaseModel):
    """Album attributes from Apple Music API."""

    artist_name: str = Field(..., alias="artistName", description="Artist name")
    artwork: Artwork = Field(..., description="Album artwork")
    content_rating: str | None = Field(
        None, alias="contentRating", description="Content rating (e.g., 'explicit')"
    )
    copyright: str | None = Field(None, description="Copyright notice")
    editorial_notes: EditorialNotes | None = Field(
        None, alias="editorialNotes", description="Editorial notes"
    )
    genre_names: list[str] = Field(..., alias="genreNames", description="Genre names")
    is_compilation: bool = Field(
        ..., alias="isCompilation", description="Whether album is a compilation"
    )
    is_complete: bool = Field(..., alias="isComplete", description="Whether album is complete")
    is_mastered_for_itunes: bool = Field(
        ..., alias="isMasteredForItunes", description="Whether mastered for iTunes"
    )
    is_single: bool = Field(..., alias="isSingle", description="Whether album is a single")
    name: str = Field(..., description="Album name")
    play_params: PlayParams | None = Field(None, alias="playParams", description="Play parameters")
    record_label: str = Field(..., alias="recordLabel", description="Record label")
    release_date: str = Field(
        ..., alias="releaseDate", description="Release date (YYYY-MM-DD format)"
    )
    track_count: int = Field(..., alias="trackCount", description="Number of tracks")
    upc: str | None = Field(None, description="Universal Product Code")
    url: HttpUrl = Field(..., description="Apple Music album URL")

    class Config:
        populate_by_name = True


class AppleMusicAlbum(BaseModel):
    """Apple Music album resource from API."""

    id: str = Field(..., description="Apple Music album ID")
    type: str = Field(..., description="Resource type (e.g., 'albums')")
    href: str = Field(..., description="API href path")
    attributes: AlbumAttributes = Field(..., description="Album attributes")
    deeplink: str = Field(default="", description="Apple Music album deep link URL")
    youtube_ios_link: str | None = Field(None, description="YouTube iOS link")
    youtube_android_link: str | None = Field(None, description="YouTube Android link")
    youtube_web_fallback: str | None = Field(None, description="YouTube web fallback")

    class Config:
        populate_by_name = True
