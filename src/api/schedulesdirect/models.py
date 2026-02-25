"""
Pydantic models and constants for SchedulesDirect primetime integration.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, model_validator

from contracts.models import MCSearchResponse, MCType
from utils.pydantic_tools import BaseModelWithMethods


class SchedulesDirectServiceUnavailableError(Exception):
    """
    Raised when the SchedulesDirect service is unavailable.

    This can happen due to:
    - Too many unique IP addresses (rate limiting)
    - Service offline
    - Authentication failures
    - Token refresh failures

    Callers should catch this and return empty results gracefully.
    """

    def __init__(self, message: str, code: int | None = None, raw_response: dict | None = None):
        super().__init__(message)
        self.code = code
        self.raw_response = raw_response


# National "primetime" broadcast networks included in the default schedule
DEFAULT_PRIMETIME_NETWORKS = ["ABC", "CBS", "NBC", "FOX", "PBS", "THE CW"]

# Station IDs for SchedulesDirect's USA national over-the-air default lineup
PRIMETIME_STATION_LOOKUP: dict[str, str] = {
    "CBS": "I10759",
    "NBC": "I10760",
    "ABC": "I10761",
    "FOX": "I10762",
    "PBS": "I10763",
    "THE CW": "I10764",
}

DEFAULT_PRIMETIME_TIMEZONE = "America/New_York"
DEFAULT_PRIMETIME_WINDOW = {"start": "20:00", "end": "00:00"}


class SDStationImage(BaseModelWithMethods):
    URL: str
    width: int
    height: int
    md5: str
    hash: str
    source: str | None = None
    category: str | None = None


class SDLineupBroadcaster(BaseModelWithMethods):
    city: str | None = None
    state: str | None = None
    postalcode: str | None = None
    country: str | None = None


class SDStationsStation(BaseModelWithMethods):
    stationID: str
    name: str
    broadcaster: SDLineupBroadcaster | None = None
    callSign: str | None = None
    broadcastLanguage: list[str] | None = None
    descriptionLanguage: list[str] | None = None
    URL: str | None = None
    logo: SDStationImage | None = None
    stationLogo: list[SDStationImage] | None = None


class SDStationsMap(BaseModelWithMethods):
    stationID: str
    channel: str


class SDStationsMetadata(BaseModelWithMethods):
    lineup: str
    modified: str
    transport: str


class SDStations(BaseModelWithMethods):
    map: list[SDStationsMap]
    stations: list[SDStationsStation]
    metadata: SDStationsMetadata


class PrimetimeAiring(BaseModelWithMethods):
    """Normalized SchedulesDirect airing metadata used to annotate TMDB items."""

    network: str
    station_id: str
    program_id: str
    air_datetime_utc: str
    air_datetime_local: str
    duration_minutes: int
    is_new: bool | None = None
    is_live: bool | None = None
    is_finale: bool | None = None
    episode_title: str | None = None
    season_number: int | None = None
    episode_number: int | None = None
    original_air_date: str | None = None
    series_id: str | None = None
    additional_metadata: dict[str, Any] = Field(default_factory=dict)


class SchedulesDirectPrimetimeResponse(MCSearchResponse):
    """Search-style response for primetime schedules enriched with TMDB data."""

    data_type: MCType = MCType.TV_SERIES
    requested_date: str
    timezone: str = DEFAULT_PRIMETIME_TIMEZONE
    window_start: str
    window_end: str
    networks: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _ensure_networks(self) -> SchedulesDirectPrimetimeResponse:
        if not self.networks:
            self.networks = DEFAULT_PRIMETIME_NETWORKS
        return self


class LineupInfo(BaseModelWithMethods):
    """Individual lineup information within a headend."""

    name: str = Field(..., description="Display name of the lineup")
    lineup: str = Field(..., description="Unique lineup identifier (e.g., USA-DISH501-X)")
    uri: str = Field(..., description="API URI path to get lineup details")


class HeadendInfo(BaseModelWithMethods):
    """Headend (provider) information with available lineups."""

    headend: str = Field(..., description="Headend identifier (e.g., DISH501, NJ29429)")
    transport: str = Field(..., description="Delivery method: Cable, Satellite, IPTV, or Antenna")
    location: str = Field(..., description="Geographic location or 'National'")
    lineups: list[LineupInfo] = Field(
        default_factory=list, description="Available lineups for this headend"
    )


class HeadendsSearchResponse(BaseModelWithMethods):
    """Response from SchedulesDirect lineup search by zip code."""

    headends: list[HeadendInfo] = Field(
        default_factory=list, description="List of available headends/providers"
    )

    @property
    def total_lineups(self) -> int:
        """Total number of lineups across all headends."""
        return sum(len(h.lineups) for h in self.headends)

    def filter_by_transport(self, transport: str) -> list[HeadendInfo]:
        """Filter headends by transport type (Cable, Satellite, IPTV, Antenna)."""
        return [h for h in self.headends if h.transport.lower() == transport.lower()]

    def filter_by_location(self, location: str) -> list[HeadendInfo]:
        """Filter headends by location."""
        return [h for h in self.headends if location.lower() in h.location.lower()]


class StationLogo(BaseModelWithMethods):
    """Station logo image with metadata."""

    URL: str = Field(..., description="Full URL to the logo image")
    height: int = Field(..., description="Logo height in pixels")
    width: int = Field(..., description="Logo width in pixels")
    md5: str = Field(..., description="MD5 hash of the image")
    hash: str = Field(..., alias="hash", description="Image hash (same as md5)")
    source: str | None = Field(None, description="Logo source (e.g., Gracenote)")
    category: str | None = Field(None, description="Logo category (dark, light, gray, white)")


class StationBroadcaster(BaseModelWithMethods):
    """Broadcaster location information for a station."""

    city: str | None = Field(None, description="City where broadcaster is located")
    state: str | None = Field(None, description="State/province code")
    postalcode: str | None = Field(None, description="Postal/zip code")
    country: str | None = Field(None, description="Country code (e.g., USA)")


class LineupStation(BaseModelWithMethods):
    """Station/channel information within a lineup."""

    stationID: str = Field(..., description="Unique station identifier")
    name: str = Field(..., description="Station display name")
    callsign: str | None = Field(None, description="Station call sign (e.g., WABC)")
    broadcaster: StationBroadcaster | None = Field(None, description="Broadcaster location info")
    broadcastLanguage: list[str] | None = Field(
        None, description="Broadcast languages (e.g., ['en'])"
    )
    descriptionLanguage: list[str] | None = Field(None, description="Description languages")
    URL: str | None = Field(None, description="Station website URL")
    stationLogo: list[StationLogo] | None = Field(None, description="Available station logos")
    logo: StationLogo | None = Field(None, description="Primary/default station logo")


class LineupChannelMap(BaseModelWithMethods):
    """Channel mapping entry linking station to channel number."""

    stationID: str = Field(..., description="Station identifier")
    channel: str = Field(..., description="Channel number/identifier")


class LineupMetadata(BaseModelWithMethods):
    """Metadata about the lineup."""

    lineup: str = Field(..., description="Lineup identifier (e.g., USA-YTBE501-X)")
    modified: str = Field(..., description="Last modified timestamp (ISO 8601)")
    transport: str = Field(..., description="Transport type (Cable, Satellite, IPTV, Antenna)")


class LineupChannelsResponse(BaseModelWithMethods):
    """Response from get_channels_for_lineup containing stations and channel mappings."""

    map: list[LineupChannelMap] = Field(
        default_factory=list, description="Channel number to station mappings"
    )
    stations: list[LineupStation] = Field(default_factory=list, description="Station details")
    metadata: LineupMetadata = Field(..., description="Lineup metadata")

    @property
    def total_channels(self) -> int:
        """Total number of channels in the lineup."""
        return len(self.map)

    @property
    def total_stations(self) -> int:
        """Total number of stations in the lineup."""
        return len(self.stations)

    def get_station_by_id(self, station_id: str) -> LineupStation | None:
        """Find a station by its ID."""
        return next((s for s in self.stations if s.stationID == station_id), None)

    def get_channel_for_station(self, station_id: str) -> str | None:
        """Get the channel number for a given station ID."""
        mapping = next((m for m in self.map if m.stationID == station_id), None)
        return mapping.channel if mapping else None

    def filter_by_language(self, language: str) -> list[LineupStation]:
        """Filter stations by broadcast language."""
        return [
            s
            for s in self.stations
            if s.broadcastLanguage
            and language.lower() in [lang.lower() for lang in s.broadcastLanguage]
        ]


class AccountInfo(BaseModelWithMethods):
    """Account information from SchedulesDirect status response."""

    expires: str = Field(..., description="Account expiration date (ISO 8601 UTC)")
    accountExpiration: int = Field(..., description="Account expiration as Unix timestamp")
    messages: list[str] = Field(default_factory=list, description="Account messages")
    maxLineups: int = Field(..., description="Maximum number of lineups allowed")


class AccountLineup(BaseModelWithMethods):
    """Lineup subscription information from account status."""

    lineup: str = Field(..., description="Lineup identifier (e.g., USA-DITV-X)")
    modified: str = Field(..., description="Last modified timestamp (ISO 8601)")
    uri: str = Field(..., description="API URI path to get lineup details")
    name: str = Field(..., description="Display name of the lineup")


class SystemStatus(BaseModelWithMethods):
    """System status information."""

    date: str = Field(..., description="Status date (ISO 8601)")
    status: str = Field(..., description="Status (e.g., Online, Maintenance)")
    message: str = Field(..., description="Status message")


class AccountStatusResponse(BaseModelWithMethods):
    """Complete response from SchedulesDirect status endpoint."""

    account: AccountInfo = Field(..., description="Account information")
    lineups: list[AccountLineup] = Field(default_factory=list, description="Subscribed lineups")
    lastDataUpdate: str = Field(..., description="Last data update timestamp (ISO 8601)")
    notifications: list[Any] = Field(default_factory=list, description="Account notifications")
    systemStatus: list[SystemStatus] = Field(
        default_factory=list, description="System status information"
    )
    serverID: str = Field(..., description="Server identifier")
    datetime: str = Field(..., description="Current server datetime (ISO 8601)")
    code: int = Field(..., description="Response code (0 = success)")
    tokenExpires: int = Field(..., description="Token expiration as Unix timestamp")
    serverTime: int = Field(..., description="Server time as Unix timestamp")

    @property
    def is_account_active(self) -> bool:
        """Check if account is currently active (not expired)."""
        from datetime import UTC, datetime

        return self.account.accountExpiration > datetime.now(UTC).timestamp()

    @property
    def is_token_valid(self) -> bool:
        """Check if token is still valid (not expired)."""
        from datetime import UTC, datetime

        return self.tokenExpires > datetime.now(UTC).timestamp()

    @property
    def total_lineups(self) -> int:
        """Total number of subscribed lineups."""
        return len(self.lineups)

    def get_lineup_by_id(self, lineup_id: str) -> AccountLineup | None:
        """Find a lineup by its identifier."""
        return next((lineup for lineup in self.lineups if lineup.lineup == lineup_id), None)


# ----------------------------------------------------------
# Program Metadata Models
# ----------------------------------------------------------


class SDProgramTitle(BaseModelWithMethods):
    """Program title with language information."""

    title120: str = Field(..., description="Title up to 120 characters")
    titleLanguage: str | None = Field(None, description="Language code (e.g., 'en-GB')")


class SDProgramDescription(BaseModelWithMethods):
    """Individual description with language."""

    descriptionLanguage: str = Field(..., description="Language code (e.g., 'en')")
    description: str = Field(..., description="Description text")


class SDProgramDescriptions(BaseModelWithMethods):
    """Container for various description lengths."""

    description1000: list[SDProgramDescription] | None = Field(
        None, description="Long descriptions (up to 1000 chars)"
    )
    description100: list[SDProgramDescription] | None = Field(
        None, description="Short descriptions (up to 100 chars)"
    )

    def get_best_description(self, language: str = "en") -> str | None:
        """Get the longest available description in the specified language."""
        for desc_list in [self.description1000, self.description100]:
            if desc_list:
                for desc in desc_list:
                    if desc.descriptionLanguage == language:
                        return desc.description
        return None


class SDCastMember(BaseModelWithMethods):
    """Cast member information."""

    billingOrder: str | None = Field(None, description="Billing order (e.g., '01')")
    role: str | None = Field(None, description="Role type (e.g., 'Actor')")
    name: str = Field(..., description="Person's name")
    characterName: str | None = Field(None, description="Character name if applicable")
    nameId: str | None = Field(None, description="Gracenote name ID")
    personId: str | None = Field(None, description="Gracenote person ID")


class SDCrewMember(BaseModelWithMethods):
    """Crew member information."""

    billingOrder: str | None = Field(None, description="Billing order (e.g., '01')")
    role: str | None = Field(
        None, description="Role type (e.g., 'Writer (Novel)', 'Executive Producer')"
    )
    name: str = Field(..., description="Person's name")
    nameId: str | None = Field(None, description="Gracenote name ID")
    personId: str | None = Field(None, description="Gracenote person ID")


class SDContentRating(BaseModelWithMethods):
    """Content rating from a ratings body."""

    body: str = Field(..., description="Ratings body name")
    code: str = Field(..., description="Rating code (e.g., '16', 'MA 15+')")
    country: str = Field(..., description="Country code (e.g., 'DEU', 'AUS')")
    contentWarning: list[str] | None = Field(
        None, description="Content warnings (e.g., ['Violence', 'Strong Themes'])"
    )


class SDGracenoteMetadata(BaseModelWithMethods):
    """Gracenote-specific metadata."""

    season: int | None = Field(None, description="Season number")
    episode: int | None = Field(None, description="Episode number")


class SDMetadataEntry(BaseModelWithMethods):
    """Container for provider-specific metadata."""

    Gracenote: SDGracenoteMetadata | None = Field(None, description="Gracenote metadata")


class SDProgramMetadata(BaseModelWithMethods):
    """Complete program metadata from SchedulesDirect /programs endpoint."""

    programID: str = Field(..., description="Unique program identifier")
    resourceID: str | None = Field(None, description="Gracenote resource ID")
    titles: list[SDProgramTitle] = Field(default_factory=list, description="Program titles")
    descriptions: SDProgramDescriptions | None = Field(None, description="Program descriptions")
    originalAirDate: str | None = Field(None, description="Original air date (YYYY-MM-DD)")
    showType: str | None = Field(None, description="Show type (e.g., 'Series', 'Movie')")
    entityType: str | None = Field(None, description="Entity type (e.g., 'Episode', 'Movie')")
    country: list[str] | None = Field(None, description="Country codes (e.g., ['GBR'])")
    genres: list[str] | None = Field(None, description="Genre list")
    cast: list[SDCastMember] | None = Field(None, description="Cast members")
    crew: list[SDCrewMember] | None = Field(None, description="Crew members")
    contentRating: list[SDContentRating] | None = Field(None, description="Content ratings")
    episodeTitle150: str | None = Field(None, description="Episode title (up to 150 chars)")
    duration: int | None = Field(None, description="Duration in seconds")
    metadata: list[SDMetadataEntry] | None = Field(None, description="Provider-specific metadata")
    hasImageArtwork: bool | None = Field(None, description="Has image artwork available")
    hasEpisodeArtwork: bool | None = Field(None, description="Has episode artwork available")
    hasSeasonArtwork: bool | None = Field(None, description="Has season artwork available")
    hasSeriesArtwork: bool | None = Field(None, description="Has series artwork available")
    hash: str | None = Field(None, description="Content hash")
    md5: str | None = Field(None, description="MD5 hash")

    @property
    def title(self) -> str | None:
        """Get the primary title."""
        return self.titles[0].title120 if self.titles else None

    @property
    def description(self) -> str | None:
        """Get the best available description in English."""
        if self.descriptions:
            return self.descriptions.get_best_description("en")
        return None

    @property
    def season_number(self) -> int | None:
        """Get season number from Gracenote metadata."""
        if self.metadata:
            for entry in self.metadata:
                if entry.Gracenote and entry.Gracenote.season is not None:
                    return entry.Gracenote.season
        return None

    @property
    def episode_number(self) -> int | None:
        """Get episode number from Gracenote metadata."""
        if self.metadata:
            for entry in self.metadata:
                if entry.Gracenote and entry.Gracenote.episode is not None:
                    return entry.Gracenote.episode
        return None

    @property
    def duration_minutes(self) -> int | None:
        """Get duration in minutes."""
        return self.duration // 60 if self.duration else None
