"""
RottenTomatoes Models - Pydantic models for RottenTomatoes Algolia API data structures.
Follows the TMDB pattern with Pydantic 2.0.
"""

from typing import Any

from pydantic import Field, model_validator

from contracts.models import (
    MCBaseItem,
    MCImage,
    MCLink,
    MCSearchResponse,
    MCSources,
    MCSubType,
    MCType,
    MCUrlType,
    generate_mc_id,
)
from utils.pydantic_tools import BaseModelWithMethods

# ========== Algolia API Response Models ==========


class Trailer(BaseModelWithMethods):
    """Model for a RottenTomatoes trailer."""

    mpxId: str | None = None
    thumbnailUrl: str | None = None
    publicId: str | None = None
    title: str | None = None
    runTime: float | None = None

    model_config = {"extra": "allow"}


class RottenTomatoes(BaseModelWithMethods):
    """Model for RottenTomatoes scores and ratings."""

    audienceScore: int | None = None
    criticsIconUrl: str | None = None
    audienceIconUrl: str | None = None
    criticsScoreSentiment: str | None = None
    criticsScore: int | None = None
    audienceScoreSentiment: str | None = None
    wantToSeeCount: int | None = None
    verifiedHot: bool | None = None
    scoreSentiment: str | None = None
    certifiedFresh: bool | None = None
    newAdjustedTMScore: int | None = None

    model_config = {"extra": "allow"}


class CastCrew(BaseModelWithMethods):
    """Model for cast and crew information."""

    cast: list[str] | None = None
    crew: dict[str, list[str]] | None = None

    model_config = {"extra": "allow"}


class CastMember(BaseModelWithMethods):
    """Model for a cast member."""

    role: str | None = None
    emsId: str | None = None
    name: str | None = None
    personId: str | None = None

    model_config = {"extra": "allow"}


class CrewMember(BaseModelWithMethods):
    """Model for a crew member."""

    role: str | None = None
    emsId: str | None = None
    name: str | None = None
    personId: str | None = None

    model_config = {"extra": "allow"}


class ContentRtHit(BaseModelWithMethods):
    """Model for a content hit from RottenTomatoes Algolia search."""

    emsId: str | None = None
    emsVersionId: str | None = None
    tmsId: str | None = None
    rtId: int | None = None
    type: str | None = None
    title: str | None = None
    titles: list[str] | None = None
    vanity: str | None = None
    description: str | None = None
    releaseYear: int | None = None
    seriesPremiere: str | None = None
    rating: str | None = None
    genres: list[str] | None = None
    posterImageUrl: str | None = None
    rottenTomatoes: RottenTomatoes | None = None
    castCrew: CastCrew | None = None
    cast: list[CastMember] | None = None
    crew: list[CrewMember] | None = None
    seasons: list[int] | None = None
    pageViews_popularity: int | None = None
    trailer: Trailer | None = None
    typeId: int | None = None
    promotion: int | None = None
    updateDate: str | None = None
    isEmsSearchable: int | None = None
    objectID: str | None = None
    keywords: list[str] | None = None
    studios: list[str] | None = None
    aka: list[str] | None = None
    runTime: int | None = None

    _highlightResult: dict | None = None
    _rankingInfo: dict | None = None
    _snippetResult: dict | None = None
    _tags: list[str] | None = None

    model_config = {"extra": "allow"}


class PeopleRtHit(BaseModelWithMethods):
    """Model for a people hit from RottenTomatoes Algolia search."""

    name: str | None = None
    personId: str | None = None
    emsId: str | None = None
    objectID: str | None = None
    imageUrl: str | None = None
    birthDate: str | None = None
    deathDate: str | None = None
    biography: str | None = None
    aliases: list[str] | None = None
    knownFor: list[str] | None = None
    gender: str | None = None
    popularity: int | None = None

    _highlightResult: dict | None = None
    _rankingInfo: dict | None = None
    _snippetResult: dict | None = None
    _tags: list[str] | None = None

    model_config = {"extra": "allow"}


class SearchResultBase(BaseModelWithMethods):
    """Base model for Algolia search results."""

    nbHits: int | None = None
    page: int | None = None
    nbPages: int | None = None
    hitsPerPage: int | None = None
    exhaustiveNbHits: bool | None = None
    query: str | None = None
    params: str | None = None
    index: str | None = None
    queryID: str | None = None

    model_config = {"extra": "allow"}


class SearchResultContent(SearchResultBase):
    """Model for content search results."""

    hits: list[ContentRtHit] = Field(default_factory=list)


class SearchResultPeople(SearchResultBase):
    """Model for people search results."""

    hits: list[PeopleRtHit] = Field(default_factory=list)


class AlgoliaMultiQueryResponse(BaseModelWithMethods):
    """Model for Algolia multi-query response."""

    results: list[SearchResultContent | SearchResultPeople] = Field(default_factory=list)
    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    @classmethod
    def parse_results_by_index(cls, data: Any) -> Any:
        """Parse results into correct types based on index name."""
        if not isinstance(data, dict):
            return data

        raw_results = data.get("results", [])
        parsed_results: list[SearchResultContent | SearchResultPeople] = []

        for result in raw_results:
            if not isinstance(result, dict):
                parsed_results.append(result)
                continue

            index_name = result.get("index", "")

            if index_name == "people_rt":
                # Parse as SearchResultPeople
                parsed_results.append(SearchResultPeople.model_validate(result))
            else:
                # Default to SearchResultContent (content_rt or unknown)
                parsed_results.append(SearchResultContent.model_validate(result))

        data["results"] = parsed_results
        return data


# ========== MediaCircle Compatible Models ==========


class MCRottenTomatoesItem(MCBaseItem):
    """Model for a RottenTomatoes content item compatible with MediaCircle."""

    # MCBaseItem fields
    mc_type: MCType = MCType.MIXED  # Will be set based on type field
    source: MCSources = MCSources.ROTTENTOMATOES

    # RottenTomatoes specific fields
    rt_id: int | None = None
    ems_id: str | None = None
    tms_id: str | None = None
    title: str | None = None
    description: str | None = None
    release_year: int | None = None
    rating: str | None = None  # MPAA rating (PG, R, etc.)
    genres: list[str] = Field(default_factory=list)
    runtime: int | None = None  # In minutes
    vanity: str | None = None  # URL slug

    # RottenTomatoes scores
    critics_score: int | None = None
    audience_score: int | None = None
    critics_sentiment: str | None = None  # "fresh", "rotten", "certified_fresh"
    audience_sentiment: str | None = None
    certified_fresh: bool = False
    verified_hot: bool = False

    # Media URLs
    poster_url: str | None = None
    rt_url: str | None = None

    # Cast and crew
    cast_names: list[str] = Field(default_factory=list)
    director: str | None = None

    # TV-specific fields
    series_premiere: str | None = None
    seasons: list[int] = Field(default_factory=list)

    # Popularity metrics
    popularity: int | None = None

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "MCRottenTomatoesItem":
        """Auto-generate mc_id and populate images/links if not provided."""
        # Set source_id from ems_id or rt_id
        if not self.source_id:
            self.source_id = self.ems_id or (str(self.rt_id) if self.rt_id else None)

        # Generate mc_id
        if not self.mc_id and self.source_id:
            id_dict = {"rt_id": self.rt_id, "ems_id": self.ems_id}
            self.mc_id = generate_mc_id(id_dict, self.mc_type)

        # Populate images array from poster_url if available
        if self.poster_url and not self.images:
            self.images = [
                MCImage(
                    url=self.poster_url,
                    key="poster",
                    type=MCUrlType.URL,
                    description="poster",
                )
            ]

        # Populate links array with RT URL
        if self.rt_url and not self.links:
            self.links = [
                MCLink(
                    url=self.rt_url,
                    key="rottentomatoes",
                    description="RottenTomatoes page",
                )
            ]

        # Populate metrics with scores
        if not self.metrics:
            self.metrics = {}
        if self.critics_score is not None:
            self.metrics["critics_score"] = self.critics_score
        if self.audience_score is not None:
            self.metrics["audience_score"] = self.audience_score
        if self.popularity is not None:
            self.metrics["popularity"] = self.popularity

        return self

    @classmethod
    def from_content_hit(cls, hit: ContentRtHit) -> "MCRottenTomatoesItem":
        """Create MCRottenTomatoesItem from a ContentRtHit."""
        # Determine mc_type based on content type
        content_type = (hit.type or "").lower()
        if content_type == "movie":
            mc_type = MCType.MOVIE
        elif content_type in ("tv", "series", "tvSeries"):
            mc_type = MCType.TV_SERIES
        else:
            mc_type = MCType.MIXED

        # Extract RT scores
        rt_scores = hit.rottenTomatoes or RottenTomatoes()

        # Extract cast names
        cast_names: list[str] = []
        if hit.cast:
            cast_names = [c.name for c in hit.cast if c.name]
        elif hit.castCrew and hit.castCrew.cast:
            cast_names = hit.castCrew.cast

        # Extract director from crew
        director: str | None = None
        if hit.crew:
            for member in hit.crew:
                if member.role and member.role.lower() == "director" and member.name:
                    director = member.name
                    break
        elif hit.castCrew and hit.castCrew.crew:
            directors = hit.castCrew.crew.get("Director", [])
            if directors:
                director = directors[0]

        # Build RT URL from vanity
        rt_url: str | None = None
        if hit.vanity:
            if content_type == "movie":
                rt_url = f"https://www.rottentomatoes.com/m/{hit.vanity}"
            elif content_type in ("tv", "series", "tvSeries"):
                rt_url = f"https://www.rottentomatoes.com/tv/{hit.vanity}"

        return cls(
            mc_type=mc_type,
            rt_id=hit.rtId,
            ems_id=hit.emsId,
            tms_id=hit.tmsId,
            title=hit.title,
            description=hit.description,
            release_year=hit.releaseYear,
            rating=hit.rating,
            genres=hit.genres or [],
            runtime=hit.runTime,
            vanity=hit.vanity,
            critics_score=rt_scores.criticsScore,
            audience_score=rt_scores.audienceScore,
            critics_sentiment=rt_scores.criticsScoreSentiment,
            audience_sentiment=rt_scores.audienceScoreSentiment,
            certified_fresh=rt_scores.certifiedFresh or False,
            verified_hot=rt_scores.verifiedHot or False,
            poster_url=hit.posterImageUrl,
            rt_url=rt_url,
            cast_names=cast_names,
            director=director,
            series_premiere=hit.seriesPremiere,
            seasons=hit.seasons or [],
            popularity=hit.pageViews_popularity,
        )


class MCRottenTomatoesPersonItem(MCBaseItem):
    """Model for a RottenTomatoes person item compatible with MediaCircle."""

    # MCBaseItem fields
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.ACTOR
    source: MCSources = MCSources.ROTTENTOMATOES

    # RottenTomatoes person fields
    person_id: str | None = None
    ems_id: str | None = None
    name: str | None = None
    biography: str | None = None
    birth_date: str | None = None
    death_date: str | None = None
    gender: str | None = None
    aliases: list[str] = Field(default_factory=list)
    known_for: list[str] = Field(default_factory=list)
    image_url: str | None = None
    popularity: int | None = None

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "MCRottenTomatoesPersonItem":
        """Auto-generate mc_id and populate images if not provided."""
        # Set source_id from person_id or ems_id
        if not self.source_id:
            self.source_id = self.person_id or self.ems_id

        # Generate mc_id
        if not self.mc_id and self.source_id:
            id_dict = {"person_id": self.person_id, "ems_id": self.ems_id}
            self.mc_id = generate_mc_id(id_dict, self.mc_type)

        # Populate images array from image_url if available
        if self.image_url and not self.images:
            self.images = [
                MCImage(
                    url=self.image_url,
                    key="profile",
                    type=MCUrlType.URL,
                    description="profile photo",
                )
            ]

        return self

    @classmethod
    def from_people_hit(cls, hit: PeopleRtHit) -> "MCRottenTomatoesPersonItem":
        """Create MCRottenTomatoesPersonItem from a PeopleRtHit."""
        return cls(
            person_id=hit.personId,
            ems_id=hit.emsId,
            name=hit.name,
            biography=hit.biography,
            birth_date=hit.birthDate,
            death_date=hit.deathDate,
            gender=hit.gender,
            aliases=hit.aliases or [],
            known_for=hit.knownFor or [],
            image_url=hit.imageUrl,
            popularity=hit.popularity,
        )


# ========== Response Models ==========


class RottenTomatoesSearchResponse(MCSearchResponse):
    """Model for RottenTomatoes search response."""

    results: list[MCRottenTomatoesItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "RottenTomatoes Algolia Search"
    data_type: MCType = MCType.MIXED
    page: int = 1

    # Additional search metadata
    content_hits: int = 0
    people_hits: int = 0


class RottenTomatoesPeopleSearchResponse(MCSearchResponse):
    """Model for RottenTomatoes people search response."""

    results: list[MCRottenTomatoesPersonItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "RottenTomatoes Algolia Search"
    data_type: MCType = MCType.PERSON
    page: int = 1
