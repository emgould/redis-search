"""
Public List index document contract.

Defines the Redis JSON document shape, index schema, and normalization
for MediaCircle public List discovery documents.

Data sourcing:
- MediaCircle (Firestore) is the canonical source of truth for public
  Lists. Documents are written by the MediaCircle backend projection
  pipeline via the authenticated public-lists write API (and rebuildable
  with the MediaCircle backfill), the same way other indices are sourced
  from their upstream APIs via ETL.
- Like every other index, ``idx:public_lists`` participates fully in the
  promote-to-dev / copy-to-local / clone environment-migration tooling.
"""

import time

from pydantic import BaseModel
from pydantic import Field as PydanticField
from redis.commands.search.field import Field, NumericField, TagField, TextField

from core.iptc import normalize_tag
from core.normalize import compact_title, normalize_search_title

PUBLIC_LIST_PREFIX = "public_list:"
PUBLIC_LIST_INDEX = "idx:public_lists"


def public_list_key(list_id: str) -> str:
    """Redis JSON key for a public List document."""
    return f"{PUBLIC_LIST_PREFIX}{list_id}"


class PublicListItem(BaseModel):
    """A single media item belonging to a public List."""

    mc_id: str
    title: str
    mc_type: str = "unknown"


class PublicListUpsertRequest(BaseModel):
    """
    Write-API payload for upserting one public List document.

    All engagement / follower metrics are computed by the MediaCircle
    projection worker; this service stores and indexes them verbatim.
    Timestamps are Unix epoch seconds.
    """

    list_id: str
    name: str
    description: str | None = None
    image: str | None = None
    circle_type: str | None = None
    visibility: str = "public"
    circle_mode: str = "public"

    owner_id: str
    owner_username: str | None = None
    owner_display_name: str | None = None
    owner_photo_url: str | None = None
    is_mediacircle_owner: bool = False

    topic_keywords: list[str] = PydanticField(default_factory=list)
    topic_labels: list[str] = PydanticField(default_factory=list)
    topic_summary: str | None = None
    llm_classification_version: str | None = None

    items: list[PublicListItem] = PydanticField(default_factory=list)

    followers_count: int = 0
    engagement_score: float = 0.0
    engagement_score_version: str | None = None
    vote_count: int = 0
    net_vote_score: int = 0
    super_up_count: int = 0

    created_at: int | None = None
    discovery_updated_at: int | None = None


def public_list_to_redis_doc(
    request: PublicListUpsertRequest,
    existing: dict[str, object] | None = None,
) -> dict[str, object]:
    """
    Build the Redis JSON document for a public List upsert.

    Derives searchable fields (``search_title``, ``title_compact``,
    normalized tags, joined item-title text) from the request payload.
    ``created_at`` from an existing document is preserved on update.
    """
    now = int(time.time())

    item_mc_ids = [item.mc_id for item in request.items if item.mc_id]
    item_titles = [item.title for item in request.items if item.title]
    item_title_compacts = [compact_title(title) for title in item_titles if title]
    item_types = sorted(
        {normalize_tag(item.mc_type) for item in request.items if item.mc_type}
    )

    created_at = request.created_at
    if created_at is None and existing is not None:
        raw_created = existing.get("created_at")
        created_at = int(raw_created) if isinstance(raw_created, (int, float)) else None
    if created_at is None:
        created_at = now

    return {
        "list_id": request.list_id,
        "name": request.name,
        "search_title": normalize_search_title(request.name),
        "title_compact": compact_title(request.name),
        "description": request.description or "",
        "description_text": normalize_search_title(request.description or ""),
        "image": request.image or "",
        "circle_type": normalize_tag(request.circle_type or ""),
        "visibility": normalize_tag(request.visibility),
        "circle_mode": normalize_tag(request.circle_mode),
        "owner_id": request.owner_id,
        "owner_username": normalize_tag(request.owner_username or ""),
        "owner_username_display": request.owner_username or "",
        "owner_display_name": request.owner_display_name or "",
        "owner_photo_url": request.owner_photo_url or "",
        "is_mediacircle_owner": "true" if request.is_mediacircle_owner else "false",
        "topic_keywords": [
            normalized
            for keyword in request.topic_keywords
            if (normalized := normalize_tag(keyword))
        ],
        "topic_labels": [
            normalized
            for label in request.topic_labels
            if (normalized := normalize_tag(label))
        ],
        "topic_summary": request.topic_summary or "",
        "llm_classification_version": request.llm_classification_version or "",
        "items": [item.model_dump() for item in request.items],
        "item_mc_ids": item_mc_ids,
        "item_titles": item_titles,
        "item_titles_text": normalize_search_title(" | ".join(item_titles)),
        "item_title_compacts": item_title_compacts,
        "item_types": item_types,
        "item_count": len(request.items),
        "followers_count": request.followers_count,
        "engagement_score": request.engagement_score,
        "engagement_score_version": request.engagement_score_version or "",
        "vote_count": request.vote_count,
        "net_vote_score": request.net_vote_score,
        "super_up_count": request.super_up_count,
        "created_at": created_at,
        "discovery_updated_at": request.discovery_updated_at or now,
        "modified_at": now,
        "_source": "mediacircle_projection",
    }


def public_list_index_schema() -> tuple[Field, ...]:
    """RediSearch schema for ``idx:public_lists`` over ``public_list:`` JSON docs."""
    return (
        # Primary text search fields
        TextField("$.search_title", as_name="search_title", weight=5.0, no_stem=True),
        TextField("$.title_compact", as_name="title_compact", weight=1.0, no_stem=True),
        TextField("$.description_text", as_name="description_text", weight=2.0),
        TextField("$.topic_summary", as_name="topic_summary", weight=2.0),
        TextField("$.item_titles_text", as_name="item_titles_text", weight=1.0, no_stem=True),
        TextField("$.owner_display_name", as_name="owner_display_name", weight=1.0),
        # Exact-match tags
        TagField("$.list_id", as_name="list_id"),
        TagField("$.owner_id", as_name="owner_id"),
        TagField("$.owner_username", as_name="owner_username"),
        TagField("$.is_mediacircle_owner", as_name="is_mediacircle_owner"),
        TagField("$.circle_type", as_name="circle_type"),
        TagField("$.visibility", as_name="visibility"),
        TagField("$.circle_mode", as_name="circle_mode"),
        TagField("$.topic_keywords[*]", as_name="topic_keywords"),
        TagField("$.topic_labels[*]", as_name="topic_labels"),
        TagField("$.item_mc_ids[*]", as_name="item_mc_ids"),
        TagField("$.item_title_compacts[*]", as_name="item_title_compacts"),
        TagField("$.item_types[*]", as_name="item_types"),
        # Sortable ranking metrics
        NumericField("$.followers_count", as_name="followers_count", sortable=True),
        NumericField("$.engagement_score", as_name="engagement_score", sortable=True),
        NumericField("$.vote_count", as_name="vote_count", sortable=True),
        NumericField("$.net_vote_score", as_name="net_vote_score", sortable=True),
        NumericField("$.super_up_count", as_name="super_up_count", sortable=True),
        NumericField("$.item_count", as_name="item_count", sortable=True),
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField(
            "$.discovery_updated_at", as_name="discovery_updated_at", sortable=True
        ),
        NumericField("$.modified_at", as_name="modified_at", sortable=True),
    )
