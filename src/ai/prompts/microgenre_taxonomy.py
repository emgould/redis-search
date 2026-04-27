from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast

from pydantic import BaseModel

JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonDict = dict[str, JsonValue]

_PACKAGED_PATH = Path(__file__).resolve().parent / "taste-profile-taxonomy.json"
_REPO_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "microgenre-classifications"
    / "taste-profile-taxonomy.json"
)
TAXONOMY_PATH = _PACKAGED_PATH if _PACKAGED_PATH.exists() else _REPO_PATH
EXAMPLE_PRIMARY_IDS = (
    "comedy.sitcom.multicamera",
    "comedy.sitcom.mockumentary",
    "comedy.dark.social_satire",
)


@dataclass(frozen=True)
class LeafMicroGenre:
    """Canonical leaf label from the taste-profile taxonomy."""

    id: str
    name: str
    description: str
    path: str
    top_level_id: str
    top_level_name: str


class TaxonomyData(BaseModel):
    """Runtime taxonomy artifacts used by the prompt and parser."""

    name: str
    version: str
    created: str
    taxonomy_hash: str
    leaves: list[LeafMicroGenre]
    leaf_index: dict[str, LeafMicroGenre]
    taxonomy_block: str

    model_config = {"arbitrary_types_allowed": True}


def load_taxonomy() -> TaxonomyData:
    """Load the taste-profile taxonomy and flatten canonical leaf labels."""
    raw = TAXONOMY_PATH.read_text(encoding="utf-8")
    parsed = cast(JsonDict, json.loads(raw))
    taxonomy_hash = sha256(
        json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    leaves: list[LeafMicroGenre] = []
    seen_ids: set[str] = set()
    genres = _get_object_list(parsed, "genres")
    if not genres:
        raise ValueError("Taste profile taxonomy has no top-level genres.")

    for genre in genres:
        genre_id = _require_str(genre, "id")
        genre_name = _require_str(genre, "name")
        _collect_leaves(
            node=genre,
            path=[genre_name],
            leaves=leaves,
            seen_ids=seen_ids,
            top_level_id=genre_id,
            top_level_name=genre_name,
        )

    if not leaves:
        raise ValueError("Taste profile taxonomy has no leaf micro-genres.")

    leaf_index = {leaf.id: leaf for leaf in leaves}
    missing_examples = [label_id for label_id in EXAMPLE_PRIMARY_IDS if label_id not in leaf_index]
    if missing_examples:
        raise ValueError(f"Prompt examples reference unknown micro-genre ids: {missing_examples}")

    return TaxonomyData(
        name=_require_str(parsed, "name"),
        version=_require_str(parsed, "version"),
        created=_require_str(parsed, "created"),
        taxonomy_hash=taxonomy_hash,
        leaves=leaves,
        leaf_index=leaf_index,
        taxonomy_block=_format_taxonomy_block(leaves),
    )


def _collect_leaves(
    *,
    node: JsonDict,
    path: list[str],
    leaves: list[LeafMicroGenre],
    seen_ids: set[str],
    top_level_id: str,
    top_level_name: str,
) -> None:
    children = _get_object_list(node, "subgenres")
    if children:
        for child in children:
            _collect_leaves(
                node=child,
                path=[*path, _require_str(child, "name")],
                leaves=leaves,
                seen_ids=seen_ids,
                top_level_id=top_level_id,
                top_level_name=top_level_name,
            )
        return

    label_id = _require_str(node, "id")
    if label_id in seen_ids:
        raise ValueError(f"Duplicate taste-profile taxonomy leaf id: {label_id}")
    seen_ids.add(label_id)

    leaves.append(
        LeafMicroGenre(
            id=label_id,
            name=_require_str(node, "name"),
            description=_require_str(node, "description"),
            path=" / ".join(path),
            top_level_id=top_level_id,
            top_level_name=top_level_name,
        )
    )


def _get_object_list(data: JsonDict, key: str) -> list[JsonDict]:
    value = data.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _require_str(data: JsonDict, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Taste profile taxonomy node is missing string field: {key}")
    return value.strip()


def _format_taxonomy_block(leaves: list[LeafMicroGenre]) -> str:
    sections: list[str] = []
    current_top_level = ""
    for leaf in leaves:
        if leaf.top_level_id != current_top_level:
            current_top_level = leaf.top_level_id
            sections.append(f"\n{leaf.top_level_name}:")
        sections.append(f"- {leaf.id} | {leaf.name} - {leaf.description}")
    return "\n".join(sections).strip()


TAXONOMY = load_taxonomy()
LEAF_INDEX = TAXONOMY.leaf_index
LEAF_IDS = tuple(LEAF_INDEX)
TAXONOMY_BLOCK = TAXONOMY.taxonomy_block
