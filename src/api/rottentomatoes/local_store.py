"""Local on-disk lookup cache for crawled Rotten Tomatoes content data.

Migrated from media-manager with the addition of a vanity index for
direct rt_id-based lookups (O(1) dict access by RT URL slug).
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RT_DATA_DIR = PROJECT_ROOT / "data" / "rt"
CONTENT_INDEX_FILE = RT_DATA_DIR / "content_index.json"
CONTENT_JSONL_FILE = RT_DATA_DIR / "content_all.jsonl"
DYNAMIC_INSERTIONS_FILE = RT_DATA_DIR / "dynamic" / "content_insertions.json"

LookupRecord = dict[str, Any]


def _normalize_title(title: str | None) -> str:
    """Normalize title text for case-insensitive dictionary lookups."""
    if not title:
        return ""
    return " ".join(title.strip().lower().split())


def _to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _extract_cast_names(hit: dict[str, Any]) -> list[str]:
    """Extract cast names from multiple hit formats."""
    cast_names: list[str] = []
    cast_list = hit.get("cast_names")
    if isinstance(cast_list, list):
        for value in cast_list:
            if isinstance(value, str) and value.strip():
                cast_names.append(value.strip())
        return cast_names

    cast = hit.get("cast")
    if isinstance(cast, list):
        for item in cast:
            if isinstance(item, str) and item.strip():
                cast_names.append(item.strip())
            elif isinstance(item, dict):
                cast_name = item.get("name")
                if isinstance(cast_name, str) and cast_name.strip():
                    cast_names.append(cast_name.strip())

    cast_crew = hit.get("castCrew")
    if isinstance(cast_crew, dict):
        from_crew = cast_crew.get("cast")
        if isinstance(from_crew, list):
            for value in from_crew:
                if isinstance(value, str) and value.strip():
                    cast_names.append(value.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for cast_name in cast_names:
        cast_lower = cast_name.lower()
        if cast_lower not in seen:
            deduped.append(cast_name)
            seen.add(cast_lower)
    return deduped


def _extract_aliases(hit: dict[str, Any]) -> list[str]:
    """Collect title aliases for local index keys."""
    aliases: list[str] = []
    raw_title = hit.get("title") or hit.get("name")
    if isinstance(raw_title, str):
        aliases.append(raw_title)

    for key in ("titles", "aka"):
        values = hit.get(key)
        if isinstance(values, list):
            for value in values:
                if isinstance(value, str):
                    aliases.append(value)

    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        norm = _normalize_title(alias)
        if norm and norm not in seen:
            deduped.append(norm)
            seen.add(norm)
    return deduped


def _extract_genres(hit: dict[str, Any]) -> list[str]:
    """Normalize genre list values."""
    values = hit.get("genres")
    if isinstance(values, list):
        genres: list[str] = []
        for value in values:
            if isinstance(value, str):
                normalized = value.strip()
                if normalized:
                    genres.append(normalized)
        return genres
    return []


def _extract_director(hit: dict[str, Any]) -> str | None:
    """Extract director from castCrew.crew.Director if present."""
    cast_crew = hit.get("castCrew")
    if isinstance(cast_crew, dict):
        crew = cast_crew.get("crew")
        if isinstance(crew, dict):
            directors = crew.get("Director")
            if isinstance(directors, list):
                for director in directors:
                    if isinstance(director, str):
                        normalized = director.strip()
                        if normalized:
                            return normalized

    director_value = hit.get("director")
    if isinstance(director_value, str):
        director_value = director_value.strip()
        if director_value:
            return director_value
    return None


def _to_lookup_record(hit: dict[str, Any]) -> LookupRecord | None:
    """Normalize a crawler hit into a compact local lookup record."""
    object_id = hit.get("objectID")
    if not isinstance(object_id, str) or not object_id.strip():
        rt_id = hit.get("rtId")
        if isinstance(rt_id, int | str):
            object_id = f"rt-{rt_id}"
        else:
            return None
    object_id = str(object_id)

    rotten = hit.get("rottenTomatoes")
    if not isinstance(rotten, dict):
        rotten = {}

    release_year = hit.get("releaseYear")
    if release_year is None:
        release_year = hit.get("release_year")

    critics_score = hit.get("critics_score")
    if critics_score is None:
        critics_score = rotten.get("criticsScore")

    audience_score = hit.get("audience_score")
    if audience_score is None:
        audience_score = rotten.get("audienceScore")

    if critics_score is None and audience_score is None:
        return None

    content_type = hit.get("type")
    if isinstance(content_type, str):
        content_type = content_type.lower()

    tms_id = hit.get("tmsId") or hit.get("tms_id")

    return {
        "objectID": object_id,
        "title": hit.get("title"),
        "type": content_type,
        "release_year": _to_int(release_year),
        "vanity": hit.get("vanity"),
        "description": hit.get("description"),
        "genres": _extract_genres(hit),
        "runtime": _to_int(
            hit.get("runtime") if hit.get("runtime") is not None else hit.get("runTime")
        ),
        "critics_score": critics_score,
        "audience_score": audience_score,
        "director": _extract_director(hit),
        "verified_hot": rotten.get("verifiedHot"),
        "score_sentiment": rotten.get("scoreSentiment"),
        "cast_names": _extract_cast_names(hit),
        "tms_id": str(tms_id) if tms_id else None,
    }


def _write_index(
    index: dict[str, set[str]],
    records: dict[str, LookupRecord],
    path: Path,
) -> None:
    payload = {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "records": records,
        "title_index": {key: sorted(value) for key, value in index.items()},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _append_dynamic_insertion_hits(hits: list[dict[str, Any]]) -> int:
    """Append raw hits to the dynamic insertion log."""
    if not hits:
        return 0

    DYNAMIC_INSERTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    appended = 0
    try:
        with DYNAMIC_INSERTIONS_FILE.open("a") as handle:
            for hit in hits:
                handle.write(json.dumps(hit))
                handle.write("\n")
                appended += 1
    except (OSError, TypeError, ValueError) as exc:
        logger.warning(
            "Failed to append dynamic insertions to %s: %s",
            DYNAMIC_INSERTIONS_FILE, exc,
        )
        return 0
    return appended


def _load_dynamic_insertions(store: "RTContentLookupStore") -> int:
    """Load raw dynamic insertions (JSONL) into the provided store."""
    if not DYNAMIC_INSERTIONS_FILE.exists():
        return 0

    loaded = 0
    try:
        with DYNAMIC_INSERTIONS_FILE.open() as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    hit = json.loads(line)
                except ValueError:
                    logger.warning(
                        "Skipping invalid JSON line in %s",
                        DYNAMIC_INSERTIONS_FILE,
                    )
                    continue
                if not isinstance(hit, dict):
                    continue
                store.add_hit(hit)
                loaded += 1
    except OSError as exc:
        logger.warning(
            "Failed to read dynamic insertions from %s: %s",
            DYNAMIC_INSERTIONS_FILE, exc,
        )
        return 0
    return loaded


class RTContentLookupStore:
    """Minimal local index for fast title and vanity lookups."""

    def __init__(self, index_path: Path = CONTENT_INDEX_FILE) -> None:
        self.index_path = index_path
        self._records: dict[str, LookupRecord] = {}
        self._title_index: dict[str, set[str]] = {}
        self._vanity_index: dict[str, str] = {}
        self._loaded = False

    def _load_if_needed(self) -> None:
        if self._loaded:
            return

        self._loaded = True
        if not self.index_path.exists():
            return

        try:
            raw = self.index_path.read_text()
            data = json.loads(raw)
        except (OSError, ValueError) as exc:
            logger.warning("Failed to read index file %s: %s", self.index_path, exc)
            return
        if not isinstance(data, dict):
            logger.warning("Ignoring invalid index format in %s", self.index_path)
            return

        raw_records = data.get("records", {})
        if isinstance(raw_records, dict):
            for object_id, payload in raw_records.items():
                if isinstance(object_id, str) and isinstance(payload, dict):
                    self._records[object_id] = payload
                    vanity = payload.get("vanity")
                    if isinstance(vanity, str) and vanity:
                        self._vanity_index[vanity] = object_id

        raw_title_index = data.get("title_index", {})
        if isinstance(raw_title_index, dict):
            for key, value in raw_title_index.items():
                if not isinstance(key, str):
                    continue
                values: set[str] = set()
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            values.add(item)
                self._title_index[key] = values

        _load_dynamic_insertions(self)

    def add_hit(self, hit: dict[str, Any]) -> bool:
        """Add/update a single hit. Returns True when index changes."""
        self._load_if_needed()
        record = _to_lookup_record(hit)
        if record is None:
            return False

        object_id = record["objectID"]
        if object_id not in self._records:
            self._records[object_id] = record
        else:
            existing = self._records[object_id].copy()
            existing.update(record)
            if existing == self._records[object_id]:
                return False
            self._records[object_id] = existing

        vanity = record.get("vanity")
        if isinstance(vanity, str) and vanity:
            self._vanity_index[vanity] = object_id

        aliases = _extract_aliases(hit)
        for alias in aliases:
            values = self._title_index.setdefault(alias, set())
            if object_id in values:
                continue
            values.add(object_id)
        return True

    def add_hits(self, hits: list[dict[str, Any]]) -> int:
        """Add or update multiple hits and return number of changed records."""
        self._load_if_needed()
        changes = 0
        for hit in hits:
            if self.add_hit(hit):
                changes += 1
        return changes

    def lookup(
        self,
        title: str,
        year: int | None = None,
        star: str | None = None,
    ) -> list[LookupRecord]:
        """Lookup matching records by normalized title."""
        self._load_if_needed()
        normalized = _normalize_title(title)
        if not normalized:
            return []

        ids = self._title_index.get(normalized, set())
        if not ids:
            return []

        star_normalized = _normalize_title(star) if star else None
        results: list[LookupRecord] = []
        for object_id in ids:
            hit = self._records.get(object_id)
            if not hit:
                continue

            if year is not None and hit.get("release_year") != year:
                continue

            if star_normalized:
                cast_names = hit.get("cast_names", [])
                if not isinstance(cast_names, list):
                    continue
                match_star = False
                for cast_name in cast_names:
                    if not isinstance(cast_name, str):
                        continue
                    if star_normalized in _normalize_title(cast_name):
                        match_star = True
                        break
                if not match_star:
                    continue

            results.append(hit)

        return results

    def lookup_by_vanity(self, vanity: str) -> LookupRecord | None:
        """O(1) lookup by RT vanity slug (the URL path component)."""
        self._load_if_needed()
        object_id = self._vanity_index.get(vanity)
        if object_id is None:
            return None
        return self._records.get(object_id)

    def persist(self) -> None:
        """Persist index to disk."""
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        _write_index(self._title_index, self._records, self.index_path)

    @property
    def size(self) -> int:
        """Return number of unique records in memory."""
        return len(self._records)


_STORE: RTContentLookupStore | None = None


def get_store() -> RTContentLookupStore:
    """Return a singleton instance of the local lookup store."""
    global _STORE  # noqa: PLW0603
    if _STORE is None:
        _STORE = RTContentLookupStore()
    return _STORE


def reset_store() -> None:
    """Reset cached store singleton (useful after replacing local file on disk)."""
    global _STORE  # noqa: PLW0603
    _STORE = None


def lookup_title(
    title: str,
    year: int | None = None,
    star: str | None = None,
) -> list[LookupRecord]:
    """Lookup title candidates from local RT index."""
    return get_store().lookup(title=title, year=year, star=star)


def add_hits_to_cache(hits: list[dict[str, Any]]) -> int:
    """Add content hits to the local index and persist."""
    store = get_store()
    changed = store.add_hits(hits)
    appended = _append_dynamic_insertion_hits(hits)
    if changed:
        store.persist()
        logger.info(
            "RT cache index persisted from dynamic hits: changed=%d total_records=%d",
            changed, store.size,
        )
    if appended:
        logger.debug("RT dynamic insertion log appended: %d rows", appended)
    if not changed and not appended:
        logger.debug(
            "RT dynamic cache write skipped: no record changes and "
            "no dynamic rows appended (input_size=%d)",
            len(hits),
        )
    if not changed and appended:
        logger.debug(
            "Dynamic insertion log updated with %d hits, but index "
            "had no effective changes",
            appended,
        )
    return changed


def build_content_index(jsonl_path: Path = CONTENT_JSONL_FILE) -> int:
    """Build local index from a deduplicated JSONL crawl output file."""
    if not jsonl_path.exists():
        logger.info(
            "Missing consolidated output %s, falling back to prefix JSON files",
            jsonl_path,
        )
        return _build_content_index_from_prefix_files()

    store = RTContentLookupStore()
    with jsonl_path.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                hit = json.loads(line)
                if not isinstance(hit, dict):
                    continue
            except ValueError:
                continue
            store.add_hit(hit)

    dynamic_loaded = _load_dynamic_insertions(store)
    if dynamic_loaded:
        logger.info(
            "Loaded %d dynamic insertions from %s",
            dynamic_loaded, DYNAMIC_INSERTIONS_FILE,
        )
    store.persist()
    logger.info("Wrote %d records to %s", store.size, CONTENT_INDEX_FILE)
    return store.size


def _build_content_index_from_prefix_files() -> int:
    """Build local index from raw per-prefix JSON files."""
    prefix_files = sorted(CONTENT_JSONL_FILE.parent.rglob("content_*.json"))
    if not prefix_files:
        logger.error(
            "No prefix JSON files found under %s", CONTENT_JSONL_FILE.parent,
        )
        return 0

    store = RTContentLookupStore()
    for path in prefix_files:
        try:
            payload = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            logger.warning("Failed to parse %s: %s", path, exc)
            continue
        if not isinstance(payload, list):
            logger.warning("Ignoring non-list payload in %s", path)
            continue

        for hit in payload:
            if not isinstance(hit, dict):
                continue
            store.add_hit(hit)

    dynamic_loaded = _load_dynamic_insertions(store)
    if dynamic_loaded:
        logger.info(
            "Loaded %d dynamic insertions from %s",
            dynamic_loaded, DYNAMIC_INSERTIONS_FILE,
        )
    store.persist()
    logger.info(
        "Wrote %d records to %s from %d files",
        store.size, CONTENT_INDEX_FILE, len(prefix_files),
    )
    return store.size
