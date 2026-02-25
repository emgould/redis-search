#!/usr/bin/env python3
"""
Reconcile TMDB watch providers against provider_map.json.

Can be run standalone (reads master_providers.json) or imported and called
with a live provider list from the TMDB API.

Migrates the old flat aggregator_ids/aggregators schema to:
  - packages: tier/variant offerings of the same provider
  - channels: content distributed through another platform's storefront

Two-pass strategy for new entries:
  Pass 1 — Standalone providers (no channel suffix) added first.
  Pass 2 — Channel entries matched against all providers (including Pass 1 additions).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import NotRequired, TypedDict

DATA_DIR = Path(__file__).resolve().parent / "data"
MASTER_PATH = DATA_DIR / "master_providers.json"
MAP_PATH = DATA_DIR / "provider_map.json"

CHANNEL_SUFFIX_RE: list[re.Pattern[str]] = [
    re.compile(r"^(.+?)\s+Amazon\s+[Cc]hannel.*$"),
    re.compile(r"^(.+?)\s+Apple\s+TV?\s*[Cc]hannel.*$"),
    re.compile(r"^(.+?)\s+Roku\s+Premium\s+[Cc]hannel.*$"),
    re.compile(r"^(.+?)\s+Apple\s+Tv\s+[Cc]hannel.*$"),
    re.compile(r"^(.+?)\s+Am[zs]on\s+[Cc]hannel.*$"),
]


class SubEntry(TypedDict):
    id: int
    name: str
    logo_path: NotRequired[str | None]


class ProviderMapEntry(TypedDict):
    provider_id: int
    provider_name: str
    base_brand: str
    mkt_share_order: int
    logo_path: NotRequired[str | None]
    packages: list[SubEntry]
    channels: list[SubEntry]


class LegacyEntry(TypedDict):
    provider_id: int
    provider_name: str
    base_brand: str
    mkt_share_order: int
    aggregator_ids: list[int]
    aggregators: list[str]


class MasterEntry(TypedDict):
    provider_id: int
    provider_name: str
    logo_path: NotRequired[str | None]


class ReconcileReport(TypedDict):
    skipped: list[str]
    attached: list[str]
    added: list[str]
    total_entries: int


def _normalize(name: str) -> str:
    """Lowercase, strip non-alphanumeric (except spaces), collapse whitespace."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", name.lower())).strip()


def _brand_variants(name: str) -> list[str]:
    """
    Generate normalized variants of a brand name to improve matching.

    Handles "Plus" <-> "+", collapsed spacing ("CuriosityStream" -> "curiosity stream"),
    and other common TMDB naming quirks.
    """
    norm = _normalize(name)
    variants = [norm]

    if norm.endswith(" plus"):
        variants.append(norm[: -len(" plus")].rstrip())
    else:
        variants.append(norm + " plus")

    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    spaced_norm = _normalize(spaced)
    if spaced_norm != norm:
        variants.append(spaced_norm)

    return variants


def _is_channel_name(name: str) -> bool:
    """Return True if the name matches a known channel/storefront suffix pattern."""
    return any(pat.match(name.strip()) for pat in CHANNEL_SUFFIX_RE)


def _classify_aggregator(name: str) -> str:
    """Classify an existing aggregator name as 'channel' or 'package'."""
    return "channel" if _is_channel_name(name) else "package"


def _migrate_legacy_entry(legacy: LegacyEntry) -> ProviderMapEntry:
    """Convert an old-schema entry to the new packages/channels schema."""
    packages: list[SubEntry] = []
    channels: list[SubEntry] = []

    for agg_id, agg_name in zip(
        legacy.get("aggregator_ids", []),
        legacy.get("aggregators", []),
        strict=False,
    ):
        entry: SubEntry = {"id": agg_id, "name": agg_name}
        if _classify_aggregator(agg_name) == "channel":
            channels.append(entry)
        else:
            packages.append(entry)

    return {
        "provider_id": legacy["provider_id"],
        "provider_name": legacy["provider_name"],
        "base_brand": legacy["base_brand"],
        "mkt_share_order": legacy["mkt_share_order"],
        "packages": packages,
        "channels": channels,
    }


def _build_brand_index(provider_map: list[ProviderMapEntry]) -> dict[str, int]:
    """Map normalized brand variants -> index in provider_map."""
    index: dict[str, int] = {}
    for idx, entry in enumerate(provider_map):
        for field in ("base_brand", "provider_name"):
            val: str = entry[field]  # type: ignore[literal-required]
            for variant in _brand_variants(val):
                if variant and variant not in index:
                    index[variant] = idx
    return index


def _register_brand(
    entry: ProviderMapEntry,
    idx: int,
    brand_index: dict[str, int],
) -> None:
    """Add new brand variants into the index after a new entry is appended."""
    for field in ("base_brand", "provider_name"):
        val: str = entry[field]  # type: ignore[literal-required]
        for variant in _brand_variants(val):
            if variant and variant not in brand_index:
                brand_index[variant] = idx


def _extract_channel_prefix(name: str) -> str | None:
    """Return the brand prefix if name matches a known channel suffix pattern."""
    for pat in CHANNEL_SUFFIX_RE:
        m = pat.match(name.strip())
        if m:
            return m.group(1).strip()
    return None


def _find_parent_by_prefix(prefix: str, brand_index: dict[str, int]) -> int | None:
    """Match the prefix (and its variants) against the brand index."""
    for variant in _brand_variants(prefix):
        hit = brand_index.get(variant)
        if hit is not None:
            return hit
    return None


def _find_parent_by_containment(
    name: str,
    provider_map: list[ProviderMapEntry],
) -> int | None:
    """
    If the provider name starts with an existing base_brand
    (e.g. "Netflix Kids" starts with "Netflix"), return that parent's index.
    Longest match wins.
    """
    norm = _normalize(name)
    best_idx: int | None = None
    best_len = 0
    for idx, entry in enumerate(provider_map):
        brand_norm = _normalize(entry["base_brand"])
        if not brand_norm or len(brand_norm) < 3:
            continue
        if norm.startswith(brand_norm + " ") and len(brand_norm) > best_len:
            best_idx = idx
            best_len = len(brand_norm)
    return best_idx


def _collect_known_ids(provider_map: list[ProviderMapEntry]) -> set[int]:
    ids: set[int] = set()
    for entry in provider_map:
        ids.add(entry["provider_id"])
        ids.update(sub["id"] for sub in entry["packages"])
        ids.update(sub["id"] for sub in entry["channels"])
    return ids


def _enrich_provider_map_with_logos(
    provider_map: list[ProviderMapEntry],
    logo_map: dict[int, str],
) -> None:
    """Fill logo_path on entries and sub-entries from logo_map where missing."""
    for entry in provider_map:
        if not entry.get("logo_path") and logo_map.get(entry["provider_id"]):
            entry["logo_path"] = logo_map[entry["provider_id"]]
        for sub in entry["packages"] + entry["channels"]:
            if not sub.get("logo_path") and logo_map.get(sub["id"]):
                sub["logo_path"] = logo_map[sub["id"]]


def _try_attach(
    master_entry: MasterEntry,
    provider_map: list[ProviderMapEntry],
    brand_index: dict[str, int],
    known_ids: set[int],
    logo_map: dict[int, str],
) -> str | None:
    """
    Attempt to attach this entry as a package or channel of an existing provider.

    Returns a log line if successful, None otherwise.
    """
    pid = master_entry["provider_id"]
    pname = master_entry["provider_name"].strip()
    logo_path = master_entry.get("logo_path") or logo_map.get(pid)

    parent_idx: int | None = None

    prefix = _extract_channel_prefix(pname)
    if prefix is not None:
        parent_idx = _find_parent_by_prefix(prefix, brand_index)

    if parent_idx is None:
        parent_idx = _find_parent_by_containment(pname, provider_map)

    if parent_idx is not None:
        parent = provider_map[parent_idx]
        is_channel = _is_channel_name(pname)
        bucket = parent["channels"] if is_channel else parent["packages"]
        sub_entry: SubEntry = {"id": pid, "name": pname}
        if logo_path:
            sub_entry["logo_path"] = logo_path
        bucket.append(sub_entry)
        known_ids.add(pid)
        kind = "channel" if is_channel else "package"
        return f"  [{kind}] {pid:>5}  {pname}  →  {parent['base_brand']}"

    return None


def _is_new_schema(entry: dict) -> bool:  # type: ignore[type-arg]
    """Detect whether an entry uses the new packages/channels schema."""
    return "packages" in entry and "channels" in entry


def reconcile_provider_map(
    master_entries: list[MasterEntry],
    provider_map_path: Path = MAP_PATH,
) -> ReconcileReport:
    """
    Core reconciliation logic. Accepts a list of {provider_id, provider_name} dicts
    from any source (file or live API) and updates provider_map.json.

    Args:
        master_entries: List of dicts with at least 'provider_id' and 'provider_name'.
        provider_map_path: Path to provider_map.json to read and overwrite.

    Returns:
        ReconcileReport with skip/attach/add counts and log lines.
    """
    raw_map: list[dict] = json.loads(provider_map_path.read_text())  # type: ignore[type-arg]

    logo_map: dict[int, str] = {}
    for me in master_entries:
        lp = me.get("logo_path")
        if lp and isinstance(lp, str) and lp.strip():
            logo_map[me["provider_id"]] = lp.strip()

    if raw_map and _is_new_schema(raw_map[0]):
        provider_map: list[ProviderMapEntry] = raw_map  # type: ignore[assignment]
    else:
        provider_map = [_migrate_legacy_entry(e) for e in raw_map]  # type: ignore[arg-type]

    _enrich_provider_map_with_logos(provider_map, logo_map)
    known_ids = _collect_known_ids(provider_map)
    brand_index = _build_brand_index(provider_map)
    next_order = max((e["mkt_share_order"] for e in provider_map), default=0) + 1

    skipped: list[str] = []
    attached: list[str] = []
    added: list[str] = []

    channels: list[MasterEntry] = []
    standalones: list[MasterEntry] = []

    for master_entry in master_entries:
        pid = master_entry["provider_id"]
        pname = master_entry["provider_name"].strip()
        if pid in known_ids:
            skipped.append(f"  [skip] {pid:>5}  {pname}")
            continue
        if _is_channel_name(pname):
            channels.append(master_entry)
        else:
            standalones.append(master_entry)

    # --- Pass 1: standalone providers ---
    for entry in standalones:
        pid = entry["provider_id"]
        pname = entry["provider_name"].strip()
        if pid in known_ids:
            skipped.append(f"  [skip] {pid:>5}  {pname}")
            continue

        log = _try_attach(entry, provider_map, brand_index, known_ids, logo_map)
        if log:
            attached.append(log)
            continue

        logo_path = entry.get("logo_path") or logo_map.get(pid)
        new_entry: ProviderMapEntry = {
            "provider_id": pid,
            "provider_name": pname,
            "base_brand": pname,
            "mkt_share_order": next_order,
            "packages": [],
            "channels": [],
        }
        if logo_path:
            new_entry["logo_path"] = logo_path
        provider_map.append(new_entry)
        known_ids.add(pid)
        _register_brand(new_entry, len(provider_map) - 1, brand_index)
        added.append(f"  [new] {pid:>5}  {pname}  (order={next_order})")
        next_order += 1

    # --- Pass 2: channel entries ---
    unmatched_channels: list[MasterEntry] = []
    for entry in channels:
        pid = entry["provider_id"]
        pname = entry["provider_name"].strip()
        if pid in known_ids:
            skipped.append(f"  [skip] {pid:>5}  {pname}")
            continue

        log = _try_attach(entry, provider_map, brand_index, known_ids, logo_map)
        if log:
            attached.append(log)
        else:
            unmatched_channels.append(entry)

    for entry in unmatched_channels:
        pid = entry["provider_id"]
        pname = entry["provider_name"].strip()
        if pid in known_ids:
            continue
        logo_path = entry.get("logo_path") or logo_map.get(pid)
        new_entry = {
            "provider_id": pid,
            "provider_name": pname,
            "base_brand": pname,
            "mkt_share_order": next_order,
            "packages": [],
            "channels": [],
        }
        if logo_path:
            new_entry["logo_path"] = logo_path
        provider_map.append(new_entry)
        known_ids.add(pid)
        _register_brand(new_entry, len(provider_map) - 1, brand_index)
        added.append(f"  [new] {pid:>5}  {pname}  (order={next_order})")
        next_order += 1

    _enrich_provider_map_with_logos(provider_map, logo_map)
    provider_map_path.write_text(json.dumps(provider_map, indent=4, ensure_ascii=False) + "\n")

    return {
        "skipped": skipped,
        "attached": attached,
        "added": added,
        "total_entries": len(provider_map),
    }


def _print_report(report: ReconcileReport) -> None:
    sep = "=" * 60
    print(f"\n{sep}")
    print("Provider Map Reconciliation Report")
    print(sep)
    print(f"\nSkipped (already known): {len(report['skipped'])}")
    for line in report["skipped"]:
        print(line)
    print(f"\nAttached (package or channel): {len(report['attached'])}")
    for line in report["attached"]:
        print(line)
    print(f"\nAdded as new provider: {len(report['added'])}")
    for line in report["added"]:
        print(line)
    print(f"\n{sep}")
    print(f"Total provider_map entries: {report['total_entries']}")
    print(f"{sep}\n")


def run() -> None:
    """Standalone entry point: reads master_providers.json and reconciles."""
    master_data: dict[str, list[MasterEntry]] = json.loads(MASTER_PATH.read_text())
    report = reconcile_provider_map(master_data["results"])
    _print_report(report)
    print(f"Written to {MAP_PATH}")


if __name__ == "__main__":
    run()
