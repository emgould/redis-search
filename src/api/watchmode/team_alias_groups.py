"""League-scoped grouping for near-duplicate Watchmode team records.

The raw roster stays cached. This module decides which records are one club,
which name is shown, and when schedule evidence can narrow the Watchmode IDs.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal

CLUB_TOKENS = frozenset({"fc", "cf", "sc", "afc", "cfc"})
PLACEHOLDER_KEYS = frozenset({"tbd", "tba", "unknown"})
AliasStatus = Literal["unresolved", "resolved", "ambiguous"]


def mapping_records(raw_teams: object) -> list[dict[str, object]]:
    """Copy roster rows into a typed mapping list. Logos are removed later."""
    if not isinstance(raw_teams, list):
        return []
    records: list[dict[str, object]] = []
    for raw in raw_teams:
        if not isinstance(raw, dict):
            continue
        records.append({str(key): value for key, value in raw.items()})
    return records


def positive_int(value: object) -> int | None:
    """Return a Watchmode id, rejecting bools because bool is a subclass of int."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


def normalized_team_key(name: object) -> str | None:
    """Collapse punctuation and trailing club tokens so alias names share a key.

    Punctuation is removed rather than turned into a word boundary, so
    "D.C. United" and "DC United" match. Words such as "city" and "united"
    stay in the key, so Manchester City and Manchester United do not.
    """
    if not isinstance(name, str) or not name.strip():
        return None
    folded = name.casefold().replace("&", " and ")
    chars: list[str] = []
    for char in folded:
        if char.isalnum():
            chars.append(char)
        elif char.isspace():
            chars.append(" ")
    tokens = "".join(chars).split()
    while tokens and tokens[0] in CLUB_TOKENS:
        tokens.pop(0)
    while tokens and tokens[-1] in CLUB_TOKENS:
        tokens.pop()
    key = " ".join(tokens)
    if not key or key in PLACEHOLDER_KEYS:
        return None
    return key


@dataclass(frozen=True)
class TeamAliasGroup:
    """One real-world club and the Watchmode ids currently attached to it."""

    league_id: int
    normalized_key: str
    display_name: str
    display_team_id: int
    candidate_ids: tuple[int, ...]
    status: AliasStatus = "unresolved"
    resolved_team_id: int | None = None

    @property
    def group_id(self) -> str:
        return f"{self.league_id}_{self.normalized_key.replace(' ', '_')}"

    def catalog_team_id(self) -> int:
        """Id the client stores. Resolved schedule ids replace the display id."""
        if (
            self.status == "resolved"
            and self.resolved_team_id is not None
            and self.resolved_team_id in self.candidate_ids
        ):
            return self.resolved_team_id
        return self.display_team_id


def parse_team_league_hints(raw: str) -> dict[int, int]:
    """Parse `teamId:leagueId` pairs. These are lookup hints, not event filters."""
    hints: dict[int, int] = {}
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) > 100:
        raise ValueError("team_league_hints accepts at most 100 entries")
    for part in parts:
        team_text, separator, league_text = part.partition(":")
        if separator != ":" or not team_text or not league_text:
            raise ValueError("team_league_hints entries must be teamId:leagueId")
        team_id = int(team_text)
        league_id = int(league_text)
        if team_id < 1 or league_id < 1:
            raise ValueError("team_league_hints ids must be positive")
        hints[team_id] = league_id
    return hints


def _clean_team(raw: Mapping[str, object], league_id: int) -> dict[str, object] | None:
    team_id = positive_int(raw.get("id"))
    name = raw.get("name")
    if team_id is None or not isinstance(name, str) or not name.strip():
        return None
    cleaned = {key: value for key, value in raw.items() if "logo" not in key.lower()}
    record_league = positive_int(cleaned.get("league_id"))
    cleaned["id"] = team_id
    cleaned["name"] = name.strip()
    cleaned["league_id"] = record_league or league_id
    return cleaned


def _display_sort_key(team: Mapping[str, object]) -> tuple[int, int, int]:
    name = str(team.get("name") or "")
    short_name = team.get("short_name")
    has_short_name = 0 if isinstance(short_name, str) and short_name.strip() else 1
    team_id = positive_int(team.get("id")) or 0
    return (-len(name), has_short_name, team_id)


def build_alias_groups(
    teams: Sequence[Mapping[str, object]], league_id: int
) -> list[TeamAliasGroup]:
    """Return only groups of two or more ids. Singletons are not aliases."""
    grouped: dict[str, list[dict[str, object]]] = {}
    for raw in teams:
        cleaned = _clean_team(raw, league_id)
        if cleaned is None:
            continue
        key = normalized_team_key(cleaned.get("name"))
        if key is None:
            continue
        grouped.setdefault(key, []).append(cleaned)

    groups: list[TeamAliasGroup] = []
    for key, records in grouped.items():
        by_id: dict[int, dict[str, object]] = {}
        for record in records:
            team_id = positive_int(record.get("id"))
            if team_id is not None:
                by_id.setdefault(team_id, record)
        if len(by_id) < 2:
            continue
        chosen = min(by_id.values(), key=_display_sort_key)
        chosen_id = positive_int(chosen.get("id"))
        if chosen_id is None:
            continue
        groups.append(
            TeamAliasGroup(
                league_id=league_id,
                normalized_key=key,
                display_name=str(chosen.get("name") or ""),
                display_team_id=chosen_id,
                candidate_ids=tuple(sorted(by_id)),
            )
        )
    return groups


def merge_stored_group(fresh: TeamAliasGroup, stored: TeamAliasGroup | None) -> TeamAliasGroup:
    """Keep a prior resolution when the same candidate ids are still present."""
    if stored is None or stored.group_id != fresh.group_id:
        return fresh
    status: AliasStatus = stored.status
    resolved_team_id = stored.resolved_team_id
    if status == "resolved" and resolved_team_id not in fresh.candidate_ids:
        status = "unresolved"
        resolved_team_id = None
    if status == "ambiguous":
        overlap = set(stored.candidate_ids).intersection(fresh.candidate_ids)
        if len(overlap) < 2:
            status = "unresolved"
            resolved_team_id = None
    if status == "unresolved":
        resolved_team_id = None
    return replace(
        fresh,
        status=status,
        resolved_team_id=resolved_team_id,
    )


def apply_observation(group: TeamAliasGroup, observed_team_ids: set[int]) -> TeamAliasGroup:
    """Narrow a group from participant ids already present in loaded snapshots.

    An empty observation is an off day and does not collapse the group.
    A resolved group reopens to ambiguous when another candidate appears.
    It does not switch the winner directly from one id to another.
    """
    relevant = observed_team_ids.intersection(group.candidate_ids)
    if not relevant or group.status == "ambiguous":
        return group
    if group.status == "resolved":
        if group.resolved_team_id is not None and relevant - {group.resolved_team_id}:
            return replace(group, status="ambiguous", resolved_team_id=None)
        return group
    if len(relevant) == 1:
        return replace(group, status="resolved", resolved_team_id=next(iter(relevant)))
    return replace(group, status="ambiguous", resolved_team_id=None)


def participant_ids(events: list[Mapping[str, object]]) -> set[int]:
    """Collect home and away ids from schedule snapshots already in memory."""
    found: set[int] = set()
    for event in events:
        for side in ("home_team", "away_team"):
            participant = event.get(side)
            if not isinstance(participant, Mapping):
                continue
            team_id = positive_int(participant.get("id"))
            if team_id is not None:
                found.add(team_id)
    return found


def expand_team_ids(requested: set[int], groups_by_team: dict[int, TeamAliasGroup]) -> set[int]:
    """Match every unresolved candidate, or only the resolved schedule id."""
    if not requested:
        return set()
    expanded: set[int] = set()
    for team_id in requested:
        group = groups_by_team.get(team_id)
        if group is None:
            expanded.add(team_id)
            continue
        resolved = group.resolved_team_id
        if group.status == "resolved" and resolved is not None and resolved in group.candidate_ids:
            expanded.add(resolved)
            continue
        expanded.update(group.candidate_ids)
    return expanded


def alias_maps(groups: list[TeamAliasGroup]) -> tuple[dict[str, str], dict[str, str]]:
    """Map every candidate id to the catalog id, plus that id's display name."""
    aliases: dict[str, str] = {}
    names: dict[str, str] = {}
    for group in groups:
        canonical = str(group.catalog_team_id())
        names[canonical] = group.display_name
        for team_id in group.candidate_ids:
            aliases[str(team_id)] = canonical
    return aliases, names


def project_team_rows(
    teams: Sequence[Mapping[str, object]],
    league_id: int,
    groups: list[TeamAliasGroup],
) -> list[dict[str, object]]:
    """One row per club. The label is the longest name even after id resolution."""
    cleaned: list[dict[str, object]] = []
    by_id: dict[int, dict[str, object]] = {}
    for raw in teams:
        record = _clean_team(raw, league_id)
        if record is None:
            continue
        team_id = positive_int(record.get("id"))
        if team_id is None:
            continue
        by_id.setdefault(team_id, record)
        cleaned.append(record)

    group_by_team = {team_id: group for group in groups for team_id in group.candidate_ids}
    rows: list[dict[str, object]] = []
    seen_groups: set[str] = set()
    for record in cleaned:
        team_id = positive_int(record.get("id"))
        group = group_by_team.get(team_id or -1)
        if group is None:
            rows.append(record)
            continue
        if group.group_id in seen_groups:
            continue
        seen_groups.add(group.group_id)
        source = by_id.get(group.display_team_id, record)
        row = dict(source)
        row["id"] = group.catalog_team_id()
        row["name"] = group.display_name
        row["league_id"] = league_id
        row["alias_team_ids"] = list(group.candidate_ids)
        rows.append(row)
    return rows
