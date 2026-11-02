"""Persistence for Watchmode team-alias groups.

Firestore is the shared registry. Tests and processes without an initialized
Firebase app use the in-memory store. Neither path calls Watchmode.
"""

import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Protocol

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore_v1.transaction import Transaction

from api.watchmode.team_alias_groups import (
    AliasStatus,
    TeamAliasGroup,
    alias_maps,
    apply_observation,
    build_alias_groups,
    expand_team_ids,
    merge_stored_group,
    participant_ids,
    project_team_rows,
)

ALIAS_COLLECTION = "watchmodeTeamAliases"
MEMBER_COLLECTION = "watchmodeTeamAliasMembers"


class TeamAliasStore(Protocol):
    """Read and update alias groups without exposing the backing database."""

    def load(self, team_ids: set[int]) -> dict[int, TeamAliasGroup]:
        """Return groups for the requested ids. Missing ids are absent."""

    def save_if_changed(
        self, groups: list[TeamAliasGroup], existing: dict[int, TeamAliasGroup]
    ) -> None:
        """Write a group only when its signature differs from the stored one."""

    def apply_evidence(self, group: TeamAliasGroup, observed_team_ids: set[int]) -> TeamAliasGroup:
        """Re-read the group and commit one schedule observation."""


class EventAliasResolution:
    """Expanded team filters and the canonical ids the client should store."""

    def __init__(
        self,
        team_ids: set[int],
        followed_team_ids: set[int],
        team_aliases: dict[str, str],
        team_alias_names: dict[str, str],
    ) -> None:
        self.team_ids = team_ids
        self.followed_team_ids = followed_team_ids
        self.team_aliases = team_aliases
        self.team_alias_names = team_alias_names


def _signature(group: TeamAliasGroup) -> tuple[object, ...]:
    return (
        group.league_id,
        group.normalized_key,
        group.display_name,
        group.display_team_id,
        group.candidate_ids,
        group.status,
        group.resolved_team_id,
    )


def _group_payload(group: TeamAliasGroup) -> dict[str, object]:
    return {
        "leagueId": group.league_id,
        "normalizedKey": group.normalized_key,
        "displayName": group.display_name,
        "displayTeamId": group.display_team_id,
        "candidateIds": list(group.candidate_ids),
        "status": group.status,
        "resolvedTeamId": group.resolved_team_id,
        "updatedAt": datetime.now(UTC).isoformat(),
    }


def _alias_status(value: object) -> AliasStatus | None:
    if value == "unresolved":
        return "unresolved"
    if value == "resolved":
        return "resolved"
    if value == "ambiguous":
        return "ambiguous"
    return None


def _group_from_payload(group_id: str, payload: Mapping[str, object]) -> TeamAliasGroup | None:
    league_id = payload.get("leagueId")
    normalized_key = payload.get("normalizedKey")
    display_name = payload.get("displayName")
    display_team_id = payload.get("displayTeamId")
    candidate_ids = payload.get("candidateIds")
    status = _alias_status(payload.get("status"))
    resolved = payload.get("resolvedTeamId")
    if (
        status is None
        or not isinstance(league_id, int)
        or isinstance(league_id, bool)
        or not isinstance(normalized_key, str)
        or not isinstance(display_name, str)
        or not isinstance(display_team_id, int)
        or isinstance(display_team_id, bool)
        or not isinstance(candidate_ids, list)
    ):
        return None
    ids: list[int] = []
    for candidate in candidate_ids:
        if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate < 1:
            return None
        ids.append(candidate)
    if len(ids) < 2:
        return None
    resolved_team_id = resolved if isinstance(resolved, int) and not isinstance(resolved, bool) else None
    group = TeamAliasGroup(
        league_id=league_id,
        normalized_key=normalized_key,
        display_name=display_name,
        display_team_id=display_team_id,
        candidate_ids=tuple(sorted(ids)),
        status=status,
        resolved_team_id=resolved_team_id,
    )
    if group.group_id != group_id:
        return None
    return group


class MemoryTeamAliasStore:
    """Process-local registry used by tests and uninitialized runtimes."""

    def __init__(self) -> None:
        self._groups: dict[str, TeamAliasGroup] = {}
        self._members: dict[int, str] = {}

    def load(self, team_ids: set[int]) -> dict[int, TeamAliasGroup]:
        found: dict[int, TeamAliasGroup] = {}
        for team_id in team_ids:
            group_id = self._members.get(team_id)
            if group_id is None:
                continue
            group = self._groups.get(group_id)
            if group is not None:
                found[team_id] = group
        return found

    def save_if_changed(
        self, groups: list[TeamAliasGroup], existing: dict[int, TeamAliasGroup]
    ) -> None:
        del existing
        for group in groups:
            current = self._groups.get(group.group_id)
            if current is not None and _signature(current) == _signature(group):
                continue
            self._write(group)

    def apply_evidence(self, group: TeamAliasGroup, observed_team_ids: set[int]) -> TeamAliasGroup:
        current = self._groups.get(group.group_id, group)
        updated = apply_observation(current, observed_team_ids)
        if updated != current:
            self._write(updated)
        return updated

    def _write(self, group: TeamAliasGroup) -> None:
        previous = self._groups.get(group.group_id)
        if previous is not None:
            for team_id in previous.candidate_ids:
                if team_id not in group.candidate_ids and self._members.get(team_id) == group.group_id:
                    del self._members[team_id]
        self._groups[group.group_id] = group
        for team_id in group.candidate_ids:
            self._members[team_id] = group.group_id


class FirestoreTeamAliasStore:
    """Admin-SDK registry. Security rules deny client access to both collections."""

    def __init__(self) -> None:
        self._db = firestore.client()

    def load(self, team_ids: set[int]) -> dict[int, TeamAliasGroup]:
        if not team_ids:
            return {}
        member_refs = [
            self._db.collection(MEMBER_COLLECTION).document(str(team_id)) for team_id in team_ids
        ]
        group_ids: set[str] = set()
        for snapshot in self._db.get_all(member_refs):
            if not snapshot.exists:
                continue
            payload = snapshot.to_dict() or {}
            group_id = payload.get("groupId")
            if isinstance(group_id, str) and group_id:
                group_ids.add(group_id)
        if not group_ids:
            return {}
        groups = self._load_groups(group_ids)
        found: dict[int, TeamAliasGroup] = {}
        for team_id in team_ids:
            for group in groups.values():
                if team_id in group.candidate_ids:
                    found[team_id] = group
        return found

    def save_if_changed(
        self, groups: list[TeamAliasGroup], existing: dict[int, TeamAliasGroup]
    ) -> None:
        del existing
        for group in groups:
            self._save_group(group)

    def _save_group(self, group: TeamAliasGroup) -> None:
        """Merge onto the latest document so a resolution cannot be overwritten."""
        group_ref = self._db.collection(ALIAS_COLLECTION).document(group.group_id)
        transaction = self._db.transaction()

        @firestore.transactional
        def _save(txn: Transaction) -> None:
            snapshot = group_ref.get(transaction=txn)
            stored = (
                _group_from_payload(group.group_id, snapshot.to_dict() or {})
                if snapshot.exists
                else None
            )
            merged = merge_stored_group(group, stored)
            if stored is not None and _signature(stored) == _signature(merged):
                return
            txn.set(group_ref, _group_payload(merged))
            previous = set(stored.candidate_ids) if stored is not None else set()
            for team_id in merged.candidate_ids:
                txn.set(
                    self._db.collection(MEMBER_COLLECTION).document(str(team_id)),
                    {"groupId": merged.group_id, "leagueId": merged.league_id, "teamId": team_id},
                )
            for team_id in previous - set(merged.candidate_ids):
                txn.delete(self._db.collection(MEMBER_COLLECTION).document(str(team_id)))

        _save(transaction)

    def apply_evidence(self, group: TeamAliasGroup, observed_team_ids: set[int]) -> TeamAliasGroup:
        if not observed_team_ids.intersection(group.candidate_ids):
            stored = self.load(set(group.candidate_ids))
            return stored.get(group.candidate_ids[0], group)
        group_ref = self._db.collection(ALIAS_COLLECTION).document(group.group_id)
        transaction = self._db.transaction()

        @firestore.transactional
        def _commit_observation(txn: Transaction) -> TeamAliasGroup:
            snapshot = group_ref.get(transaction=txn)
            current = group
            if snapshot.exists:
                parsed = _group_from_payload(group.group_id, snapshot.to_dict() or {})
                if parsed is not None:
                    current = parsed
            updated = apply_observation(current, observed_team_ids)
            if updated != current:
                txn.set(group_ref, _group_payload(updated))
            return updated

        committed = _commit_observation(transaction)
        if isinstance(committed, TeamAliasGroup):
            return committed
        return group

    def _load_groups(self, group_ids: set[str]) -> dict[str, TeamAliasGroup]:
        refs = [self._db.collection(ALIAS_COLLECTION).document(group_id) for group_id in group_ids]
        groups: dict[str, TeamAliasGroup] = {}
        for snapshot in self._db.get_all(refs):
            if not snapshot.exists:
                continue
            parsed = _group_from_payload(snapshot.id, snapshot.to_dict() or {})
            if parsed is not None:
                groups[parsed.group_id] = parsed
        return groups


_memory_store = MemoryTeamAliasStore()
_override: TeamAliasStore | None = None


def reset_memory_team_alias_store() -> None:
    """Clear the process-local registry between tests."""
    global _memory_store
    _memory_store = MemoryTeamAliasStore()


def get_team_alias_store() -> TeamAliasStore:
    """Use Firestore only for a running Firebase app outside the test environment."""
    if _override is not None:
        return _override
    if os.getenv("ENVIRONMENT") == "test":
        return _memory_store
    try:
        firebase_admin.get_app()
    except ValueError:
        return _memory_store
    return FirestoreTeamAliasStore()


def project_league_teams(
    teams: Sequence[Mapping[str, object]],
    league_id: int,
    store: TeamAliasStore | None = None,
) -> list[dict[str, object]]:
    """Collapse a cached roster to one row per alias group."""
    active_store = store or get_team_alias_store()
    fresh_groups = build_alias_groups(teams, league_id)
    if not fresh_groups:
        return project_team_rows(teams, league_id, [])
    candidate_ids = {team_id for group in fresh_groups for team_id in group.candidate_ids}
    existing = active_store.load(candidate_ids)
    merged = [
        merge_stored_group(group, _stored_group(group, existing)) for group in fresh_groups
    ]
    active_store.save_if_changed(merged, existing)
    return project_team_rows(teams, league_id, merged)


def _stored_group(
    group: TeamAliasGroup, existing: dict[int, TeamAliasGroup]
) -> TeamAliasGroup | None:
    for team_id in group.candidate_ids:
        stored = existing.get(team_id)
        if stored is not None and stored.group_id == group.group_id:
            return stored
    return None


async def prepare_event_aliases(
    *,
    team_ids: set[int],
    followed_team_ids: set[int],
    league_hints: dict[int, int],
    load_roster: Callable[[int], Awaitable[Sequence[Mapping[str, object]]]],
    events: list[Mapping[str, object]],
    store: TeamAliasStore | None = None,
) -> EventAliasResolution:
    """Expand requested ids through the registry and record schedule evidence.

    Roster reads happen only for hinted leagues whose ids are not registered yet.
    The caller supplies the cached roster loader.
    """
    active_store = store or get_team_alias_store()
    requested = team_ids | followed_team_ids
    loaded = active_store.load(requested)
    missing = {team_id for team_id in requested if team_id not in loaded}
    league_ids = {league_hints[team_id] for team_id in missing if team_id in league_hints}
    for league_id in sorted(league_ids):
        roster = await load_roster(league_id)
        fresh_groups = build_alias_groups(roster, league_id)
        if not fresh_groups:
            continue
        candidate_ids = {team_id for group in fresh_groups for team_id in group.candidate_ids}
        existing = active_store.load(candidate_ids)
        merged = [
            merge_stored_group(group, _stored_group(group, existing)) for group in fresh_groups
        ]
        active_store.save_if_changed(merged, existing)
        for group in merged:
            for team_id in group.candidate_ids:
                loaded[team_id] = group

    observed = participant_ids(events)
    unique = {group.group_id: group for group in loaded.values()}
    resolved = {
        group_id: active_store.apply_evidence(group, observed)
        for group_id, group in unique.items()
    }
    groups_by_team: dict[int, TeamAliasGroup] = {}
    for group in resolved.values():
        for team_id in group.candidate_ids:
            groups_by_team[team_id] = group
    aliases, names = alias_maps(list(resolved.values()))
    return EventAliasResolution(
        team_ids=expand_team_ids(team_ids, groups_by_team),
        followed_team_ids=expand_team_ids(followed_team_ids, groups_by_team),
        team_aliases=aliases,
        team_alias_names=names,
    )
