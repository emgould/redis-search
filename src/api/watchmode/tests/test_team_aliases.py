"""Team-alias grouping and schedule resolution. No Watchmode or Firestore calls."""

import os
import unittest
from collections.abc import Mapping

os.environ["ENVIRONMENT"] = "test"

from api.watchmode.team_alias_groups import (
    apply_observation,
    build_alias_groups,
    normalized_team_key,
)
from api.watchmode.team_alias_store import (
    MemoryTeamAliasStore,
    prepare_event_aliases,
    project_league_teams,
)


def _team(team_id: int, name: str, short_name: str | None = None) -> dict[str, object]:
    record: dict[str, object] = {"id": team_id, "name": name, "league_id": 470}
    if short_name is not None:
        record["short_name"] = short_name
    return record


class TeamAliasGroupTests(unittest.TestCase):
    def test_seattle_uses_the_longest_name_and_keeps_both_ids(self) -> None:
        groups = build_alias_groups(
            [
                _team(234220, "Seattle Sounders"),
                _team(1093, "Seattle Sounders FC", "Seattle Sounders FC"),
            ],
            470,
        )
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].display_name, "Seattle Sounders FC")
        self.assertEqual(groups[0].display_team_id, 1093)
        self.assertEqual(groups[0].candidate_ids, (1093, 234220))

    def test_manchester_clubs_stay_separate(self) -> None:
        groups = build_alias_groups(
            [
                _team(1, "Manchester City"),
                _team(2, "Manchester United"),
            ],
            110166,
        )
        self.assertEqual(groups, [])

    def test_punctuation_and_exact_duplicates_group(self) -> None:
        groups = build_alias_groups(
            [
                _team(1075, "D.C. United"),
                _team(268713, "DC United"),
                _team(961, "Atlanta Hawks", "Hawks"),
                _team(142034, "Atlanta Hawks"),
            ],
            6,
        )
        names = {group.display_name for group in groups}
        self.assertEqual(names, {"D.C. United", "Atlanta Hawks"})

    def test_placeholder_names_are_not_aliases(self) -> None:
        self.assertIsNone(normalized_team_key("TBD"))
        self.assertEqual(
            build_alias_groups([_team(1, "TBD"), _team(2, "TBD")], 1),
            [],
        )

    def test_same_name_in_another_league_is_a_different_group(self) -> None:
        left = build_alias_groups(
            [_team(1, "Seattle Sounders"), _team(2, "Seattle Sounders FC")], 470
        )
        right = build_alias_groups(
            [_team(3, "Seattle Sounders"), _team(4, "Seattle Sounders FC")], 999
        )
        self.assertNotEqual(left[0].group_id, right[0].group_id)

    def test_empty_schedule_does_not_resolve_and_one_id_does(self) -> None:
        group = build_alias_groups(
            [_team(234220, "Seattle Sounders"), _team(1093, "Seattle Sounders FC")],
            470,
        )[0]
        self.assertEqual(apply_observation(group, set()).status, "unresolved")
        resolved = apply_observation(group, {1093})
        self.assertEqual(resolved.status, "resolved")
        self.assertEqual(resolved.resolved_team_id, 1093)
        self.assertEqual(resolved.catalog_team_id(), 1093)

    def test_second_id_reopens_to_ambiguous_without_flapping(self) -> None:
        group = build_alias_groups(
            [_team(234220, "Seattle Sounders"), _team(1093, "Seattle Sounders FC")],
            470,
        )[0]
        resolved = apply_observation(group, {1093})
        reopened = apply_observation(resolved, {234220})
        self.assertEqual(reopened.status, "ambiguous")
        self.assertIsNone(reopened.resolved_team_id)
        self.assertEqual(apply_observation(reopened, {1093}).status, "ambiguous")


class TeamAliasStoreTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.store = MemoryTeamAliasStore()
        self.roster = [
            _team(234220, "Seattle Sounders"),
            _team(1093, "Seattle Sounders FC", "Seattle Sounders FC"),
            _team(1, "Manchester City"),
            _team(2, "Manchester United"),
        ]

    async def _roster(self, league_id: int) -> list[Mapping[str, object]]:
        self.loaded_leagues.append(league_id)
        return self.roster

    async def test_projection_and_event_resolution_do_not_need_a_second_roster_read(self) -> None:
        self.loaded_leagues: list[int] = []
        rows = project_league_teams(self.roster, 470, self.store)
        seattle = next(row for row in rows if row["name"] == "Seattle Sounders FC")
        self.assertEqual(seattle["id"], 1093)
        self.assertEqual(seattle["alias_team_ids"], [1093, 234220])
        self.assertEqual(
            sorted(str(row["name"]) for row in rows),
            ["Manchester City", "Manchester United", "Seattle Sounders FC"],
        )

        quiet = await prepare_event_aliases(
            team_ids={234220},
            followed_team_ids=set(),
            league_hints={234220: 470},
            load_roster=self._roster,
            events=[],
            store=self.store,
        )
        self.assertEqual(quiet.team_ids, {1093, 234220})
        self.assertEqual(self.loaded_leagues, [])
        self.assertEqual(quiet.team_aliases["234220"], "1093")

        game = {"home_team": {"id": 1093, "name": "Seattle Sounders FC"}, "away_team": {"id": 8}}
        resolved = await prepare_event_aliases(
            team_ids={234220},
            followed_team_ids=set(),
            league_hints={234220: 470},
            load_roster=self._roster,
            events=[game],
            store=self.store,
        )
        self.assertEqual(resolved.team_ids, {1093})
        self.assertEqual(resolved.team_alias_names["1093"], "Seattle Sounders FC")
        self.assertEqual(self.loaded_leagues, [])

    async def test_both_ids_with_events_stay_expanded(self) -> None:
        project_league_teams(self.roster, 470, self.store)
        events = [
            {"id": 1, "home_team": {"id": 1093}, "away_team": {"id": 8}},
            {"id": 2, "home_team": {"id": 234220}, "away_team": {"id": 9}},
        ]
        resolved = await prepare_event_aliases(
            team_ids={1093},
            followed_team_ids=set(),
            league_hints={},
            load_roster=self._roster,
            events=events,
            store=self.store,
        )
        self.assertEqual(resolved.team_ids, {1093, 234220})

    async def test_hint_builds_a_missing_group_from_the_cached_roster(self) -> None:
        self.loaded_leagues = []
        resolved = await prepare_event_aliases(
            team_ids={234220},
            followed_team_ids=set(),
            league_hints={234220: 470},
            load_roster=self._roster,
            events=[{"home_team": {"id": 1093}, "away_team": {"id": 8}}],
            store=self.store,
        )
        self.assertEqual(self.loaded_leagues, [470])
        self.assertEqual(resolved.team_ids, {1093})


if __name__ == "__main__":
    unittest.main()
