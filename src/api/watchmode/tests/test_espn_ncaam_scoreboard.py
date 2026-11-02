"""Unit tests for ESPN NCAAM scoreboard enrichment helpers."""

import unittest

from api.watchmode.espn_ncaam_scoreboard import (
    apply_ncaam_espn_start_time,
    build_ncaam_start_times_by_matchup,
    espn_team_id_from_watchmode_logo,
    normalize_espn_iso_timestamp,
)


class EspnNcaamScoreboardTests(unittest.TestCase):
    def test_logo_id_extraction(self) -> None:
        url = "https://a.espncdn.com/i/teamlogos/ncaa/500/183.png"
        self.assertEqual(espn_team_id_from_watchmode_logo(url), 183)
        self.assertIsNone(espn_team_id_from_watchmode_logo("not-licensed"))

    def test_normalize_espn_timestamp(self) -> None:
        self.assertEqual(
            normalize_espn_iso_timestamp("2026-11-03T00:00Z"),
            "2026-11-03T00:00:00+00:00",
        )

    def test_build_matchup_times(self) -> None:
        scoreboard = {
            "events": [
                {
                    "competitions": [
                        {
                            "date": "2026-11-03T00:00Z",
                            "competitors": [
                                {"homeAway": "away", "team": {"id": "2441"}},
                                {"homeAway": "home", "team": {"id": "183"}},
                            ],
                        }
                    ]
                }
            ]
        }
        times = build_ncaam_start_times_by_matchup(scoreboard)
        self.assertEqual(times[(2441, 183)], "2026-11-03T00:00:00+00:00")

    def test_apply_start_time_override(self) -> None:
        event = {
            "league": {"id": 472},
            "start_time": "2026-11-02T05:00:00+00:00",
            "away_team": {
                "logo_url": "https://a.espncdn.com/i/teamlogos/ncaa/500/2441.png"
            },
            "home_team": {
                "logo_url": "https://a.espncdn.com/i/teamlogos/ncaa/500/183.png"
            },
        }
        apply_ncaam_espn_start_time(event, {(2441, 183): "2026-11-03T00:00:00+00:00"})
        self.assertEqual(event["start_time"], "2026-11-03T00:00:00+00:00")
