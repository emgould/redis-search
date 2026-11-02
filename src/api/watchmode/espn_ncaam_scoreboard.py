"""ESPN Division I men's college basketball scoreboard helpers for Watchmode enrichment."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

ESPN_NCAAM_SCOREBOARD_PATH = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
)

NCAAM_LEAGUE_ID = 472

_LOGO_TEAM_ID = re.compile(r"/teamlogos/ncaa/500/(\d+)\.png", re.IGNORECASE)


def espn_team_id_from_watchmode_logo(logo_url: str | None) -> int | None:
    if not logo_url or logo_url == "not-licensed":
        return None
    match = _LOGO_TEAM_ID.search(logo_url)
    if not match:
        return None
    return int(match.group(1))


def normalize_espn_iso_timestamp(raw: str) -> str:
    """Normalize ESPN scoreboard dates to ISO-8601 with explicit UTC offset."""
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if len(text) == 19 and text[10] == "T":
        text = f"{text}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def build_ncaam_start_times_by_matchup(
    scoreboard: dict[str, object],
) -> dict[tuple[int, int], str]:
    """Map (away_espn_team_id, home_espn_team_id) to normalized start_time."""
    out: dict[tuple[int, int], str] = {}
    events = scoreboard.get("events")
    if not isinstance(events, list):
        return out
    for item in events:
        if not isinstance(item, dict):
            continue
        competitions = item.get("competitions")
        if not isinstance(competitions, list) or not competitions:
            continue
        competition = competitions[0]
        if not isinstance(competition, dict):
            continue
        date_raw = competition.get("date")
        if not isinstance(date_raw, str) or not date_raw.strip():
            continue
        competitors = competition.get("competitors")
        if not isinstance(competitors, list):
            continue
        away_id: int | None = None
        home_id: int | None = None
        for competitor in competitors:
            if not isinstance(competitor, dict):
                continue
            side = competitor.get("homeAway")
            team = competitor.get("team")
            if not isinstance(team, dict):
                continue
            team_id_raw = team.get("id")
            if team_id_raw is None:
                continue
            try:
                team_id = int(str(team_id_raw))
            except ValueError:
                continue
            if side == "away":
                away_id = team_id
            elif side == "home":
                home_id = team_id
        if away_id is None or home_id is None:
            continue
        out[(away_id, home_id)] = normalize_espn_iso_timestamp(date_raw)
    return out


def _league_id(raw: dict[str, Any]) -> int | None:
    league = raw.get("league")
    if not isinstance(league, dict):
        return None
    league_id = league.get("id")
    return league_id if isinstance(league_id, int) else None


def apply_ncaam_espn_start_time(
    event: dict[str, Any],
    start_times_by_matchup: dict[tuple[int, int], str],
) -> None:
    """Replace Watchmode start_time when ESPN D1 scoreboard has a matching game."""
    if not start_times_by_matchup or _league_id(event) != NCAAM_LEAGUE_ID:
        return
    away_team = event.get("away_team")
    home_team = event.get("home_team")
    if not isinstance(away_team, dict) or not isinstance(home_team, dict):
        return
    away_logo = away_team.get("logo_url")
    home_logo = home_team.get("logo_url")
    away_id = espn_team_id_from_watchmode_logo(
        away_logo if isinstance(away_logo, str) else None
    )
    home_id = espn_team_id_from_watchmode_logo(
        home_logo if isinstance(home_logo, str) else None
    )
    if away_id is None or home_id is None:
        return
    replacement = start_times_by_matchup.get((away_id, home_id))
    if replacement:
        event["start_time"] = replacement
