"""
KickTips Deep Scraper Layer
============================
Scrapes publicly available football statistics from:
- understat.com  — xG, goals, BTTS data (JSON embedded in page)
- fbref.com      — Corners, cards, shots data

Both sites allow non-commercial scraping per their robots.txt.
All requests are polite: 3s delay, no login, no anti-bot bypass.

Returns None on any failure — never crashes the pipeline.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 20
DELAY = 3.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Understat league slugs
UNDERSTAT_SLUGS = {
    39:  "EPL",
    140: "La_liga",
    135: "Serie_A",
    78:  "Bundesliga",
    61:  "Ligue_1",
}

# FBref league IDs + slugs
FBREF_SLUGS = {
    39:  ("9",  "Premier-League"),
    140: ("12", "La-Liga"),
    135: ("11", "Serie-A"),
    78:  ("20", "Bundesliga"),
    61:  ("13", "Ligue-1"),
    94:  ("32", "Primeira-Liga"),
    88:  ("23", "Eredivisie"),
    203: ("26", "Super-Lig"),
    2:   ("8",  "Champions-League"),
}


@dataclass
class TeamStats:
    games_played: int = 0

    home_avg_goals_for: float = 0.0
    home_avg_goals_against: float = 0.0
    away_avg_goals_for: float = 0.0
    away_avg_goals_against: float = 0.0

    home_avg_corners_for: float = 0.0
    home_avg_corners_against: float = 0.0
    away_avg_corners_for: float = 0.0
    away_avg_corners_against: float = 0.0

    home_avg_cards: float = 0.0
    away_avg_cards: float = 0.0

    home_win_rate: float = 0.0
    home_draw_rate: float = 0.0
    away_win_rate: float = 0.0
    away_draw_rate: float = 0.0

    # Deep stats
    home_btts_rate: float = 0.0
    away_btts_rate: float = 0.0
    home_xg_for: float = 0.0
    home_xg_against: float = 0.0
    away_xg_for: float = 0.0
    away_xg_against: float = 0.0

    source: str = ""

    def is_valid(self) -> bool:
        return self.games_played >= 3

    def to_dict(self) -> dict:
        return asdict(self)


class DeepScraperClient:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._cache: Dict[str, Optional[TeamStats]] = {}

    def enrich_team(
        self,
        team_name: str,
        league_name: Optional[str] = None,
        league_api_id: Optional[int] = None,
    ) -> Optional[Dict]:
        cache_key = f"{team_name}:{league_api_id}"
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            return cached.to_dict() if cached else None

        stats = None

        # 1. Try Understat (has xG + BTTS embedded JSON — most reliable)
        if league_api_id and league_api_id in UNDERSTAT_SLUGS:
            stats = self._from_understat(team_name, league_api_id)

        # 2. Try FBref for corners / cards supplement
        if league_api_id and league_api_id in FBREF_SLUGS:
            fbref_stats = self._from_fbref_corners(team_name, league_api_id)
            if fbref_stats and stats:
                # Merge corners into understat stats
                stats.home_avg_corners_for = fbref_stats.home_avg_corners_for
                stats.away_avg_corners_for = fbref_stats.away_avg_corners_for
                stats.home_avg_corners_against = fbref_stats.home_avg_corners_against
                stats.away_avg_corners_against = fbref_stats.away_avg_corners_against
                stats.home_avg_cards = fbref_stats.home_avg_cards
                stats.away_avg_cards = fbref_stats.away_avg_cards
            elif fbref_stats and not stats:
                stats = fbref_stats

        self._cache[cache_key] = stats
        return stats.to_dict() if stats and stats.is_valid() else None

    # Backwards compat
    def fetch_team_enrichment(self, team_name: str, league_name: Optional[str] = None) -> Optional[Dict]:
        return self.enrich_team(team_name, league_name=league_name)

    # ───────────────────────────────────────────────
    # UNDERSTAT
    # ───────────────────────────────────────────────

    def _from_understat(self, team_name: str, league_api_id: int) -> Optional[TeamStats]:
        slug = UNDERSTAT_SLUGS.get(league_api_id)
        if not slug:
            return None

        url = f"https://understat.com/league/{slug}"
        html = self._get(url)
        if not html:
            return None

        try:
            match = re.search(r"var teamsData\s*=\s*JSON\.parse\('(.+?)'\)", html, re.DOTALL)
            if not match:
                return None

            raw = match.group(1).encode("utf-8").decode("unicode_escape")
            data = json.loads(raw)

            for _, team_data in data.items():
                name = team_data.get("title", "")
                if not self._fuzzy_match(team_name, name):
                    continue

                history = team_data.get("history", [])
                if not history:
                    continue

                home_g = [g for g in history if g.get("h_a") == "h"]
                away_g = [g for g in history if g.get("h_a") == "a"]

                def avg(games, key):
                    vals = [float(g.get(key) or 0) for g in games]
                    return round(sum(vals) / len(vals), 3) if vals else 0.0

                def wr(games):
                    wins = sum(1 for g in games if g.get("result") == "w")
                    return round(wins / len(games), 3) if games else 0.0

                def dr(games):
                    draws = sum(1 for g in games if g.get("result") == "d")
                    return round(draws / len(games), 3) if games else 0.0

                def btts(games):
                    b = sum(
                        1 for g in games
                        if int(g.get("scored") or 0) > 0
                        and int(g.get("missed") or 0) > 0
                    )
                    return round(b / len(games), 3) if games else 0.0

                stats = TeamStats(
                    games_played=len(history),
                    home_avg_goals_for=avg(home_g, "scored"),
                    home_avg_goals_against=avg(home_g, "missed"),
                    away_avg_goals_for=avg(away_g, "scored"),
                    away_avg_goals_against=avg(away_g, "missed"),
                    home_xg_for=avg(home_g, "xG"),
                    home_xg_against=avg(home_g, "xGA"),
                    away_xg_for=avg(away_g, "xG"),
                    away_xg_against=avg(away_g, "xGA"),
                    home_win_rate=wr(home_g),
                    home_draw_rate=dr(home_g),
                    away_win_rate=wr(away_g),
                    away_draw_rate=dr(away_g),
                    home_btts_rate=btts(home_g),
                    away_btts_rate=btts(away_g),
                    source="understat",
                )
                logger.info("[SCRAPER][Understat] %s ok (%d games)", team_name, stats.games_played)
                return stats

        except Exception as exc:
            logger.warning("[SCRAPER][Understat] %s failed: %s", team_name, exc)

        return None

    # ───────────────────────────────────────────────
    # FBREF — corners & cards only
    # ───────────────────────────────────────────────

    def _from_fbref_corners(self, team_name: str, league_api_id: int) -> Optional[TeamStats]:
        slug_info = FBREF_SLUGS.get(league_api_id)
        if not slug_info:
            return None

        comp_id, comp_slug = slug_info
        url = f"https://fbref.com/en/comps/{comp_id}/{comp_slug}-Stats"
        html = self._get(url)
        if not html:
            return None

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Corners from passing table
            corners_for = self._fbref_team_stat(soup, team_name, "stats_squads_passing_for", "corner_kicks")
            corners_agt = self._fbref_team_stat(soup, team_name, "stats_squads_passing_against", "corner_kicks")
            games = self._fbref_team_stat(soup, team_name, "stats_squads_passing_for", "games", cast=int)

            # Cards from misc table
            yellows = self._fbref_team_stat(soup, team_name, "stats_squads_misc_for", "cards_yellow")

            if not games or games <= 0:
                return None

            stats = TeamStats(
                games_played=games,
                home_avg_corners_for=round(corners_for / games, 2) if corners_for else 0.0,
                away_avg_corners_for=round(corners_for / games * 0.9, 2) if corners_for else 0.0,
                home_avg_corners_against=round(corners_agt / games, 2) if corners_agt else 0.0,
                away_avg_corners_against=round(corners_agt / games * 1.1, 2) if corners_agt else 0.0,
                home_avg_cards=round(yellows / games * 0.45, 2) if yellows else 0.0,
                away_avg_cards=round(yellows / games * 0.55, 2) if yellows else 0.0,
                source="fbref",
            )
            logger.info("[SCRAPER][FBref] %s corners ok", team_name)
            return stats

        except Exception as exc:
            logger.warning("[SCRAPER][FBref] %s failed: %s", team_name, exc)
            return None

    def _fbref_team_stat(self, soup, team_name, table_id, stat_name, cast=float):
        table = soup.find("table", {"id": table_id})
        if not table:
            return 0

        tbody = table.find("tbody")
        if not tbody:
            return 0

        for row in tbody.find_all("tr"):
            th = row.find("th", {"data-stat": "team"})
            if not th or not self._fuzzy_match(team_name, th.get_text(strip=True)):
                continue
            td = row.find("td", {"data-stat": stat_name})
            if not td:
                return 0
            raw = td.get_text(strip=True).replace(",", "")
            try:
                return cast(raw) if raw else 0
            except Exception:
                return 0
        return 0

    # ───────────────────────────────────────────────
    # HELPERS
    # ───────────────────────────────────────────────

    def _get(self, url: str) -> Optional[str]:
        try:
            time.sleep(DELAY)
            r = self.session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException as exc:
            logger.warning("[SCRAPER] HTTP error %s: %s", url, exc)
            return None

    def _fuzzy_match(self, a: str, b: str) -> bool:
        a = a.lower().strip()
        b = b.lower().strip()
        if a == b or a in b or b in a:
            return True
        stop = {"fc", "cf", "sc", "ac", "united", "city", "club", "football", "association"}
        wa = set(a.split()) - stop
        wb = set(b.split()) - stop
        if not wa or not wb:
            return False
        overlap = len(wa & wb) / max(len(wa), len(wb))
        return overlap >= 0.5


scraper = DeepScraperClient()
scraper_client = scraper  # backwards compat
