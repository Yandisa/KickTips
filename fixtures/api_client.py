"""
KickTips API Client — FlashScore Edition (v3 — DB-driven tournaments)
======================================================================
All data sourced from FlashScore via RapidAPI.
Host: flashscore4.p.rapidapi.com

Changes from v2:
  - Tournament/league filtering is now DB-driven via League.tournament_url
  - KNOWN_TOURNAMENT_URLS dict is used only as a seed reference
  - New leagues can be added via Django admin without code changes
  - _resolve_league_info now queries DB first, falls back to dict

Endpoints used:
  Existing (unchanged):
    GET /api/flashscore/v2/matches/list
    GET /api/flashscore/v2/matches/details
    GET /api/flashscore/v2/matches/h2h
    GET /api/flashscore/v2/matches/match/stats
    GET /api/flashscore/v2/teams/results          (pages 1 & 2)
    GET /api/flashscore/v2/matches/standings
    GET /api/flashscore/v2/matches/standings/over-under
    GET /api/flashscore/v2/matches/standings/ht-ft

  NEW — deep enrichment:
    GET /api/flashscore/v2/matches/odds              → bookmaker prices → value gate
    GET /api/flashscore/v2/matches/standings/form    → last-N form string
    GET /api/flashscore/v2/matches/match/lineups     → starting XI → key-player check

API call budget per day (~15 fixtures, PRO plan):
  Morning pipeline per fixture:
    1  list (shared)
    1  odds
    1  form
    1  standings
    1  over-under 2.5
    1  ht-ft
    1  lineups
  = 7 per fixture × 15 = 105
  + team results (up to 30 teams × 2 pages) = 60
  Total: ~165 calls/day — well within PRO 20k/month
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RAPID_API_KEY = getattr(settings, "RAPID_API_KEY", "")
FS_HOST       = getattr(settings, "FLASHSCORE_API_HOST", "flashscore4.p.rapidapi.com")
FS_BASE       = getattr(settings, "FLASHSCORE_API_BASE_URL", "https://flashscore4.p.rapidapi.com")
FS_TIMEOUT    = getattr(settings, "FLASHSCORE_API_TIMEOUT", 35)

# ── Soccer Football Info API ──────────────────────────────────────────────────
SI_HOST    = getattr(settings, "SOCCER_INFO_API_HOST",    "soccer-football-info.p.rapidapi.com")
SI_BASE    = getattr(settings, "SOCCER_INFO_API_BASE_URL", "https://soccer-football-info.p.rapidapi.com")
SI_TIMEOUT = getattr(settings, "SOCCER_INFO_API_TIMEOUT",  15)

# Geo-IP code for odds — use ZA (South Africa) as default
GEO_IP_CODE = getattr(settings, "ODDS_GEO_IP_CODE", "ZA")

# ── Seed data — used only to populate DB on first run ────────────────────────
# To add a new league: add it here AND in Django admin (or just admin after seeding)
KNOWN_TOURNAMENT_URLS: Dict[str, dict] = {
    # ── Tier 1: Elite European + continental ──────────────────────────────
    "/football/england/premier-league/":      {"name": "Premier League",       "country": "England",      "tier": 1, "api_id": 39},
    "/football/spain/laliga/":                {"name": "La Liga",               "country": "Spain",        "tier": 1, "api_id": 140},
    "/football/italy/serie-a/":               {"name": "Serie A",               "country": "Italy",        "tier": 1, "api_id": 135},
    "/football/germany/bundesliga/":          {"name": "Bundesliga",            "country": "Germany",      "tier": 1, "api_id": 78},
    "/football/france/ligue-1/":              {"name": "Ligue 1",               "country": "France",       "tier": 1, "api_id": 61},
    "/football/europe/champions-league/":     {"name": "Champions League",      "country": "Europe",       "tier": 1, "api_id": 2},
    "/football/europe/europa-league/":        {"name": "Europa League",         "country": "Europe",       "tier": 1, "api_id": 3},
    "/football/europe/conference-league/":    {"name": "Conference League",     "country": "Europe",       "tier": 1, "api_id": 848},
    "/football/netherlands/eredivisie/":      {"name": "Eredivisie",            "country": "Netherlands",  "tier": 1, "api_id": 88},
    "/football/portugal/liga-portugal/":      {"name": "Liga Portugal",         "country": "Portugal",     "tier": 1, "api_id": 94},
    "/football/turkey/super-lig/":            {"name": "Super Lig",             "country": "Turkey",       "tier": 1, "api_id": 203},
    "/football/scotland/premiership/":        {"name": "Scottish Premiership",  "country": "Scotland",     "tier": 1, "api_id": 179},
    "/football/belgium/pro-league/":          {"name": "Pro League",            "country": "Belgium",      "tier": 1, "api_id": 144},
    "/football/greece/super-league/":         {"name": "Super League",          "country": "Greece",       "tier": 1, "api_id": 197},
    "/football/russia/premier-league/":       {"name": "Russian Premier",       "country": "Russia",       "tier": 1, "api_id": 235},
    "/football/austria/bundesliga/":          {"name": "Austrian Bundesliga",   "country": "Austria",      "tier": 1, "api_id": 218},
    "/football/switzerland/super-league/":    {"name": "Swiss Super League",    "country": "Switzerland",  "tier": 1, "api_id": 207},
    "/football/denmark/superliga/":           {"name": "Superliga",             "country": "Denmark",      "tier": 1, "api_id": 119},
    "/football/sweden/allsvenskan/":          {"name": "Allsvenskan",           "country": "Sweden",       "tier": 1, "api_id": 113},
    "/football/norway/eliteserien/":          {"name": "Eliteserien",           "country": "Norway",       "tier": 1, "api_id": 108},
    "/football/czech-republic/fortuna-liga/": {"name": "Fortuna Liga",          "country": "Czech Rep.",   "tier": 1, "api_id": 345},
    "/football/poland/ekstraklasa/":          {"name": "Ekstraklasa",           "country": "Poland",       "tier": 1, "api_id": 106},
    "/football/croatia/hnl/":                 {"name": "HNL",                   "country": "Croatia",      "tier": 1, "api_id": 210},
    "/football/serbia/super-liga/":           {"name": "Super Liga",            "country": "Serbia",       "tier": 1, "api_id": 292},
    # ── Tier 1: African ───────────────────────────────────────────────────
    "/football/south-africa/psl/":            {"name": "PSL",                   "country": "South Africa", "tier": 1, "api_id": 288},
    "/football/egypt/premier-league/":        {"name": "Egyptian Premier",      "country": "Egypt",        "tier": 1, "api_id": 233},
    "/football/morocco/botola-pro/":          {"name": "Botola Pro",            "country": "Morocco",      "tier": 1, "api_id": 1322},
    "/football/nigeria/npfl/":                {"name": "NPFL",                  "country": "Nigeria",      "tier": 1, "api_id": 1368},
    "/football/africa/caf-champions-league/": {"name": "CAF Champions League",  "country": "Africa",       "tier": 1, "api_id": 1038},
    # ── Tier 1: Americas ─────────────────────────────────────────────────
    "/football/usa/mls/":                     {"name": "MLS",                   "country": "USA",          "tier": 1, "api_id": 253},
    "/football/brazil/serie-a/":              {"name": "Brasileirão Serie A",   "country": "Brazil",       "tier": 1, "api_id": 325},
    "/football/argentina/primera-division/":  {"name": "Primera División",      "country": "Argentina",    "tier": 1, "api_id": 155},
    "/football/mexico/liga-mx/":              {"name": "Liga MX",               "country": "Mexico",       "tier": 1, "api_id": 262},
    "/football/colombia/primera-a/":          {"name": "Primera A",             "country": "Colombia",     "tier": 1, "api_id": 311},
    "/football/chile/primera-division/":      {"name": "Primera División",      "country": "Chile",        "tier": 1, "api_id": 335},
    "/football/uruguay/primera-division/":    {"name": "Primera División",      "country": "Uruguay",      "tier": 1, "api_id": 278},
    # ── Tier 1: Asia/Middle East ──────────────────────────────────────────
    "/football/saudi-arabia/saudi-pro-league/": {"name": "Saudi Pro League",    "country": "Saudi Arabia", "tier": 1, "api_id": 307},
    "/football/japan/j1-league/":             {"name": "J1 League",             "country": "Japan",        "tier": 1, "api_id": 98},
    "/football/south-korea/k-league-1/":      {"name": "K League 1",            "country": "South Korea",  "tier": 1, "api_id": 871},
    "/football/china/super-league/":          {"name": "Chinese Super League",  "country": "China",        "tier": 1, "api_id": 169},
    "/football/australia/a-league/":          {"name": "A-League",              "country": "Australia",    "tier": 1, "api_id": 188},
    # ── Tier 2: Second divisions ──────────────────────────────────────────
    "/football/england/championship/":        {"name": "Championship",          "country": "England",      "tier": 2, "api_id": 40},
    "/football/spain/laliga2/":               {"name": "Segunda División",      "country": "Spain",        "tier": 2, "api_id": 141},
    "/football/italy/serie-b/":               {"name": "Serie B",               "country": "Italy",        "tier": 2, "api_id": 136},
    "/football/germany/2-bundesliga/":        {"name": "2. Bundesliga",         "country": "Germany",      "tier": 2, "api_id": 79},
    "/football/france/ligue-2/":              {"name": "Ligue 2",               "country": "France",       "tier": 2, "api_id": 62},
    "/football/netherlands/eerste-divisie/":  {"name": "Eerste Divisie",        "country": "Netherlands",  "tier": 2, "api_id": 89},
    "/football/portugal/liga-portugal-2/":    {"name": "Liga Portugal 2",       "country": "Portugal",     "tier": 2, "api_id": 95},
    "/football/england/league-one/":          {"name": "League One",            "country": "England",      "tier": 2, "api_id": 41},
    "/football/england/league-two/":          {"name": "League Two",            "country": "England",      "tier": 2, "api_id": 42},
    "/football/scotland/championship/":       {"name": "Scottish Championship", "country": "Scotland",     "tier": 2, "api_id": 180},
    "/football/brazil/serie-b/":              {"name": "Brasileirão Serie B",   "country": "Brazil",       "tier": 2, "api_id": 326},
    "/football/usa/usl-championship/":        {"name": "USL Championship",      "country": "USA",          "tier": 2, "api_id": 254},
}

# ── In-memory cache for DB league lookups (rebuilt each process start) ────────
_DB_LEAGUE_CACHE: Dict[str, dict] = {}
_DB_CACHE_LOADED = False


def _load_league_cache():
    """Load all active leagues with tournament_url from DB into memory cache."""
    global _DB_LEAGUE_CACHE, _DB_CACHE_LOADED
    if _DB_CACHE_LOADED:
        return
    try:
        from fixtures.models import League
        _DB_LEAGUE_CACHE = {}
        for league in League.objects.filter(active=True).exclude(tournament_url=""):
            _DB_LEAGUE_CACHE[league.tournament_url] = {
                "name":    league.name,
                "country": league.country,
                "tier":    league.tier,
                "api_id":  league.api_id,
            }
        _DB_CACHE_LOADED = True
        logger.info("[LeagueCache] Loaded %d active leagues from DB", len(_DB_LEAGUE_CACHE))
    except Exception as exc:
        logger.warning("[LeagueCache] Failed to load from DB: %s", exc)


def reload_league_cache():
    """Force reload of league cache — call after adding leagues in admin."""
    global _DB_CACHE_LOADED
    _DB_CACHE_LOADED = False
    _load_league_cache()


def seed_leagues_from_known_urls():
    """
    One-time seeder — populates League.tournament_url from KNOWN_TOURNAMENT_URLS.
    Safe to run multiple times — only updates leagues missing a tournament_url.
    Call from management command or shell after migration.
    """
    from fixtures.models import League
    updated = 0
    for url, info in KNOWN_TOURNAMENT_URLS.items():
        league = League.objects.filter(api_id=info["api_id"]).first()
        if league and not league.tournament_url:
            league.tournament_url = url
            league.save(update_fields=["tournament_url"])
            updated += 1
            logger.info("[Seeder] %s → %s", league.name, url)
        elif not league:
            # Create if it doesn't exist yet
            League.objects.create(
                api_id=info["api_id"],
                name=info["name"],
                country=info["country"],
                tier=info["tier"],
                active=True,
                tournament_url=url,
            )
            updated += 1
            logger.info("[Seeder] Created %s → %s", info["name"], url)
    logger.info("[Seeder] Done — %d leagues updated/created", updated)
    return updated


# ── Internal HTTP helper ──────────────────────────────────────────────────────

def _headers() -> Dict[str, str]:
    return {
        "x-rapidapi-key":  RAPID_API_KEY,
        "x-rapidapi-host": FS_HOST,
    }


def _get(path: str, params: Optional[dict] = None):
    """
    GET a FlashScore endpoint.
    Returns parsed JSON or None on any error.
    """
    if not RAPID_API_KEY:
        logger.error("RAPID_API_KEY is not set.")
        return None

    url = f"{FS_BASE}{path}"
    try:
        resp = requests.get(url, headers=_headers(), params=params or {}, timeout=FS_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        logger.error("FlashScore HTTP %s [%s]: %s", code, path, exc)
        return None
    except requests.RequestException as exc:
        logger.error("FlashScore request failed [%s]: %s", path, exc)
        return None

    try:
        return resp.json()
    except ValueError:
        logger.error("Invalid JSON from [%s]", path)
        return None


def _si_get(path: str, params: Optional[dict] = None):
    """GET a Soccer Football Info endpoint. Returns parsed JSON or None."""
    if not RAPID_API_KEY:
        logger.error("RAPID_API_KEY is not set.")
        return None

    url = f"{SI_BASE}{path}"
    headers = {
        "x-rapidapi-key":  RAPID_API_KEY,
        "x-rapidapi-host": SI_HOST,
    }
    try:
        resp = requests.get(url, headers=headers, params=params or {}, timeout=SI_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        logger.error("SoccerInfo HTTP %s [%s]: %s", code, path, exc)
        return None
    except requests.RequestException as exc:
        logger.error("SoccerInfo request failed [%s]: %s", path, exc)
        return None

    try:
        return resp.json()
    except ValueError:
        logger.error("Invalid JSON from SoccerInfo [%s]", path)
        return None


# ── Stable ID ────────────────────────────────────────────────────────────────

def _stable_id(flashscore_id: str) -> int:
    """
    Deterministic integer ID from a FlashScore string ID.
    Uses MD5 — stable across processes unlike Python's built-in hash().
    """
    return int(hashlib.md5(str(flashscore_id).encode()).hexdigest(), 16) % (10 ** 9)


# ── Status resolver ───────────────────────────────────────────────────────────

def _resolve_status(status_d: dict) -> str:
    if status_d.get("is_finished"):    return "finished"
    if status_d.get("is_in_progress"): return "live"
    if status_d.get("is_postponed"):   return "postponed"
    if status_d.get("is_cancelled"):   return "cancelled"
    return "scheduled"


def _resolve_league_info(tournament_url: str) -> Optional[dict]:
    """
    Look up league info by tournament URL.
    Queries DB cache first, falls back to hardcoded dict.
    DB takes priority — allows admin overrides.
    """
    if not tournament_url:
        return None

    # Ensure cache is loaded
    _load_league_cache()

    # Try exact match in DB cache
    if tournament_url in _DB_LEAGUE_CACHE:
        return _DB_LEAGUE_CACHE[tournament_url]

    # Try base URL (strip trailing path components)
    parts = tournament_url.strip("/").split("/")
    base  = "/" + "/".join(parts[:3]) + "/"
    if base in _DB_LEAGUE_CACHE:
        return _DB_LEAGUE_CACHE[base]

    # Fall back to hardcoded dict (for leagues not yet in DB)
    return KNOWN_TOURNAMENT_URLS.get(tournament_url) or KNOWN_TOURNAMENT_URLS.get(base)


# ══════════════════════════════════════════════════════════════════════════════
# 1. FIXTURES BY DATE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fixtures_by_date(date_str: str, day: int = 0) -> List[dict]:
    """Cost: 1 call. day=0 → today, day=1 → tomorrow."""
    logger.info("[FlashScore] Fetching fixtures for %s (day=%s)", date_str, day)
    data = _get("/api/flashscore/v2/matches/list", {"sport_id": 1, "day": day})
    if not isinstance(data, list):
        logger.warning("[FlashScore] matches/list returned no data")
        return []

    results = []
    for tournament_block in data:
        t_url = tournament_block.get("tournament_url", "")
        league_info = _resolve_league_info(t_url)
        if not league_info:
            continue
        for match in tournament_block.get("matches", []):
            norm = _normalize_list_match(match, tournament_block, league_info)
            if norm:
                results.append(norm)

    logger.info("[FlashScore] %d fixtures in known leagues", len(results))
    return results


def _normalize_list_match(match: dict, tournament_block: dict, league_info: dict) -> Optional[dict]:
    match_id = match.get("match_id")
    if not match_id:
        return None

    status_d = match.get("match_status") or {}
    home     = match.get("home_team") or {}
    away     = match.get("away_team") or {}
    scores   = match.get("scores") or {}

    ts = match.get("timestamp")
    kickoff = None
    if ts:
        try:
            kickoff = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass

    return {
        "match_id":            match_id,
        "kickoff":             kickoff,
        "status":              _resolve_status(status_d),
        "tournament_url":      tournament_block.get("tournament_url", ""),
        "tournament_id":       tournament_block.get("tournament_id", ""),
        "tournament_stage_id": match.get("tournament_stage_id", ""),
        "league_name":         league_info["name"],
        "country_name":        league_info["country"],
        "league_api_id":       league_info["api_id"],
        "league_tier":         league_info["tier"],
        "home_team_id":        home.get("team_id", "") or home.get("id", ""),
        "home_team_name":      (home.get("name") or home.get("short_name") or
                                home.get("shortName") or home.get("title") or
                                home.get("participant_name") or ""),
        "away_team_id":        away.get("team_id", "") or away.get("id", ""),
        "away_team_name":      (away.get("name") or away.get("short_name") or
                                away.get("shortName") or away.get("title") or
                                away.get("participant_name") or ""),
        "home_score":          scores.get("home"),
        "away_score":          scores.get("away"),
        "venue":               "",
        "referee":             "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2. MATCH ODDS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_match_odds(match_id: str) -> dict:
    """
    Fetch bookmaker consensus odds for a match.
    Cost: 1 call per fixture.
    """
    if not match_id:
        return {}

    data = _get("/api/flashscore/v2/matches/odds", {
        "match_id":    match_id,
        "geo_ip_code": GEO_IP_CODE,
    })

    if not isinstance(data, list):
        return {}

    collected = {"1x2": [], "ou_goals": {}, "btts": [], "ou_corners": {}, "dc": []}

    for bookmaker in data:
        bk_odds = bookmaker.get("odds") or []
        for market in bk_odds:
            btype     = market.get("bettingType",  "")
            bscope    = market.get("bettingScope", "")
            odds_list = market.get("odds") or []

            if bscope != "FULL_TIME":
                continue

            if btype == "HOME_DRAW_AWAY":
                participant_odds = []
                draw = None
                for o in odds_list:
                    if not o.get("active"):
                        continue
                    v   = _safe_float(o.get("value"))
                    pid = o.get("eventParticipantId")
                    if v is None:
                        continue
                    if pid is not None:
                        participant_odds.append(v)
                    else:
                        draw = v
                if len(participant_odds) >= 2:
                    home, away = participant_odds[0], participant_odds[1]
                    if home and draw and away:
                        collected["1x2"].append({"home": home, "draw": draw, "away": away})

            elif btype == "OVER_UNDER":
                for o in odds_list:
                    if not o.get("active"):
                        continue
                    v         = _safe_float(o.get("value"))
                    handicap  = o.get("handicap") or {}
                    line_str  = str(handicap.get("value") or "").strip()
                    selection = (o.get("selection") or "").upper()
                    if v is None or not line_str or selection not in ("OVER", "UNDER"):
                        continue
                    try:
                        line_f = float(line_str)
                    except ValueError:
                        continue
                    bucket = "ou_corners" if line_f >= 6.5 else "ou_goals"
                    if line_str not in collected[bucket]:
                        collected[bucket][line_str] = {"over": [], "under": []}
                    if selection == "OVER":
                        collected[bucket][line_str]["over"].append(v)
                    else:
                        collected[bucket][line_str]["under"].append(v)

            elif btype == "BOTH_TEAMS_TO_SCORE":
                yes_v = no_v = None
                for o in odds_list:
                    if not o.get("active"):
                        continue
                    v = _safe_float(o.get("value"))
                    if v is None:
                        continue
                    if o.get("bothTeamsToScore") is True:
                        yes_v = v
                    elif o.get("bothTeamsToScore") is False:
                        no_v = v
                if yes_v and no_v:
                    collected["btts"].append({"yes": yes_v, "no": no_v})

            elif btype == "DOUBLE_CHANCE":
                null_v = None
                participant_vals = []
                for o in odds_list:
                    if not o.get("active"):
                        continue
                    v   = _safe_float(o.get("value"))
                    pid = o.get("eventParticipantId")
                    if v is None:
                        continue
                    if pid is None:
                        null_v = v
                    else:
                        participant_vals.append(v)
                if len(participant_vals) >= 2:
                    home_v, away_v = participant_vals[0], participant_vals[1]
                    if null_v and home_v and away_v:
                        collected["dc"].append({
                            "12": null_v,
                            "1x": home_v,
                            "x2": away_v,
                        })

    result = {}

    if collected["1x2"]:
        n = len(collected["1x2"])
        result["1x2"] = {
            "home": round(sum(x["home"] for x in collected["1x2"]) / n, 3),
            "draw": round(sum(x["draw"] for x in collected["1x2"]) / n, 3),
            "away": round(sum(x["away"] for x in collected["1x2"]) / n, 3),
        }

    for bucket in ("ou_goals", "ou_corners"):
        if collected[bucket]:
            result[bucket] = {}
            for line_str, sides in collected[bucket].items():
                if sides["over"] and sides["under"]:
                    result[bucket][line_str] = {
                        "over":  round(sum(sides["over"])  / len(sides["over"]),  3),
                        "under": round(sum(sides["under"]) / len(sides["under"]), 3),
                    }

    if collected["btts"]:
        n = len(collected["btts"])
        result["btts"] = {
            "yes": round(sum(x["yes"] for x in collected["btts"]) / n, 3),
            "no":  round(sum(x["no"]  for x in collected["btts"]) / n, 3),
        }

    if collected["dc"]:
        n = len(collected["dc"])
        result["dc"] = {
            "12": round(sum(x["12"] for x in collected["dc"]) / n, 3),
            "1x": round(sum(x["1x"] for x in collected["dc"]) / n, 3),
            "x2": round(sum(x["x2"] for x in collected["dc"]) / n, 3),
        }

    logger.info("[Odds] match=%s markets=%s", match_id, list(result.keys()))
    return result


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 3. STANDINGS FORM
# ══════════════════════════════════════════════════════════════════════════════

def fetch_match_standings_form(match_id: str) -> dict:
    """Fetch last-N form for both teams in this match. Cost: 1 call."""
    if not match_id:
        return {}

    data = _get("/api/flashscore/v2/matches/standings/form", {
        "match_id": match_id,
        "type":     "overall",
    })

    if not isinstance(data, list):
        return {}

    result = {}
    for row in data:
        team_id = row.get("team_id") or row.get("id") or ""
        if not team_id:
            continue

        form_str = ""
        raw_form = row.get("form") or row.get("recent_form") or ""
        if isinstance(raw_form, str):
            form_str = raw_form.upper()
        elif isinstance(raw_form, list):
            chars = []
            for f in raw_form:
                r = (f.get("result") or f.get("type") or "").upper()
                if r in ("W", "WIN"):
                    chars.append("W")
                elif r in ("D", "DRAW"):
                    chars.append("D")
                elif r in ("L", "LOSS", "LOSE"):
                    chars.append("L")
            form_str = "".join(chars)

        result[str(team_id)] = {
            "team_id": str(team_id),
            "form":    form_str,
            "played":  int(row.get("matches_played") or row.get("played") or 0),
            "won":     int(row.get("wins") or row.get("won") or 0),
            "drawn":   int(row.get("draws") or row.get("drawn") or 0),
            "lost":    int(row.get("losses") or row.get("lost") or 0),
            "gf":      int(row.get("goals_for") or row.get("scored") or 0),
            "ga":      int(row.get("goals_against") or row.get("conceded") or 0),
        }

    return result


# ══════════════════════════════════════════════════════════════════════════════
# 4. LINEUPS
# ══════════════════════════════════════════════════════════════════════════════

_KEY_POSITIONS = {"goalkeeper", "striker", "forward", "centre-forward", "attacker"}
_IMPORTANT_POSITIONS = {"midfielder", "winger", "centre-back", "defender"}


def fetch_match_lineups(match_id: str) -> dict:
    """Fetch confirmed starting lineups. Cost: 1 call."""
    if not match_id:
        return {"available": False}

    data = _get("/api/flashscore/v2/matches/match/lineups", {"match_id": match_id})

    if not isinstance(data, dict) or not data:
        return {"available": False}

    home_data = data.get("home_team") or data.get("home") or {}
    away_data = data.get("away_team") or data.get("away") or {}

    def parse_side(side_data: dict) -> dict:
        formation    = side_data.get("formation") or ""
        starters_raw = (
            side_data.get("starters") or
            side_data.get("lineup") or
            side_data.get("starting_lineup") or
            []
        )
        starters = []
        for p in starters_raw:
            pos = (p.get("position") or p.get("pos") or "").lower().strip()
            starters.append({
                "name":     p.get("name") or p.get("player_name") or "",
                "position": pos,
                "number":   p.get("number") or p.get("shirt_number") or 0,
            })
        return {
            "formation": formation,
            "starters":  starters,
            "count":     len(starters),
        }

    home_parsed = parse_side(home_data)
    away_parsed = parse_side(away_data)
    available   = home_parsed["count"] >= 11 and away_parsed["count"] >= 11

    return {
        "available": available,
        "home":      home_parsed,
        "away":      away_parsed,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. HEAD TO HEAD
# ══════════════════════════════════════════════════════════════════════════════

def fetch_head_to_head(match_id: str, *args, last: int = 8, **kwargs) -> List[dict]:
    """Cost: 1 call."""
    if args:
        try: last = int(args[0])
        except: pass
    if "last" in kwargs:
        try: last = int(kwargs["last"])
        except: pass
    if not match_id:
        return []

    data = _get("/api/flashscore/v2/matches/h2h", {"match_id": match_id})
    if not isinstance(data, list):
        return []

    results = []
    for item in data[:last]:
        scores = item.get("scores") or {}
        h = scores.get("home")
        a = scores.get("away")
        if h is None or a is None:
            continue
        try:
            h, a = int(h), int(a)
        except (ValueError, TypeError):
            continue
        results.append({
            "home_score":    h,
            "away_score":    a,
            "winner":        "home" if h > a else ("away" if a > h else "draw"),
            "total_corners": None,
        })

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 6. MATCH STATS — grading
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fixture_stats(match_id: str) -> dict:
    """Cost: 1 call per finished match."""
    if not match_id:
        return {}

    data = _get("/api/flashscore/v2/matches/match/stats", {"match_id": match_id})
    if not isinstance(data, dict):
        return {}

    stats_list = data.get("match") or []
    if not stats_list:
        return {}

    result = {
        "corner_kicks": 0,
        "home_corners": None,
        "away_corners": None,
        "yellow_cards": 0,
        "red_cards":    0,
        "xg_home":      0.0,
        "xg_away":      0.0,
        "xgot_home":    0.0,
        "xgot_away":    0.0,
        "shots_home":   0,
        "shots_away":   0,
    }

    seen_corner = seen_xg = seen_xgot = False

    for stat in stats_list:
        name = (stat.get("name") or "").lower().strip()
        h = stat.get("home_team", 0) or 0
        a = stat.get("away_team", 0) or 0
        try:
            if "corner" in name and not seen_corner:
                result["home_corners"] = int(h)
                result["away_corners"] = int(a)
                result["corner_kicks"] = int(h) + int(a)
                seen_corner = True
            elif "yellow card" in name:
                result["yellow_cards"] = int(h) + int(a)
            elif "red card" in name:
                result["red_cards"] = int(h) + int(a)
            elif "xg on target" in name and not seen_xgot:
                result["xgot_home"] = float(h)
                result["xgot_away"] = float(a)
                seen_xgot = True
            elif "expected goals" in name and not seen_xg:
                result["xg_home"] = float(h)
                result["xg_away"] = float(a)
                seen_xg = True
            elif "total shots" in name and result["shots_home"] == 0:
                result["shots_home"] = int(h)
                result["shots_away"] = int(a)
        except (TypeError, ValueError):
            pass

    return result


# ══════════════════════════════════════════════════════════════════════════════
# 7. TEAM RESULTS — with recency weighting
# ══════════════════════════════════════════════════════════════════════════════

def fetch_team_results(team_id: str, page: int = 1) -> List[dict]:
    """Cost: 1 call per page per team."""
    if not team_id:
        return []

    data = _get("/api/flashscore/v2/teams/results", {"team_id": team_id, "page": page})
    if not isinstance(data, list):
        return []

    matches = []
    for tournament_block in data:
        for match in tournament_block.get("matches", []):
            home   = match.get("home_team") or {}
            away   = match.get("away_team") or {}
            scores = match.get("scores") or {}
            matches.append({
                "match_id":     match.get("match_id"),
                "timestamp":    match.get("timestamp"),
                "home_team_id": home.get("team_id"),
                "away_team_id": away.get("team_id"),
                "home_score":   scores.get("home"),
                "away_score":   scores.get("away"),
                "is_home":      str(home.get("team_id") or "") == str(team_id),
            })
    return matches


def compute_team_stats_from_results(team_id: str, results: List[dict]) -> dict:
    """Compute team stats with exponential recency weighting."""
    home_games = [r for r in results if r.get("is_home")     and r.get("home_score") is not None]
    away_games = [r for r in results if not r.get("is_home") and r.get("away_score") is not None]

    def safe_int(v):
        try:   return int(v)
        except: return 0

    DECAY = 0.85

    def _weighted_rates(games, is_home):
        if not games:
            return {"win": 0.0, "draw": 0.0, "gf": 0.0, "ga": 0.0,
                    "rw_gf": 0.0, "rw_ga": 0.0, "btts": 0.0,
                    "cf": None, "ca": None}

        total_w = wins_w = draws_w = gf_w = ga_w = btts_w = 0.0
        cf_w = ca_w = corner_total_w = 0.0
        wins = draws = gf_sum = ga_sum = btts_hits = 0
        cf_sum = ca_sum = corner_games = 0

        for i, g in enumerate(games):
            w   = DECAY ** i
            h   = safe_int(g.get("home_score"))
            a   = safe_int(g.get("away_score"))
            gf  = h if is_home else a
            ga  = a if is_home else h
            won = (h > a and is_home) or (a > h and not is_home)
            drw = h == a
            bt  = h > 0 and a > 0

            total_w  += w
            wins_w   += w if won else 0
            draws_w  += w if drw else 0
            gf_w     += w * gf
            ga_w     += w * ga
            btts_w   += w if bt else 0

            wins      += 1 if won else 0
            draws     += 1 if drw else 0
            gf_sum    += gf
            ga_sum    += ga
            btts_hits += 1 if bt else 0

            hc = g.get("home_corners")
            ac = g.get("away_corners")
            if hc is not None and ac is not None:
                cf_raw = safe_int(hc) if is_home else safe_int(ac)
                ca_raw = safe_int(ac) if is_home else safe_int(hc)
                cf_w           += w * cf_raw
                ca_w           += w * ca_raw
                corner_total_w += w
                cf_sum         += cf_raw
                ca_sum         += ca_raw
                corner_games   += 1

        n = len(games)
        return {
            "win":   round(wins / n, 3),
            "draw":  round(draws / n, 3),
            "gf":    round(gf_sum / n, 2),
            "ga":    round(ga_sum / n, 2),
            "rw_gf": round(gf_w / total_w, 2),
            "rw_ga": round(ga_w / total_w, 2),
            "btts":  round(btts_hits / n, 3),
            "cf":    round(cf_w / corner_total_w, 2) if corner_total_w > 0 else None,
            "ca":    round(ca_w / corner_total_w, 2) if corner_total_w > 0 else None,
        }

    h = _weighted_rates(home_games, True)
    a = _weighted_rates(away_games, False)

    stats = {
        "games_played":           len(home_games) + len(away_games),
        "home_win_rate":          h["win"],
        "home_draw_rate":         h["draw"],
        "home_avg_goals_for":     h["gf"],
        "home_avg_goals_against": h["ga"],
        "away_win_rate":          a["win"],
        "away_draw_rate":         a["draw"],
        "away_avg_goals_for":     a["gf"],
        "away_avg_goals_against": a["ga"],
        "home_btts_rate":         h["btts"],
        "away_btts_rate":         a["btts"],
        "rw_home_goals_for":      h["rw_gf"],
        "rw_home_goals_against":  h["rw_ga"],
        "rw_away_goals_for":      a["rw_gf"],
        "rw_away_goals_against":  a["rw_ga"],
    }

    if h["cf"] is not None:
        stats["home_avg_corners_for"]     = h["cf"]
        stats["home_avg_corners_against"] = h["ca"]
    if a["cf"] is not None:
        stats["away_avg_corners_for"]     = a["cf"]
        stats["away_avg_corners_against"] = a["ca"]

    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 8. STANDINGS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_match_standings(match_id: str) -> List[dict]:
    """Cost: 1 call."""
    if not match_id:
        return []
    data = _get("/api/flashscore/v2/matches/standings", {"match_id": match_id, "type": "overall"})
    return data if isinstance(data, list) else []


# ══════════════════════════════════════════════════════════════════════════════
# 9. OVER/UNDER STANDINGS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_match_over_under(match_id: str, sub_type: str = "2.5") -> List[dict]:
    """Cost: 1 call."""
    if not match_id:
        return []
    data = _get("/api/flashscore/v2/matches/standings/over-under", {
        "match_id": match_id, "type": "overall", "sub_type": sub_type,
    })
    return data if isinstance(data, list) else []


# ══════════════════════════════════════════════════════════════════════════════
# 10. HT/FT STANDINGS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_match_ht_ft(match_id: str) -> List[dict]:
    """Cost: 1 call."""
    if not match_id:
        return []
    data = _get("/api/flashscore/v2/matches/standings/ht-ft", {
        "match_id": match_id, "type": "overall",
    })
    return data if isinstance(data, list) else []


# ══════════════════════════════════════════════════════════════════════════════
# 11. TOURNAMENT STANDINGS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tournament_over_under(tournament_id: str, tournament_stage_id: str, sub_type: str = "2.5") -> List[dict]:
    """Cost: 1 call."""
    if not tournament_id:
        return []
    data = _get("/api/flashscore/v2/tournaments/standings/over-under", {
        "tournament_id": tournament_id, "tournament_stage_id": tournament_stage_id,
        "type": "overall", "sub_type": sub_type,
    })
    return data if isinstance(data, list) else []


def fetch_tournament_ht_ft(tournament_id: str, tournament_stage_id: str) -> List[dict]:
    """Cost: 1 call."""
    if not tournament_id:
        return []
    data = _get("/api/flashscore/v2/tournaments/standings/ht-ft", {
        "tournament_id": tournament_id, "tournament_stage_id": tournament_stage_id,
        "type": "overall",
    })
    return data if isinstance(data, list) else []


# ══════════════════════════════════════════════════════════════════════════════
# 12. GRADE RESULTS helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fixtures_finished(date_str: str) -> List[dict]:
    """Targeted score update for the night pipeline. Cost: 1 call per unfinished fixture."""
    from fixtures.models import Fixture

    unfinished = Fixture.objects.filter(
        kickoff__date=date_str,
    ).exclude(status="finished")

    if not unfinished.exists():
        logger.info("[fetch_fixtures_finished] All fixtures already finished for %s", date_str)
        return []

    logger.info(
        "[fetch_fixtures_finished] Fetching scores for %d unfinished fixtures on %s",
        unfinished.count(), date_str,
    )

    results = []
    for fixture in unfinished:
        venue = fixture.venue or ""
        if not venue.startswith("fs:"):
            continue

        match_id = venue[3:]
        try:
            details = get_match_details(match_id)
            if not details:
                continue
            norm = normalize_match(details)
            if not norm.get("match_id"):
                continue
            norm.setdefault("league_api_id",  fixture.league.api_id if fixture.league else None)
            norm.setdefault("league_name",    fixture.league.name    if fixture.league else "")
            norm.setdefault("country_name",   fixture.league.country if fixture.league else "")
            norm.setdefault("league_tier",    fixture.league.tier    if fixture.league else 3)
            norm.setdefault("home_team_id",   "")
            norm.setdefault("away_team_id",   "")
            norm.setdefault("home_team_name", fixture.home_team.name if fixture.home_team else "")
            norm.setdefault("away_team_name", fixture.away_team.name if fixture.away_team else "")
            results.append(norm)
            time.sleep(0.4)
        except Exception as exc:
            logger.warning("fetch_fixtures_finished: error for match_id=%s: %s", match_id, exc)

    logger.info("[fetch_fixtures_finished] Retrieved %d score updates", len(results))
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 13. CORNER ODDS FALLBACK — Soccer Football Info API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_corner_odds_fallback(home_team: str, away_team: str, match_date: str) -> dict:
    """
    Fetch corner odds from Soccer Football Info API when FlashScore only
    returns a 6.5 line or nothing at all.
    Cost: 1 call. match_date: "YYYYMMDD" string.
    """
    if not home_team or not away_team or not match_date:
        return {}

    data = _si_get("/matches/day/full/", {
        "d": match_date,
        "p": "1",
        "l": "en_US",
    })

    if not isinstance(data, dict):
        return {}

    matches = data.get("result") or []
    if not isinstance(matches, list):
        return {}

    def _norm(name: str) -> str:
        import re
        return re.sub(r"[^a-z0-9]", "", name.lower())

    def _partial_match(a: str, b: str) -> bool:
        if a in b or b in a:
            return True
        min_len = min(len(a), len(b))
        if min_len >= 5 and a[:5] == b[:5]:
            return True
        return False

    home_norm = _norm(home_team)
    away_norm = _norm(away_team)
    best_match = None
    best_score = 0

    for m in matches:
        team_a = _norm(m.get("teamA", {}).get("name", ""))
        team_b = _norm(m.get("teamB", {}).get("name", ""))

        score = 0
        if _partial_match(home_norm, team_a): score += 2
        if _partial_match(away_norm, team_b): score += 2
        if _partial_match(home_norm, team_b): score += 1
        if _partial_match(away_norm, team_a): score += 1

        if score > best_score:
            best_score = score
            best_match = m

    if best_score < 3 or best_match is None:
        logger.debug(
            "[SoccerInfo] No corner odds match found for %s vs %s on %s (best_score=%d)",
            home_team, away_team, match_date, best_score,
        )
        return {}

    odds          = best_match.get("odds") or {}
    live_odds     = odds.get("live") or {}
    starting_odds = odds.get("starting") or {}
    corner_data   = live_odds.get("asian_corner") or starting_odds.get("asian_corner")

    if not corner_data:
        logger.debug(
            "[SoccerInfo] Match found (%s vs %s) but no asian_corner odds",
            best_match.get("teamA", {}).get("name"),
            best_match.get("teamB", {}).get("name"),
        )
        return {}

    line_str = str(corner_data.get("v") or "").strip()
    over_v   = _safe_float(corner_data.get("o"))
    under_v  = _safe_float(corner_data.get("u"))

    if not line_str or over_v is None or under_v is None:
        return {}

    logger.info(
        "[SoccerInfo] Corner odds found: %s vs %s → line=%s over=%.3f under=%.3f",
        home_team, away_team, line_str, over_v, under_v,
    )

    return {line_str: {"over": over_v, "under": under_v}}


# ══════════════════════════════════════════════════════════════════════════════
# BACKWARD COMPAT STUBS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_team_statistics(team_id, league_id, season) -> dict:
    logger.debug("fetch_team_statistics deprecated — use fetch_team_results")
    return {}


def get_match_details(match_id: str) -> Optional[dict]:
    if not match_id:
        return None
    data = _get("/api/flashscore/v2/matches/details", {"match_id": match_id})
    return data if isinstance(data, dict) and data.get("match_id") else None


def normalize_match(details: dict) -> dict:
    if not isinstance(details, dict):
        return {}
    home_team  = details.get("home_team") or {}
    away_team  = details.get("away_team") or {}
    tournament = details.get("tournament") or {}
    scores     = details.get("scores") or {}
    status_d   = details.get("match_status") or {}
    country    = details.get("country") or {}
    ts = details.get("timestamp")
    kickoff = None
    if ts:
        try: kickoff = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except: pass
    t_url       = tournament.get("tournament_url", "")
    league_info = _resolve_league_info(t_url)
    return {
        "match_id":        details.get("match_id"),
        "kickoff":         kickoff,
        "status":          _resolve_status(status_d),
        "league_name":     tournament.get("name", ""),
        "country_name":    country.get("name", ""),
        "league_api_id":   league_info["api_id"] if league_info else None,
        "league_tier":     league_info["tier"] if league_info else 3,
        "home_team_id":    home_team.get("team_id", "") or home_team.get("id", ""),
        "home_team_name":  (home_team.get("name") or home_team.get("short_name") or
                            home_team.get("shortName") or home_team.get("title") or
                            home_team.get("participant_name") or ""),
        "away_team_id":    away_team.get("team_id", "") or away_team.get("id", ""),
        "away_team_name":  (away_team.get("name") or away_team.get("short_name") or
                            away_team.get("shortName") or away_team.get("title") or
                            away_team.get("participant_name") or ""),
        "home_score":      scores.get("home"),
        "away_score":      scores.get("away"),
        "venue":           (details.get("venue") or {}).get("name", ""),
        "referee":         details.get("referee", ""),
    }
