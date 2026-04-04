"""
seed_match_ids — Manual match seeding command
=============================================

Since /matches/list is unavailable on the free RapidAPI plan,
you seed today's match IDs manually from FlashScore URLs.

HOW TO GET MATCH IDs:
  1. Go to flashscore.com
  2. Click any football match
  3. The URL is: flashscore.com/match/GCxZ2uHc/#/...
  4. The ID is the 8-character code: GCxZ2uHc
  5. Collect 50+ IDs from today's big leagues
  6. Run: python manage.py seed_match_ids GCxZ2uHc Ab1Cd2Ef ...

TYPICAL USAGE (morning, before run_predictions):
  python manage.py seed_match_ids <id1> <id2> ... <id50+>

The command will:
  - Call get_match_details() for each ID (1 API call each)
  - Save League, Team, Referee, Fixture to DB
  - Enrich teams with scraper data (Understat + FBref)
  - Ready for run_predictions immediately after
"""

import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from fixtures.models import League, Team, Fixture, Referee
from fixtures import api_client

try:
    from fixtures.scraper_client import scraper_client
except Exception:
    scraper_client = None

logger = logging.getLogger(__name__)

# Map tournament_url patterns to known league DB entries
# Add more as you discover tournament_urls from the API
TOURNAMENT_URL_TO_LEAGUE = {
    "/football/england/premier-league/":   {"name": "Premier League",  "country": "England", "tier": 1, "fake_api_id": 39},
    "/football/spain/laliga/":             {"name": "La Liga",         "country": "Spain",   "tier": 1, "fake_api_id": 140},
    "/football/italy/serie-a/":            {"name": "Serie A",         "country": "Italy",   "tier": 1, "fake_api_id": 135},
    "/football/germany/bundesliga/":       {"name": "Bundesliga",      "country": "Germany", "tier": 1, "fake_api_id": 78},
    "/football/france/ligue-1/":           {"name": "Ligue 1",         "country": "France",  "tier": 1, "fake_api_id": 61},
    "/football/europe/champions-league/":  {"name": "Champions League","country": "Europe",  "tier": 1, "fake_api_id": 2},
    "/football/south-africa/psl/":         {"name": "PSL",             "country": "South Africa", "tier": 1, "fake_api_id": 288},
    "/football/portugal/liga-portugal/":   {"name": "Liga Portugal",   "country": "Portugal","tier": 2, "fake_api_id": 94},
    "/football/netherlands/eredivisie/":   {"name": "Eredivisie",      "country": "Netherlands","tier": 2, "fake_api_id": 88},
    "/football/turkey/super-lig/":         {"name": "Super Lig",       "country": "Turkey",  "tier": 2, "fake_api_id": 203},
    "/football/spain/laliga2/":            {"name": "Segunda División","country": "Spain",   "tier": 2, "fake_api_id": 141},
    "/football/france/ligue-2/":           {"name": "Ligue 2",         "country": "France",  "tier": 2, "fake_api_id": 62},
}


class Command(BaseCommand):
    help = "Seed today's fixtures by providing FlashScore match IDs directly"

    def add_arguments(self, parser):
        parser.add_argument(
            "match_ids", nargs="+", type=str,
            help="FlashScore match IDs (e.g. GCxZ2uHc). Get from flashscore.com/match/<ID>",
        )
        parser.add_argument(
            "--enrich", action="store_true", default=True,
            help="Enrich teams with scraper data after seeding (default: True)",
        )
        parser.add_argument(
            "--no-enrich", action="store_false", dest="enrich",
            help="Skip scraper enrichment",
        )

    def handle(self, *args, **options):
        match_ids = list(dict.fromkeys(options["match_ids"]))  # dedupe, preserve order
        do_enrich = options["enrich"]

        self.stdout.write(f"=== Seeding {len(match_ids)} match IDs ===")
        self.stdout.write(f"API quota: each ID = 1 call. You have ~500/month on free plan.")

        saved = 0
        skipped = 0
        enriched_count = 0
        teams_to_enrich = []

        for i, match_id in enumerate(match_ids, 1):
            self.stdout.write(f"[{i}/{len(match_ids)}] Fetching {match_id}...")
            details = api_client.get_match_details(match_id)

            if not details:
                self.stdout.write(f"  ❌ No data returned for {match_id}")
                skipped += 1
                continue

            norm = api_client.normalize_match(details)
            if not norm.get("match_id"):
                self.stdout.write(f"  ❌ Could not normalize {match_id}")
                skipped += 1
                continue

            try:
                league = self._get_or_create_league(norm)
                if not league:
                    self.stdout.write(f"  ⏭  Skipping {match_id} — unknown league: {norm.get('tournament_url')}")
                    skipped += 1
                    continue

                home_team = self._get_or_create_team(
                    norm["home_team_id"], norm["home_team_name"], league
                )
                away_team = self._get_or_create_team(
                    norm["away_team_id"], norm["away_team_name"], league
                )
                referee = self._get_or_create_referee(norm.get("referee", ""))

                fixture, created = Fixture.objects.update_or_create(
                    api_id=self._stable_id(norm["match_id"]),
                    defaults={
                        "league":     league,
                        "home_team":  home_team,
                        "away_team":  away_team,
                        "kickoff":    norm["kickoff"] or timezone.now(),
                        "referee":    referee,
                        "venue":      norm.get("venue", ""),
                        "status":     norm["status"],
                        "home_score": norm.get("home_score"),
                        "away_score": norm.get("away_score"),
                    },
                )

                action = "Created" if created else "Updated"
                self.stdout.write(
                    f"  ✅ {action}: {home_team.name} vs {away_team.name} "
                    f"({league.name}) [{norm['status']}]"
                )
                saved += 1

                if do_enrich and league.tier in (1, 2):
                    teams_to_enrich.append((home_team, league))
                    teams_to_enrich.append((away_team, league))

            except Exception as exc:
                logger.error("Error processing %s: %s", match_id, exc)
                self.stdout.write(f"  ❌ Error: {exc}")
                skipped += 1

        # Enrich teams with scraper data
        if do_enrich and scraper_client and teams_to_enrich:
            seen = set()
            unique_teams = []
            for team, league in teams_to_enrich:
                if team.id not in seen:
                    seen.add(team.id)
                    unique_teams.append((team, league))

            self.stdout.write(f"\n=== Enriching {len(unique_teams)} teams via scraper ===")
            for team, league in unique_teams:
                try:
                    scraped = scraper_client.enrich_team(
                        team.name,
                        league_name=league.name,
                        league_api_id=league.api_id,
                    )
                    if scraped and scraped.get("games_played", 0) >= 3:
                        self._apply_scraped_stats(team, scraped)
                        self.stdout.write(f"  ✅ {team.name}: {scraped['games_played']} games, source={scraped.get('source','?')}")
                        enriched_count += 1
                    else:
                        self.stdout.write(f"  ⚠️  {team.name}: insufficient data")
                except Exception as exc:
                    self.stdout.write(f"  ❌ {team.name}: {exc}")
        elif do_enrich and not scraper_client:
            self.stdout.write("⚠️  Scraper not available — teams will use DB defaults")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"Done: {saved} fixtures saved, {skipped} skipped, {enriched_count} teams enriched"
        ))
        self.stdout.write(
            f"Next: python manage.py run_predictions"
        )

    def _stable_id(self, flashscore_id: str) -> int:
        """Convert FlashScore string ID to a stable integer for DB api_id field."""
        return abs(hash(flashscore_id)) % (10 ** 9)

    def _get_or_create_league(self, norm: dict):
        """Match tournament_url to a known league, or create an unknown one."""
        url = norm.get("tournament_url", "")

        # Strip round info from URL (e.g. /football/spain/laliga/ from /football/spain/laliga/round-37/)
        base_url = "/".join(url.rstrip("/").split("/")[:4]) + "/" if url else ""

        known = TOURNAMENT_URL_TO_LEAGUE.get(url) or TOURNAMENT_URL_TO_LEAGUE.get(base_url)

        if known:
            league, _ = League.objects.update_or_create(
                api_id=known["fake_api_id"],
                defaults={
                    "name":    known["name"],
                    "country": known["country"],
                    "tier":    known["tier"],
                    "active":  True,
                },
            )
            return league

        # Unknown league — create with tier 3 (won't be predicted)
        league_name = norm.get("league_name") or norm.get("tournament_url") or "Unknown"
        # Clean round info: "LaLiga - Round 37" → "LaLiga"
        league_name = league_name.split(" - Round")[0].split(" - Matchday")[0].strip()
        country = norm.get("country_name", "Unknown")
        fake_id = abs(hash(f"{league_name}:{country}")) % (10 ** 9)

        league, created = League.objects.get_or_create(
            api_id=fake_id,
            defaults={
                "name":    league_name,
                "country": country,
                "tier":    3,
                "active":  False,
            },
        )
        if created:
            logger.info("Created unknown league: %s (%s) — tier 3, inactive", league_name, country)
        return league

    def _get_or_create_team(self, team_id: str, team_name: str, league) -> Team:
        fake_int_id = abs(hash(team_id)) % (10 ** 9)
        team, created = Team.objects.get_or_create(
            api_id=fake_int_id,
            defaults={"name": team_name, "league": league},
        )
        if not created and team.name != team_name:
            team.name = team_name
            team.save(update_fields=["name"])
        if not created and not team.league:
            team.league = league
            team.save(update_fields=["league"])
        return team

    def _get_or_create_referee(self, ref_name: str):
        if not ref_name:
            return None
        name = ref_name.split(",")[0].strip()
        if not name:
            return None
        referee, _ = Referee.objects.get_or_create(name=name)
        return referee

    def _apply_scraped_stats(self, team: Team, scraped: dict):
        fields = {
            "games_played":              int(scraped.get("games_played") or 0),
            "home_avg_goals_for":        round(float(scraped.get("home_avg_goals_for") or 0), 2),
            "home_avg_goals_against":    round(float(scraped.get("home_avg_goals_against") or 0), 2),
            "away_avg_goals_for":        round(float(scraped.get("away_avg_goals_for") or 0), 2),
            "away_avg_goals_against":    round(float(scraped.get("away_avg_goals_against") or 0), 2),
            "home_avg_corners_for":      round(float(scraped.get("home_avg_corners_for") or 0), 2),
            "home_avg_corners_against":  round(float(scraped.get("home_avg_corners_against") or 0), 2),
            "away_avg_corners_for":      round(float(scraped.get("away_avg_corners_for") or 0), 2),
            "away_avg_corners_against":  round(float(scraped.get("away_avg_corners_against") or 0), 2),
            "home_avg_cards":            round(float(scraped.get("home_avg_cards") or 0), 2),
            "away_avg_cards":            round(float(scraped.get("away_avg_cards") or 0), 2),
            "home_win_rate":             round(float(scraped.get("home_win_rate") or 0), 3),
            "home_draw_rate":            round(float(scraped.get("home_draw_rate") or 0), 3),
            "away_win_rate":             round(float(scraped.get("away_win_rate") or 0), 3),
            "away_draw_rate":            round(float(scraped.get("away_draw_rate") or 0), 3),
            "home_xg_for":               round(float(scraped.get("home_xg_for") or 0), 3),
            "home_xg_against":           round(float(scraped.get("home_xg_against") or 0), 3),
            "away_xg_for":               round(float(scraped.get("away_xg_for") or 0), 3),
            "away_xg_against":           round(float(scraped.get("away_xg_against") or 0), 3),
            "home_btts_rate":            round(float(scraped.get("home_btts_rate") or 0), 3),
            "away_btts_rate":            round(float(scraped.get("away_btts_rate") or 0), 3),
            "scraper_source":            scraped.get("source", ""),
            "scraper_updated_at":        timezone.now(),
        }
        for field, value in fields.items():
            setattr(team, field, value)
        team.save()
