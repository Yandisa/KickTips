import logging
import time
from datetime import date, datetime, timedelta, timezone

from django.core.management.base import BaseCommand
from django.db.models import Q

from fixtures.models import Fixture, Team
from fixtures import api_client

logger = logging.getLogger(__name__)

MIN_MATCHES = 10    # minimum historical matches we want per team in DB
STAT_LIMIT  = 12    # per-match stat API calls per team (corners/xG/cards)
LOOK_AHEAD  = 7    # days ahead to scan for upcoming fixtures

DELAY_STAT  = 1  # seconds between match stat calls
DELAY_TEAM  = 1  # seconds between teams


class Command(BaseCommand):
    help = "Fetch match history for teams playing in the next 7 days (run daily before predictions)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--days", type=int, default=LOOK_AHEAD,
            help=f"Days ahead to scan for upcoming fixtures (default {LOOK_AHEAD})",
        )
        parser.add_argument(
            "--min", type=int, default=MIN_MATCHES,
            help=f"Minimum historical matches required per team (default {MIN_MATCHES})",
        )
        parser.add_argument(
            "--force", action="store_true", default=False,
            help="Re-fetch even for teams that already have enough history",
        )
        parser.add_argument(
            "--recompute", action="store_true", default=False,
            help="Skip API fetch — just recompute Team stats from existing Fixture rows",
        )

    def handle(self, *args, **kwargs):
        days    = kwargs["days"]
        min_req = kwargs["min"]
        force   = kwargs["force"]
        recompute = kwargs["recompute"]

        today    = date.today()
        end_date = today + timedelta(days=days)

        self.stdout.write(
            f"=== fetch_history: teams playing {today} to {end_date} "
            f"| min {min_req} matches | force={force} ==="
        )

        # Find all teams with upcoming scheduled fixtures in the window
        upcoming_fixtures = Fixture.objects.filter(
            kickoff__date__gte=today,
            kickoff__date__lte=end_date,
            league__active=True,
            status="scheduled",
        ).select_related("home_team", "away_team", "league")

        # Deduplicate into unique teams
        teams_map = {}
        for fix in upcoming_fixtures:
            for team in (fix.home_team, fix.away_team):
                if team and team.pk not in teams_map:
                    teams_map[team.pk] = team

        teams = list(teams_map.values())

        if not teams:
            self.stdout.write("No upcoming fixtures found in DB — nothing to do.")
            return

        self.stdout.write(
            f"Found {len(teams)} unique teams with fixtures in the next {days} days"
        )

        fetched = skipped = failed = stats_total = 0

        if not recompute:
            for i, team in enumerate(teams, 1):
                existing = self._count_history(team)

                if not force and existing >= min_req:
                    self.stdout.write(
                        f"[{i:2d}/{len(teams)}] {team.name[:28]:<28} "
                        f"{existing:2d} matches in DB — skip"
                    )
                    skipped += 1
                    continue

                self.stdout.write(
                    f"[{i:2d}/{len(teams)}] {team.name[:28]:<28} "
                    f"{existing:2d} in DB — fetching...",
                    ending="",
                )
                self.stdout.flush()

                fs_id = self._resolve_fs_team_id(team)
                if not fs_id:
                    self.stdout.write(" no FlashScore ID — skip")
                    failed += 1
                    continue

                try:
                    n_saved, n_stats = self._fetch_and_store(
                        team, fs_id, min_req, existing
                    )
                    fetched     += n_saved
                    stats_total += n_stats
                    new_total    = existing + n_saved
                    self.stdout.write(
                        f" +{n_saved} fixtures ({new_total} total), {n_stats} stats"
                    )
                except Exception as exc:
                    logger.error("fetch_history failed for %s: %s", team.name, exc)
                    self.stdout.write(f" ERROR: {exc}")
                    failed += 1

                if i < len(teams):
                    time.sleep(DELAY_TEAM)

        # Recompute stats for all teams that now have enough history
        qualified = [t for t in teams if self._count_history(t) >= min_req]
        if qualified:
            self.stdout.write(f"\nRecomputing stats for {len(qualified)} teams...")
            ok, bad = self._recompute_stats(qualified, min_req)
            self.stdout.write(f"Stats: {ok} updated, {bad} skipped")

        self.stdout.write(self.style.SUCCESS(
            f"\nfetch_history done — "
            f"{fetched} new fixtures | {stats_total} stat-enriched | "
            f"{skipped} already had history | {failed} failed"
        ))

    # ── Fetch and store ───────────────────────────────────────────────────────

    def _fetch_and_store(self, team, fs_team_id, min_req, already_have):
        """
        Fetch page 1 of team results (~20 matches, 1 API call).
        Upsert each result as a Fixture row.
        Enrich the newest ones with corners/xG/cards.
        Returns (fixtures_saved, stats_enriched).
        """
        try:
            import time as _time
            raw = api_client.fetch_team_results(fs_team_id, page=1)
            _time.sleep(0.4)
            raw += api_client.fetch_team_results(fs_team_id, page=2)
            _time.sleep(0.4)
            raw += api_client.fetch_team_results(fs_team_id, page=3)
        except Exception as exc:
            logger.warning("fetch_team_results failed %s: %s", team.name, exc)
            return 0, 0

        if not raw:
            return 0, 0

        saved_fixtures = []

        for result in raw:
            match_id   = result.get("match_id")
            home_score = result.get("home_score")
            away_score = result.get("away_score")
            timestamp  = result.get("timestamp")
            home_tid   = result.get("home_team_id")
            away_tid   = result.get("away_team_id")

            if not match_id or home_score is None or away_score is None:
                continue

            try:
                fixture = self._upsert_fixture(
                    match_id     = match_id,
                    home_team_id = home_tid,
                    away_team_id = away_tid,
                    home_score   = int(home_score),
                    away_score   = int(away_score),
                    timestamp    = timestamp,
                    league       = team.league,
                )
                if fixture:
                    saved_fixtures.append((fixture, match_id))
            except Exception as exc:
                logger.debug("Upsert failed match=%s: %s", match_id, exc)

        # Enrich newest fixtures with match stats (corners / xG / cards)
        stats_enriched = 0
        for fixture, fs_match_id in saved_fixtures[:STAT_LIMIT]:
            if fixture.total_corners is not None:
                continue  # already enriched
            try:
                stats = api_client.fetch_fixture_stats(fs_match_id)
                if stats:
                    update_fields = []
                    corners = stats.get("corner_kicks")
                    cards   = (
                        (stats.get("yellow_cards") or 0) +
                        (stats.get("red_cards") or 0)
                    )
                    if corners is not None:
                        fixture.total_corners = corners
                        update_fields.append("total_corners")
                    if cards:
                        fixture.total_cards = cards
                        update_fields.append("total_cards")
                    if update_fields:
                        fixture.save(update_fields=update_fields)
                        stats_enriched += 1
                time.sleep(DELAY_STAT)
            except Exception as exc:
                logger.debug("Stats failed match=%s: %s", fs_match_id, exc)

        return len(saved_fixtures), stats_enriched

    def _upsert_fixture(
        self, match_id, home_team_id, away_team_id,
        home_score, away_score, timestamp, league,
    ):
        """
        Insert or update a historical Fixture row.
        Dedup key: same home+away team pair on same calendar date.
        """
        stable_api_id = api_client._stable_id(match_id)
        venue_str     = f"fs:{match_id}"

        kickoff = None
        if timestamp:
            try:
                kickoff = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                pass
        if not kickoff:
            return None

        home_team = self._get_or_create_stub(home_team_id, league)
        away_team = self._get_or_create_stub(away_team_id, league)
        if not home_team or not away_team:
            return None

        # Dedup: same home+away+date
        existing = Fixture.objects.filter(
            home_team=home_team,
            away_team=away_team,
            kickoff__date=kickoff.date(),
        ).first()

        if existing:
            changed = False
            if existing.home_score is None:
                existing.home_score = home_score
                changed = True
            if existing.away_score is None:
                existing.away_score = away_score
                changed = True
            if existing.status != "finished":
                existing.status = "finished"
                changed = True
            if changed:
                existing.save(update_fields=["home_score", "away_score", "status"])
            return existing

        fixture, _ = Fixture.objects.get_or_create(
            api_id=stable_api_id,
            defaults={
                "league":     league,
                "home_team":  home_team,
                "away_team":  away_team,
                "kickoff":    kickoff,
                "venue":      venue_str,
                "status":     "finished",
                "home_score": home_score,
                "away_score": away_score,
                "graded":     False,  # historical — not prediction-graded
            },
        )
        return fixture

    def _get_or_create_stub(self, fs_team_id, league):
        """Get or create a minimal Team stub for a FlashScore team_id string."""
        if not fs_team_id:
            return None
        stable_id = api_client._stable_id(fs_team_id)
        team = Team.objects.filter(api_id=stable_id).first()
        if team:
            return team
        team, _ = Team.objects.get_or_create(
            api_id=stable_id,
            defaults={
                "name":           f"Unknown_{fs_team_id[:8]}",
                "league":         league,
                "scraper_source": f"fs:{fs_team_id}",
            },
        )
        return team

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _count_history(self, team):
        """Count finished Fixture rows in DB for this team."""
        return Fixture.objects.filter(
            Q(home_team=team) | Q(away_team=team),
            status="finished",
            home_score__isnull=False,
            away_score__isnull=False,
        ).count()

    def _resolve_fs_team_id(self, team):
        """
        Get the original FlashScore string team_id for a Team.
        1. Check scraper_source (set during get_or_create or cached here)
        2. Find any fixture with fs: venue for this team, fetch match details
        """
        src = team.scraper_source or ""
        if src.startswith("fs:"):
            return src[3:]

        # Fallback: find a fixture for this team with a known fs: match_id
        fixture = (
            Fixture.objects.filter(
                Q(home_team=team) | Q(away_team=team),
                venue__startswith="fs:",
            )
            .order_by("-kickoff")
            .first()
        )
        if not fixture:
            return None

        match_id = fixture.venue[3:]
        try:
            details = api_client._get(
                "/api/flashscore/v2/matches/details", {"match_id": match_id}
            )
            if not isinstance(details, dict):
                return None
            is_home = (fixture.home_team_id == team.pk)
            key     = "home_team" if is_home else "away_team"
            fs_id   = (details.get(key) or {}).get("team_id", "")
            if fs_id:
                # Cache it so we never need to look it up again
                team.scraper_source = f"fs:{fs_id}"
                team.save(update_fields=["scraper_source"])
            return fs_id or None
        except Exception as exc:
            logger.warning("Could not resolve fs team_id for %s: %s", team.name, exc)
            return None

    # ── Recompute Team stats from stored Fixture rows ─────────────────────────

    def _recompute_stats(self, teams, min_req):
        ok = skipped = 0
        for team in teams:
            results = self._fixtures_to_results(team)
            if len(results) < min_req:
                skipped += 1
                continue
            try:
                stats = api_client.compute_team_stats_from_results(
                    str(team.api_id), results
                )
                if stats.get("games_played", 0) >= min_req:
                    for field, value in stats.items():
                        if hasattr(team, field):
                            setattr(team, field, value)
                    team.save()
                    ok += 1
                    logger.info(
                        "[stats] %-28s %2d games | "
                        "home GF %.2f GA %.2f | away GF %.2f GA %.2f",
                        team.name, stats["games_played"],
                        stats.get("home_avg_goals_for", 0),
                        stats.get("home_avg_goals_against", 0),
                        stats.get("away_avg_goals_for", 0),
                        stats.get("away_avg_goals_against", 0),
                    )
                else:
                    skipped += 1
            except Exception as exc:
                logger.error("Recompute failed %s: %s", team.name, exc)
                skipped += 1
        return ok, skipped

    def _fixtures_to_results(self, team):
        """
        Convert DB Fixture rows to the dict shape compute_team_stats_from_results expects.
        Filters to team's own league only — prevents Champions League/Europa League
        away matches corrupting domestic home/away averages (e.g. Salzburg 0-5 Bayern
        being counted as a home Austrian Bundesliga result).
        """
        qs = Fixture.objects.filter(
            Q(home_team=team) | Q(away_team=team),
            status="finished",
            home_score__isnull=False,
            away_score__isnull=False,
        ).order_by("-kickoff")

        if team.league_id and team.league.is_domestic:
            qs = qs.filter(league=team.league)

        fixtures = list(qs[:30])
        results = []
        for f in fixtures:
            is_home = (f.home_team_id == team.pk)
            hc = ac = None
            if f.total_corners is not None:
                hc = f.total_corners // 2
                ac = f.total_corners - hc
            results.append({
                "match_id":     f.venue[3:] if (f.venue or "").startswith("fs:") else str(f.api_id),
                "timestamp":    int(f.kickoff.timestamp()) if f.kickoff else 0,
                "home_team_id": str(team.api_id) if is_home else "other",
                "away_team_id": "other" if is_home else str(team.api_id),
                "home_score":   f.home_score,
                "away_score":   f.away_score,
                "is_home":      is_home,
                "home_corners": hc,
                "away_corners": ac,
            })
        return results
