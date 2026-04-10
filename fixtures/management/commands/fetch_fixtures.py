"""
fetch_fixtures — Morning pipeline (v2)
=======================================
What's new vs v1:
  - Fetches form, standings, O/U rates, HT/FT, lineups per fixture
  - Fetches team results page 2 for recency weighting
  - Stores form strings and league position on Team model
  - Stores lineup key-player count on Team model
  - Deduplication guard unchanged
"""

import logging
import random
import time
from datetime import date, timedelta
from typing import Optional

from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from fixtures.models import Fixture, League, Referee, Team
from fixtures import api_client

logger = logging.getLogger(__name__)

MAX_TEAM_STAT_UPDATES = 50
MAX_ENRICHMENT_PER_RUN = 25
ENRICHMENT_DELAY = 1.5
TEAM_RESULTS_DELAY = 1.0
MATCH_STATS_DELAY = 1.0


class Command(BaseCommand):
    help = "Fetch fixtures (today or tomorrow), enrich team stats, fetch odds/form/lineups"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tomorrow",
            action="store_true",
            default=False,
            help="Fetch tomorrow's fixtures instead of today's (use for the 20:00 evening pipeline)",
        )

    def handle(self, *args, **kwargs):
        fetch_tomorrow = kwargs.get("tomorrow", False)
        target_date = date.today() + timedelta(days=1) if fetch_tomorrow else date.today()
        target_str = target_date.strftime("%Y-%m-%d")
        api_day = 1 if fetch_tomorrow else 0

        label = "Evening Pipeline v2 (tomorrow)" if fetch_tomorrow else "Morning Pipeline v2"
        self.stdout.write(f"=== {label}: {target_str} ===")

        removed = self._cleanup_duplicate_fixtures(target_str)
        if removed:
            self.stdout.write(f"Removed {removed} duplicate fixture rows")

        fixtures_data = api_client.fetch_fixtures_by_date(target_str, day=api_day)
        if not fixtures_data:
            self.stdout.write(f"No fixtures found for {target_str}.")
            return

        self.stdout.write(f"Found {len(fixtures_data)} fixtures")

        saved = skipped = dupe_skips = 0
        teams_to_update = []
        fixtures_to_enrich = []
        seen_ids = set()

        for item in fixtures_data:
            match_id = (item.get("match_id") or "").strip()
            if not match_id or match_id in seen_ids:
                dupe_skips += 1 if match_id in seen_ids else 0
                skipped += 1
                continue
            seen_ids.add(match_id)

            try:
                league = self._get_or_create_league(item)
                fixture = self._save_fixture(item, league)
                if fixture:
                    saved += 1
                    if league.active:
                        teams_to_update.append((item["home_team_id"], item["home_team_name"], league))
                        teams_to_update.append((item["away_team_id"], item["away_team_name"], league))
                        fixtures_to_enrich.append((fixture, item))
                else:
                    skipped += 1
            except Exception as exc:
                skipped += 1
                logger.error("Error processing fixture %s: %s", match_id, exc)

        self.stdout.write(f"Saved {saved}, skipped {skipped}, payload dupes {dupe_skips}")

        unique_teams = self._deduplicate_teams(teams_to_update)

        qualified_teams = [
            (tid, tname, lg)
            for tid, tname, lg in unique_teams
            if self._league_has_data(lg)
        ]
        random.shuffle(qualified_teams)
        limited_teams = qualified_teams[:MAX_TEAM_STAT_UPDATES]
        skipped_teams = len(unique_teams) - len(qualified_teams)
        deferred_teams = max(0, len(qualified_teams) - len(limited_teams))

        self.stdout.write(
            f"Updating stats for {len(limited_teams)} teams "
            f"({skipped_teams} skipped — leagues lack finished history, "
            f"{deferred_teams} deferred by rate limit guard)"
        )

        ok = fail = 0
        for team_id, team_name, league in limited_teams:
            if self._update_team_stats(team_id, team_name, league):
                ok += 1
            else:
                fail += 1
        self.stdout.write(f"Stats: {ok} OK, {fail} failed")

        qualified_enrichments = [
            (fixture, item)
            for fixture, item in fixtures_to_enrich
            if (fixture.venue or "").startswith("fs:")
            and self._league_has_data(fixture.league)
        ]
        random.shuffle(qualified_enrichments)
        limited_enrichments = qualified_enrichments[:MAX_ENRICHMENT_PER_RUN]

        self.stdout.write(
            f"Enriching {len(limited_enrichments)} fixtures "
            f"(of {len(fixtures_to_enrich)} total, {len(qualified_enrichments)} qualified)..."
        )

        ef_ok = ef_fail = 0
        for fixture, item in limited_enrichments:
            try:
                self._enrich_fixture(fixture, item)
                ef_ok += 1
            except Exception as exc:
                ef_fail += 1
                logger.error("Enrichment failed for %s: %s", fixture, exc)
        self.stdout.write(f"Enrichment: {ef_ok} OK, {ef_fail} failed")

        self.stdout.write(self.style.SUCCESS("Morning pipeline v2 complete ✅"))

    def _enrich_fixture(self, fixture: "Fixture", item: dict):
        venue = fixture.venue or ""
        if not venue.startswith("fs:"):
            return
        match_id = venue[3:]

        home_team_id = item.get("home_team_id", "")
        away_team_id = item.get("away_team_id", "")

        try:
            form_data = api_client.fetch_match_standings_form(match_id)
            if form_data:
                self._apply_form(fixture.home_team, home_team_id, form_data)
                self._apply_form(fixture.away_team, away_team_id, form_data)
        except Exception as exc:
            logger.debug("Form fetch failed for %s: %s", match_id, exc)
        time.sleep(ENRICHMENT_DELAY)

        try:
            standings = api_client.fetch_match_standings(match_id)
            if standings:
                self._apply_standings(fixture.home_team, home_team_id, standings)
                self._apply_standings(fixture.away_team, away_team_id, standings)
        except Exception as exc:
            logger.debug("Standings fetch failed for %s: %s", match_id, exc)
        time.sleep(ENRICHMENT_DELAY)

        for sub_type in ("1.5", "2.5", "3.5"):
            try:
                ou_data = api_client.fetch_match_over_under(match_id, sub_type=sub_type)
                if ou_data:
                    self._apply_ou_rates(fixture.home_team, home_team_id, ou_data, sub_type)
                    self._apply_ou_rates(fixture.away_team, away_team_id, ou_data, sub_type)
            except Exception as exc:
                logger.debug("O/U %s fetch failed for %s: %s", sub_type, match_id, exc)
            time.sleep(ENRICHMENT_DELAY)

        try:
            htft_data = api_client.fetch_match_ht_ft(match_id)
            if htft_data:
                self._apply_htft(fixture.home_team, home_team_id, htft_data)
                self._apply_htft(fixture.away_team, away_team_id, htft_data)
        except Exception as exc:
            logger.debug("HT/FT fetch failed for %s: %s", match_id, exc)
        time.sleep(ENRICHMENT_DELAY)

        try:
            lineups = api_client.fetch_match_lineups(match_id)
            if lineups.get("available"):
                self._apply_lineups(fixture.home_team, lineups.get("home", {}))
                self._apply_lineups(fixture.away_team, lineups.get("away", {}))
                logger.info("Lineups confirmed for %s", fixture)
            else:
                logger.debug("Lineups not yet available for %s", fixture)
        except Exception as exc:
            logger.debug("Lineups fetch failed for %s: %s", match_id, exc)

        now = timezone.now()
        for team in (fixture.home_team, fixture.away_team):
            team.enriched_at = now
            team.save(update_fields=["enriched_at"])

    def _apply_form(self, team: "Team", fs_team_id: str, form_data: dict):
        from fixtures.models import Fixture as Fix
        row = form_data.get(str(fs_team_id))
        if not row:
            return
        form_str = row.get("form", "")
        if not form_str:
            return

        # Overall form from API
        team.form_overall = form_str[:10]

        # Build real home/away form from DB fixtures — sorted most recent first
        def _result_char(f, is_home):
            if f.home_score is None:
                return None
            if is_home:
                if f.home_score > f.away_score: return 'W'
                if f.home_score < f.away_score: return 'L'
                return 'D'
            else:
                if f.away_score > f.home_score: return 'W'
                if f.away_score < f.home_score: return 'L'
                return 'D'

        home_fixtures = Fix.objects.filter(
            home_team=team, status='finished', home_score__isnull=False
        ).order_by('-kickoff')[:5]
        away_fixtures = Fix.objects.filter(
            away_team=team, status='finished', away_score__isnull=False
        ).order_by('-kickoff')[:5]

        home_form = ''.join(filter(None, [_result_char(f, True)  for f in home_fixtures]))
        away_form = ''.join(filter(None, [_result_char(f, False) for f in away_fixtures]))

        team.form_home = home_form[:6]
        team.form_away = away_form[:6]
        team.save(update_fields=["form_overall", "form_home", "form_away"])

    def _apply_standings(self, team: "Team", fs_team_id: str, standings: list):
        for row in standings:
            tid = str(row.get("team_id") or row.get("id") or "")
            if tid != str(fs_team_id):
                continue
            team.league_position = int(row.get("position") or row.get("rank") or 0) or None
            team.league_points = int(row.get("points") or 0) or None
            team.league_gf = int(row.get("goals_for") or row.get("scored") or 0) or None
            team.league_ga = int(row.get("goals_against") or row.get("conceded") or 0) or None
            team.save(update_fields=["league_position", "league_points", "league_gf", "league_ga"])
            break

    def _apply_ou_rates(self, team: "Team", fs_team_id: str, ou_data: list, sub_type: str):
        field_map = {
            "1.5": ("home_ou15_over_rate", "away_ou15_over_rate"),
            "2.5": ("home_ou25_over_rate", "away_ou25_over_rate"),
            "3.5": ("home_ou35_over_rate", "away_ou35_over_rate"),
        }
        if sub_type not in field_map:
            return

        home_field, away_field = field_map[sub_type]

        for row in ou_data:
            tid = str(row.get("team_id") or row.get("id") or "")
            if tid != str(fs_team_id):
                continue
            played = int(row.get("matches_played") or row.get("played") or 0)
            over = int(row.get("over") or 0)
            if played > 0:
                rate = round(over / played, 3)
                setattr(team, home_field, rate)
                setattr(team, away_field, rate)
                team.save(update_fields=[home_field, away_field])
            break

    def _apply_htft(self, team: "Team", fs_team_id: str, htft_data: list):
        for row in htft_data:
            tid = str(row.get("team_id") or row.get("id") or "")
            if tid != str(fs_team_id):
                continue
            played = int(row.get("matches_played") or row.get("played") or 1) or 1

            def rate(key):
                return round(int(row.get(key) or 0) / played, 3)

            team.htft_ww_rate = rate("win_win")
            team.htft_wd_rate = rate("win_draw")
            team.htft_wl_rate = rate("win_lose")
            team.htft_dw_rate = rate("draw_win")
            team.htft_ll_rate = rate("lose_lose")
            team.save(
                update_fields=[
                    "htft_ww_rate",
                    "htft_wd_rate",
                    "htft_wl_rate",
                    "htft_dw_rate",
                    "htft_ll_rate",
                ]
            )
            break

    def _apply_lineups(self, team: "Team", side_data: dict):
        starters = side_data.get("starters", [])
        if not starters:
            return

        has_gk = any(("goalkeeper" in (p.get("position") or "").lower()) for p in starters)
        striker_cnt = sum(
            1
            for p in starters
            if any(
                x in (p.get("position") or "").lower()
                for x in ("forward", "striker", "attacker", "centre-forward")
            )
        )

        missing = 0
        if not has_gk:
            missing += 2
        if striker_cnt == 0:
            missing += 1

        team.key_players_missing = missing
        team.lineup_checked_at = timezone.now()
        team.save(update_fields=["key_players_missing", "lineup_checked_at"])

    def _update_team_stats(self, team_id: str, team_name: str, league: "League") -> bool:
        fake_int_id = api_client._stable_id(team_id)
        team = Team.objects.filter(api_id=fake_int_id).first()
        if not team:
            return False

        try:
            p1 = api_client.fetch_team_results(team_id, page=1)
            time.sleep(TEAM_RESULTS_DELAY)
            p2 = api_client.fetch_team_results(team_id, page=2)
            time.sleep(TEAM_RESULTS_DELAY)
            # Sort by timestamp descending — most recent first — so that
            # the exponential decay in compute_team_stats_from_results
            # correctly weights recent matches more than old ones.
            # Without this, position 0 gets weight 1.0 regardless of date.
            combined = p1 + p2

            # Filter to domestic league only for domestic competitions.
            # For European/continental cups use combined history — a team's
            # overall form including European matches is relevant context.
            use_domestic = team.league and team.league.is_domestic
            domestic_url = getattr(team.league, "tournament_url", "") if team.league else ""

            if use_domestic and domestic_url:
                domestic_only = [
                    r for r in combined
                    if not r.get("tournament_url") or
                    r.get("tournament_url") == domestic_url
                ]
                results_pool = domestic_only if len(domestic_only) >= 5 else combined
            else:
                # Cup/European fixture — use all competitions
                results_pool = combined

            results = sorted(
                [r for r in results_pool if r.get("timestamp")],
                key=lambda r: int(r["timestamp"]),
                reverse=True,
            )
            # Keep up to 20 most recent — beyond that the decay is negligible
            results = results[:20]

            if results and len(results) >= 5:
                corner_stat_matches = 6
                enriched = 0
                for r in results[:corner_stat_matches]:
                    mid = r.get("match_id")
                    if not mid:
                        continue
                    try:
                        mstats = api_client.fetch_fixture_stats(mid)
                        if mstats.get("home_corners") is not None:
                            r["home_corners"] = mstats["home_corners"]
                            r["away_corners"] = mstats["away_corners"]
                            enriched += 1
                    except Exception:
                        pass
                    time.sleep(MATCH_STATS_DELAY)

                if enriched:
                    logger.info(
                        "[CORNERS] %s — %d/%d matches enriched with corner data",
                        team_name,
                        enriched,
                        corner_stat_matches,
                    )

                stats = api_client.compute_team_stats_from_results(team_id, results)
                if stats.get("games_played", 0) >= 5:
                    self._apply_stats(team, stats)
                    logger.info("[OK] %s — %d games", team_name, stats["games_played"])
                    return True
        except Exception as exc:
            logger.debug("Results failed for %s: %s", team_name, exc)

        logger.warning("[NO DATA] %s", team_name)
        return False

    def _apply_stats(self, team: "Team", stats: dict):
        for field, value in stats.items():
            if hasattr(team, field):
                setattr(team, field, value)
        team.save()

    def _cleanup_duplicate_fixtures(self, today_str: str) -> int:
        groups = (
            Fixture.objects.filter(kickoff__date=today_str)
            .values("api_id")
            .annotate(c=Count("id"))
            .filter(c__gt=1)
        )
        removed = 0
        for group in groups:
            rows = Fixture.objects.filter(
                kickoff__date=today_str,
                api_id=group["api_id"],
            ).order_by("id")
            keep = rows.first()
            extras = rows.exclude(id=keep.id)
            n = extras.count()
            if n:
                extras.delete()
                removed += n
        return removed

    def _get_or_create_league(self, item: dict) -> "League":
        name = item["league_name"].split(" - Round")[0].split(" - Matchday")[0].strip()
        league, _ = League.objects.update_or_create(
            api_id=item["league_api_id"],
            defaults={
                "name": name,
                "country": item["country_name"],
                "tier": item["league_tier"],
                "active": True,
                "season": self._current_season(),
            },
        )
        return league

    def _current_season(self) -> int:
        today = date.today()
        return today.year if today.month >= 7 else today.year - 1

    def _save_fixture(self, item: dict, league: "League") -> Optional["Fixture"]:
        try:
            match_id = item["match_id"]
            stable_api_id = api_client._stable_id(match_id)
            venue_str = f"fs:{match_id}"

            home_team = self._get_or_create_team(item["home_team_id"], item["home_team_name"], league)
            away_team = self._get_or_create_team(item["away_team_id"], item["away_team_name"], league)

            referee = None
            ref_name = (item.get("referee") or "").strip()
            if ref_name:
                ref_name = ref_name.split(",")[0].strip()
                if ref_name:
                    referee, _ = Referee.objects.get_or_create(name=ref_name)

            kickoff = item["kickoff"] or timezone.now()
            existing_by_teams = Fixture.objects.filter(
                home_team=home_team,
                away_team=away_team,
                kickoff__date=kickoff.date() if hasattr(kickoff, "date") else kickoff,
            ).order_by("id")

            if existing_by_teams.exists():
                keep = existing_by_teams.first()
                existing_by_teams.exclude(id=keep.id).delete()
                status_rank = {
                    "finished": 4,
                    "live": 3,
                    "scheduled": 2,
                    "postponed": 1,
                    "cancelled": 0,
                }
                new_status = item["status"]
                if status_rank.get(new_status, 0) >= status_rank.get(keep.status, 0):
                    keep.status = new_status
                    keep.home_score = (
                        int(item["home_score"]) if item.get("home_score") is not None else keep.home_score
                    )
                    keep.away_score = (
                        int(item["away_score"]) if item.get("away_score") is not None else keep.away_score
                    )
                    keep.venue = venue_str
                    keep.api_id = stable_api_id
                    keep.save()
                return keep

            fixture, _ = Fixture.objects.update_or_create(
                api_id=stable_api_id,
                defaults={
                    "league": league,
                    "home_team": home_team,
                    "away_team": away_team,
                    "kickoff": kickoff,
                    "referee": referee,
                    "venue": venue_str,
                    "status": item["status"],
                    "home_score": int(item["home_score"]) if item.get("home_score") is not None else None,
                    "away_score": int(item["away_score"]) if item.get("away_score") is not None else None,
                },
            )
            return fixture
        except Exception as exc:
            logger.error("Error saving fixture: %s", exc)
            return None

    def _get_or_create_team(self, team_id: str, team_name: str, league: "League") -> "Team":
        fake_int_id = api_client._stable_id(team_id)

        team = Team.objects.filter(api_id=fake_int_id).first()
        if team:
            # Fix blank or Unknown_ names if we now have a real name
            if team_name and (not team.name or team.name.startswith("Unknown_")):
                team.name = team_name
                team.save(update_fields=["name"])
                logger.info("Fixed team name: %s → %s", team.api_id, team_name)
            return team

        team = Team.objects.filter(name=team_name, league=league).first()
        if team:
            team.api_id = fake_int_id
            team.save(update_fields=["api_id"])
            return team

        team, created = Team.objects.get_or_create(
            api_id=fake_int_id,
            defaults={
                "name": team_name,
                "league": league,
                "form_home": "",
                "form_away": "",
                "form_overall": "",
                "key_players_missing": 0,
                "rw_home_goals_for": 0.0,
                "rw_home_goals_against": 0.0,
                "rw_away_goals_for": 0.0,
                "rw_away_goals_against": 0.0,
                "home_ou15_over_rate": 0.0,
                "home_ou25_over_rate": 0.0,
                "home_ou35_over_rate": 0.0,
                "away_ou15_over_rate": 0.0,
                "away_ou25_over_rate": 0.0,
                "away_ou35_over_rate": 0.0,
                "htft_ww_rate": 0.0,
                "htft_wd_rate": 0.0,
                "htft_wl_rate": 0.0,
                "htft_dw_rate": 0.0,
                "htft_ll_rate": 0.0,
            },
        )
        if not created:
            changed = False
            if team.name != team_name:
                team.name = team_name
                changed = True
            if not team.league:
                team.league = league
                changed = True
            if changed:
                team.save()
        return team

    def _league_has_data(self, league: "League") -> bool:
        from fixtures.models import Fixture as Fix

        return Fix.objects.filter(
            league=league,
            status="finished",
        ).count() >= 1

    def _deduplicate_teams(self, team_list):
        seen, unique = set(), []
        for team_id, name, league in team_list:
            if team_id and team_id not in seen:
                seen.add(team_id)
                unique.append((team_id, name, league))
        return unique
