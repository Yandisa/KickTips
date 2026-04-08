from django.core.management.base import BaseCommand
import time
from datetime import date
from fixtures.models import Fixture, Team
from fixtures import api_client


class Command(BaseCommand):
    help = "Repair team stats - fix goals for all teams still on default values"

    def handle(self, *args, **kwargs):
        today = date.today()

        fixtures = list(
            Fixture.objects.filter(kickoff__date=today, league__active=True)
            .select_related("home_team", "away_team", "league")
            .order_by("id")
        )

        seen_venues = set()
        unique_fixtures = []
        for f in fixtures:
            if f.venue and f.venue not in seen_venues:
                seen_venues.add(f.venue)
                unique_fixtures.append(f)

        self.stdout.write(f"Today: {len(fixtures)} fixtures, {len(unique_fixtures)} unique venues")

        # Target ANY team in today's fixtures still on default goals (1.5)
        # This catches teams that got games_played set but goals left at 0 or default
        needy = set(
            Team.objects.filter(
                id__in=[t for f in fixtures for t in (f.home_team_id, f.away_team_id)],
                home_avg_goals_for=1.5,
            ).values_list("id", flat=True)
        )
        self.stdout.write(f"Teams needing goals stats: {len(needy)}")

        updated = 0

        for fixture in unique_fixtures:
            if not fixture.venue or not fixture.venue.startswith("fs:"):
                continue
            if fixture.home_team_id not in needy and fixture.away_team_id not in needy:
                continue

            match_id = fixture.venue[3:]

            try:
                rows = api_client.fetch_match_standings(match_id)
                time.sleep(0.8)
            except Exception as e:
                self.stdout.write(f"  [ERROR] {match_id}: {e}")
                continue

            if not rows:
                continue

            for row in rows:
                fs_team_id_str = str(row.get("team_id") or row.get("id") or "")
                if not fs_team_id_str:
                    continue

                stable = api_client._stable_id(fs_team_id_str)
                try:
                    team = Team.objects.get(api_id=stable)
                except Team.DoesNotExist:
                    continue

                if team.id not in needy:
                    continue

                played = int(row.get("matches_played") or row.get("played") or 0)
                if played < 5:
                    continue

                won   = int(row.get("wins")  or row.get("won")   or 0)
                drawn = int(row.get("draws") or row.get("drawn") or 0)

                # Goals are returned as "102:39" string
                goals_str = str(row.get("goals") or "0:0")
                try:
                    gf, ga = [int(x) for x in goals_str.split(":")]
                except (ValueError, AttributeError):
                    gf, ga = 0, 0

                if gf == 0 and ga == 0:
                    continue  # API returned no goal data for this team

                avg_gf    = round(gf / played, 2)
                avg_ga    = round(ga / played, 2)
                win_rate  = round(won   / played, 3)
                draw_rate = round(drawn / played, 3)

                # repair_stats only has overall season totals from standings,
                # not the home/away split. Only update fields that are still
                # at their model defaults — don't overwrite real home/away
                # stats that fetch_fixtures already populated correctly.
                update_fields = ["games_played", "home_win_rate", "away_win_rate",
                                 "home_draw_rate", "away_draw_rate"]
                team.games_played   = played
                team.home_win_rate  = win_rate
                team.away_win_rate  = win_rate
                team.home_draw_rate = draw_rate
                team.away_draw_rate = draw_rate

                # Only overwrite goals if still at hard defaults (1.5/1.2)
                # — means fetch_fixtures never populated real home/away stats
                if team.home_avg_goals_for == 1.5 and team.away_avg_goals_for == 1.2:
                    team.home_avg_goals_for     = avg_gf
                    team.home_avg_goals_against = avg_ga
                    team.away_avg_goals_for     = avg_gf
                    team.away_avg_goals_against = avg_ga
                    update_fields += ["home_avg_goals_for", "home_avg_goals_against",
                                      "away_avg_goals_for", "away_avg_goals_against"]

                team.save(update_fields=update_fields)
                needy.discard(team.id)
                updated += 1
                self.stdout.write(
                    f"  [OK] {team.name}: {played}gp  "
                    f"gf/g={avg_gf}  ga/g={avg_ga}  win={round(win_rate*100)}%"
                )

        self.stdout.write(self.style.SUCCESS(
            f"Done: {updated} teams updated, {len(needy)} still on defaults"
        ))
        if needy:
            names = list(Team.objects.filter(id__in=needy).values_list("name", flat=True)[:10])
            self.stdout.write(f"Still missing: {names}")
