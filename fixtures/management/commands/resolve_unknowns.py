"""
resolve_unknowns — Fix Unknown_ team names
==========================================
Finds all teams with name starting 'Unknown_', fetches their real name
from FlashScore using a fixture they played in, and updates the DB.

Usage:
    python manage.py resolve_unknowns
"""
import logging
import time

from django.core.management.base import BaseCommand
from django.db.models import Q

from fixtures.models import Team, Fixture
from fixtures import api_client

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Resolve Unknown_ team names by fetching real names from FlashScore"

    def handle(self, *args, **options):
        unknowns = Team.objects.filter(name__startswith="Unknown_")
        total = unknowns.count()
        self.stdout.write(f"Found {total} Unknown_ teams to resolve\n")

        resolved = skipped = failed = 0

        for team in unknowns:
            # Try scraper_source first — stored as fs:TEAMID
            src = team.scraper_source or ""
            fs_team_id = src[3:] if src.startswith("fs:") else None

            # Find a fixture this team played in
            fixture = Fixture.objects.filter(
                Q(home_team=team) | Q(away_team=team),
                venue__startswith="fs:",
            ).order_by("-kickoff").first()

            if not fixture:
                self.stdout.write(f"  SKIP {team.name} — no fixture found")
                skipped += 1
                continue

            match_id = fixture.venue[3:]
            is_home  = fixture.home_team_id == team.pk

            try:
                details = api_client._get(
                    "/api/flashscore/v2/matches/details",
                    {"match_id": match_id}
                )
                time.sleep(0.5)

                if not isinstance(details, dict):
                    self.stdout.write(f"  FAIL {team.name} — no details for {match_id}")
                    failed += 1
                    continue

                key      = "home_team" if is_home else "away_team"
                team_data = details.get(key) or {}
                real_name = (
                    team_data.get("name") or
                    team_data.get("short_name") or
                    team_data.get("shortName") or
                    team_data.get("title") or
                    team_data.get("participant_name") or
                    ""
                ).strip()

                real_fs_id = team_data.get("team_id") or team_data.get("id") or ""

                if not real_name:
                    self.stdout.write(f"  FAIL {team.name} — no name in response")
                    failed += 1
                    continue

                old_name = team.name
                team.name = real_name
                if real_fs_id:
                    team.scraper_source = f"fs:{real_fs_id}"
                team.save(update_fields=["name", "scraper_source"])

                self.stdout.write(f"  ✅ {old_name} → {real_name}")
                resolved += 1

            except Exception as exc:
                self.stdout.write(f"  FAIL {team.name} — {exc}")
                failed += 1

        self.stdout.write(self.style.SUCCESS(
            f"\nDone: {resolved} resolved, {skipped} skipped, {failed} failed"
        ))
