"""
backfill_form — Rebuild home/away form strings from DB fixtures
================================================================
Run once after deploying the _apply_form fix to populate
form_home and form_away correctly for all teams.

Usage:
    python manage.py backfill_form
"""
from django.core.management.base import BaseCommand
from fixtures.models import Team, Fixture


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


class Command(BaseCommand):
    help = "Rebuild form_home and form_away from DB fixtures for all teams"

    def handle(self, *args, **options):
        teams = Team.objects.all().order_by('name')
        total = teams.count()
        self.stdout.write(f"Backfilling form strings for {total} teams...")

        updated = skipped = 0

        for team in teams:
            home_fixtures = Fixture.objects.filter(
                home_team=team, status='finished', home_score__isnull=False
            ).order_by('-kickoff')[:5]

            away_fixtures = Fixture.objects.filter(
                away_team=team, status='finished', away_score__isnull=False
            ).order_by('-kickoff')[:5]

            home_form = ''.join(filter(None, [_result_char(f, True)  for f in home_fixtures]))
            away_form = ''.join(filter(None, [_result_char(f, False) for f in away_fixtures]))

            if not home_form and not away_form:
                skipped += 1
                continue

            team.form_home = home_form[:6]
            team.form_away = away_form[:6]
            team.save(update_fields=['form_home', 'form_away'])
            updated += 1

        self.stdout.write(self.style.SUCCESS(
            f"Done: {updated} teams updated, {skipped} skipped (no finished fixtures)"
        ))
