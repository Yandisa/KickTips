"""
grade_results — Night pipeline
================================
Grades today's published predictions against actual results.
Fetches corners and cards from FlashScore for each finished match.

Usage:
    python manage.py grade_results
    python manage.py grade_results --date 2026-04-03

API calls per run:
    N finished predicted matches * 1 (match/stats) = ~10-25 calls
"""
import logging
import re
from datetime import date
from django.core.management.base import BaseCommand
from fixtures.models import Fixture, League
from fixtures.api_client import fetch_fixture_stats, fetch_fixtures_finished, _stable_id
from predictions.models import Prediction
from results.models import PerformanceRecord



def _parse_line(tip: str, default: float) -> float:
    """
    Extract the numeric line from a tip string like 'Over 2.5' or 'Under 10.5'.
    Falls back to `default` if no number is found.
    """
    match = re.search(r"(\d+\.?\d*)", tip)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return default

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Grade today's prediction results against actual match scores"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date", type=str, default=None,
            help="Date to grade (YYYY-MM-DD). Defaults to today.",
        )

    def handle(self, *args, **options):
        target_date = date.today()
        if options.get("date"):
            try:
                target_date = date.fromisoformat(options["date"])
            except ValueError:
                self.stdout.write("Invalid date format. Use YYYY-MM-DD.")
                return

        self.stdout.write(f"=== Night Grader: {target_date} ===")

        # ── Step 1: pull final scores for any fixtures not yet finished ──
        updated = self._update_scores(target_date.isoformat())
        if updated:
            self.stdout.write(f"Score update: {updated} fixture(s) refreshed")

        finished_fixtures = Fixture.objects.filter(
            kickoff__date=target_date,
            status="finished",
            home_score__isnull=False,
            away_score__isnull=False,
            graded=False,
        )

        if not finished_fixtures.exists():
            self.stdout.write(
                "No ungraded finished fixtures in DB. "
                "The morning pipeline updates scores — re-run fetch_fixtures if needed."
            )
            return

        self.stdout.write(f"Grading {finished_fixtures.count()} finished fixtures...")

        graded = won = lost = void_count = 0

        for fixture in finished_fixtures:
            predictions = Prediction.objects.filter(
                fixture=fixture, published=True, result="pending"
            )
            if not predictions.exists():
                continue

            # Fetch corners + cards from FlashScore if we have a match_id
            # The fixture.api_id is a stable int hash of the FlashScore match_id.
            # We store the original string match_id in the venue field as fallback,
            # or reconstruct from the DB api_id — but we can look it up via API
            # using the fixture api_id mapped back. For now we use fixture.api_id
            # as a proxy — fetch_fixture_stats accepts string match_id.
            # The FlashScore match_id is stored as a note or we pass api_id str.
            corners = fixture.total_corners or 0
            cards   = fixture.total_cards or 0

            # Try to get corners/cards from API if not already stored
            if corners == 0:
                # Use the venue field to store match_id (set during fetch_fixtures)
                fs_match_id = self._get_fs_match_id(fixture)
                if fs_match_id:
                    stats = fetch_fixture_stats(fs_match_id)
                    if stats:
                        corners = stats.get("corner_kicks") or 0
                        cards   = (stats.get("yellow_cards") or 0) + (stats.get("red_cards") or 0)
                        if corners > 0:
                            fixture.total_corners = corners
                        if cards > 0:
                            fixture.total_cards = cards
                        fixture.save(update_fields=["total_corners", "total_cards"])

            for pred in predictions:
                result = self._grade(pred, fixture, corners, cards)
                pred.result = result
                pred.save()
                graded += 1
                if result == "won":
                    won += 1
                elif result == "lost":
                    lost += 1
                else:
                    void_count += 1

            fixture.graded = True
            fixture.save(update_fields=["graded"])

        self._update_record(target_date)

        self.stdout.write(self.style.SUCCESS(
            f"Grading complete: {graded} tips | {won} won, {lost} lost, {void_count} void ✅"
        ))

    def _update_scores(self, date_str: str) -> int:
        """
        Fetch final scores for fixtures not yet marked 'finished' and
        update the DB in-place.  Returns the number of rows updated.
        """
        STATUS_RANK = {"finished": 4, "live": 3, "scheduled": 2, "postponed": 1, "cancelled": 0}
        score_updates = fetch_fixtures_finished(date_str)
        updated = 0
        for item in score_updates:
            match_id = (item.get("match_id") or "").strip()
            if not match_id:
                continue
            stable_id = _stable_id(match_id)
            fixture = (
                Fixture.objects.filter(api_id=stable_id).first()
                or Fixture.objects.filter(venue=f"fs:{match_id}").first()
            )
            if not fixture:
                continue
            new_status = item.get("status", "scheduled")
            if STATUS_RANK.get(new_status, 0) <= STATUS_RANK.get(fixture.status, 0):
                continue  # already at equal or better status — skip
            fixture.status = new_status
            if item.get("home_score") is not None:
                fixture.home_score = int(item["home_score"])
            if item.get("away_score") is not None:
                fixture.away_score = int(item["away_score"])
            fixture.save(update_fields=["status", "home_score", "away_score"])
            updated += 1
            logger.info("Score updated: %s → %s (%s-%s)",
                        fixture, new_status,
                        fixture.home_score, fixture.away_score)
        return updated

    def _get_fs_match_id(self, fixture) -> str:
        """
        Retrieve the FlashScore match_id string for a fixture.
        FlashScore match IDs are stored in the fixture venue field
        when the fixture is saved via fetch_fixtures (prefixed with 'fs:').
        Falls back to None if not stored.
        """
        venue = fixture.venue or ""
        if venue.startswith("fs:"):
            return venue[3:]
        return None

    def _grade(self, pred, fixture, total_corners: int, total_cards: int) -> str:
        try:
            h = fixture.home_score
            a = fixture.away_score
            if h is None or a is None:
                return "void"

            market      = pred.market
            tip         = pred.tip
            total_goals = h + a

            if market == "1x2":
                actual = fixture.result
                return "won" if (
                    (tip == "Home Win" and actual == "home") or
                    (tip == "Away Win" and actual == "away") or
                    (tip == "Draw"     and actual == "draw")
                ) else "lost"

            elif market == "dc":
                actual = fixture.result
                return "won" if (
                    (tip == "Home or Draw" and actual in ["home", "draw"]) or
                    (tip == "Away or Draw" and actual in ["away", "draw"]) or
                    (tip == "Home or Away" and actual in ["home", "away"])
                ) else "lost"

            elif market == "ou_goals":
                line = _parse_line(tip, default=2.5)
                return "won" if (
                    ("Over" in tip and total_goals > line) or
                    ("Under" in tip and total_goals < line)
                ) else "lost"

            elif market == "btts":
                actual_btts = h > 0 and a > 0
                return "won" if (
                    ("Yes" in tip and actual_btts) or
                    ("No"  in tip and not actual_btts)
                ) else "lost"

            elif market == "corners":
                if total_corners == 0:
                    return "void"
                line = _parse_line(tip, default=9.5)
                return "won" if (
                    ("Over"  in tip and total_corners > line) or
                    ("Under" in tip and total_corners < line)
                ) else "lost"

            return "void"

        except Exception as exc:
            logger.error("Grading error for %s: %s", pred, exc)
            return "void"

    def _update_record(self, target_date):
        preds = Prediction.objects.filter(
            fixture__kickoff__date=target_date,
            published=True,
        ).exclude(result="pending")

        total = preds.count()
        won   = preds.filter(result="won").count()
        lost  = preds.filter(result="lost").count()
        void  = preds.filter(result="void").count()

        def ms(m):
            qs = preds.filter(market=m)
            return qs.filter(result="won").count(), qs.count()

        def bs(lo, hi=None):
            qs = preds.filter(confidence__gte=lo) if hi is None else \
                 preds.filter(confidence__gte=lo, confidence__lt=hi)
            return qs.filter(result="won").count(), qs.count()

        w1, t1 = ms("1x2");     w2, t2 = ms("dc")
        w3, t3 = ms("ou_goals"); w4, t4 = ms("corners"); w5, t5 = ms("btts")
        w65, tot65 = bs(65, 70); w70, tot70 = bs(70, 75)
        w75, tot75 = bs(75, 80); w80, tot80 = bs(80)

        PerformanceRecord.objects.update_or_create(
            date=target_date,
            defaults={
                "total_published": total,
                "total_won":       won,
                "total_lost":      lost,
                "total_void":      void,
                "win_rate":        round(won / total * 100, 1) if total else 0,
                "won_1x2":   w1,  "total_1x2":    t1,
                "won_dc":    w2,  "total_dc":      t2,
                "won_goals": w3,  "total_goals":   t3,
                "won_corners": w4,"total_corners":  t4,
                "won_btts":  w5,  "total_btts":    t5,
                "won_65_70": w65, "total_65_70":   tot65,
                "won_70_75": w70, "total_70_75":   tot70,
                "won_75_80": w75, "total_75_80":   tot75,
                "won_80_plus": w80,"total_80_plus": tot80,
            },
        )
        logger.info("PerformanceRecord updated for %s: %s%% win rate",
                    target_date, round(won / total * 100, 1) if total else 0)
