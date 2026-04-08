"""
refresh_odds — Closing Line Value pipeline
==========================================
Run this ~1 hour before the first kickoff of the day to capture closing
bookmaker prices. These are stored on each Prediction as closing_decimal
and used to compute CLV (Closing Line Value) at grading time.

CLV is the most important leading indicator of model edge:
  clv > 0  →  we published at better odds than the market settled on
  clv < 0  →  market moved away from our position

A model that consistently beats the closing line has real edge,
regardless of short-term win rate noise.

Usage:
    python manage.py refresh_odds
    python manage.py refresh_odds --date 2026-04-10
"""

import logging
import time
from datetime import date

from django.core.management.base import BaseCommand

from fixtures.models import Fixture
from fixtures.api_client import fetch_match_odds
from predictions.models import Prediction

logger = logging.getLogger(__name__)

MARKET_ODDS_KEYS = {
    "1x2":      ["home", "draw", "away"],
    "dc":       ["1x", "x2", "12"],
    "ou_goals": None,   # keyed by line string
    "btts":     ["yes", "no"],
    "corners":  None,   # keyed by line string
}


def _extract_closing_decimal(market: str, tip: str, odds: dict) -> float | None:
    """
    Pull the specific bookmaker decimal for the published tip from the
    freshly fetched odds dict. Returns None if market/key not found.
    """
    import re

    if market == "1x2":
        ou = odds.get("1x2", {})
        if "Home Win" in tip:   return ou.get("home")
        if "Away Win" in tip:   return ou.get("away")
        if "Draw" in tip:       return ou.get("draw")

    elif market == "dc":
        ou = odds.get("dc", {})
        if "Home or Draw" in tip: return ou.get("1x")
        if "Away or Draw" in tip: return ou.get("x2")
        if "Home or Away" in tip: return ou.get("12")

    elif market == "btts":
        ou = odds.get("btts", {})
        if "Yes" in tip: return ou.get("yes")
        if "No"  in tip: return ou.get("no")

    elif market in ("ou_goals", "corners"):
        bucket = "ou_goals" if market == "ou_goals" else "ou_corners"
        ou = odds.get(bucket, {})
        m = re.search(r"(\d+\.?\d*)", tip)
        if not m:
            return None
        line_str = m.group(1)
        # Normalise: "2.5" and "2.50" should match
        line_data = ou.get(line_str) or ou.get(f"{float(line_str):.1f}")
        if not line_data:
            return None
        if "Over" in tip:  return line_data.get("over")
        if "Under" in tip: return line_data.get("under")

    return None


class Command(BaseCommand):
    help = "Capture closing bookmaker odds for today's published predictions"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date", type=str, default=None,
            help="Date to refresh (YYYY-MM-DD). Defaults to today.",
        )

    def handle(self, *args, **options):
        target_date = date.today()
        if options.get("date"):
            try:
                target_date = date.fromisoformat(options["date"])
            except ValueError:
                self.stdout.write("Invalid date format. Use YYYY-MM-DD.")
                return

        self.stdout.write(f"=== Closing Odds Refresh: {target_date} ===")

        # Get all fixtures today with published pending predictions
        fixtures = list(
            Fixture.objects.filter(
                kickoff__date=target_date,
                status="scheduled",
            ).select_related("home_team", "away_team", "league")
            .prefetch_related("predictions")
        )

        if not fixtures:
            self.stdout.write("No scheduled fixtures found.")
            return

        self.stdout.write(f"Refreshing odds for {len(fixtures)} fixtures...")

        refreshed = skipped = 0

        for fixture in fixtures:
            preds = list(Prediction.objects.filter(
                fixture=fixture,
                published=True,
                result="pending",
                bookie_decimal__isnull=False,
                closing_decimal__isnull=True,   # not yet captured
            ))

            if not preds:
                continue

            venue = fixture.venue or ""
            if not venue.startswith("fs:"):
                skipped += 1
                continue

            match_id = venue[3:]
            try:
                odds = fetch_match_odds(match_id) or {}
                time.sleep(0.8)
            except Exception as exc:
                logger.warning("Odds fetch failed for %s: %s", fixture, exc)
                skipped += 1
                continue

            if not odds:
                skipped += 1
                continue

            # Filter ou_goals to clean lines only
            VALID_GOAL_LINES = {"1.5", "2.5", "3.5", "4.5"}
            if "ou_goals" in odds:
                odds["ou_goals"] = {
                    k: v for k, v in odds["ou_goals"].items()
                    if k in VALID_GOAL_LINES
                }

            for pred in preds:
                closing = _extract_closing_decimal(pred.market, pred.tip, odds)
                if closing is None or closing <= 1.0:
                    continue

                # CLV: positive means we got better odds than closing
                clv = round((pred.bookie_decimal / closing) - 1, 4)

                pred.closing_decimal = closing
                pred.clv             = clv
                pred.save(update_fields=["closing_decimal", "clv"])
                refreshed += 1

                direction = "✅ beat" if clv > 0 else "❌ missed"
                self.stdout.write(
                    f"  {fixture.home_team} vs {fixture.away_team} | "
                    f"{pred.market} {pred.tip} | "
                    f"open={pred.bookie_decimal} close={closing} "
                    f"CLV={clv:+.1%} {direction}"
                )

        self.stdout.write(self.style.SUCCESS(
            f"Done: {refreshed} closing lines captured, {skipped} fixtures skipped"
        ))
