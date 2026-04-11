"""
save_accumulators — Daily accumulator snapshot
===============================================
Run after run_predictions to snapshot today's three accumulator tiers
(Faka Yonke, Shaya Zonke, Istimela) as DB records. This enables honest
tracking — every acca is stored with its legs and combined odds, then
graded by grade_results when all legs settle.

The acca building logic mirrors views.py exactly so the DB snapshot
matches what punters see on the website.

Usage:
    python manage.py save_accumulators
    python manage.py save_accumulators --date 2026-04-10
"""

import logging
import math
from datetime import date

from django.core.management.base import BaseCommand

from predictions.models import Prediction, Accumulator, AccumulatorLeg

logger = logging.getLogger(__name__)

# ── Mirror the exact constants from views.py ──────────────────────────────────
# FAKA_MIN_CONF lowered from 70.0 to 66.5 — with MAX_DISPLAY_CONFIDENCE=67.0
# in the engine, nothing reaches 70% so Faka Yonke would never fire.
FAKA_MIN_CONF     = 66.5
SHAYA_MIN_CONF    = 65.0
ISTIMELA_MIN_CONF = 60.0

MAX_SAME_MARKET_PER_ACCA = 3   # raised from 2 — DC-heavy days need more slots
MAX_SAME_LEAGUE_PER_ACCA = 4   # raised from 3 — fewer fixtures per day need more flexibility

MARKET_PREFERENCE = {'1x2': 1, 'btts': 2, 'ou_goals': 3, 'dc': 4, 'corners': 5}


# Historical win rates by market — used to score tips for acca selection
# Higher win rate = higher score = more likely to appear in Faka Yonke
MARKET_WIN_RATES = {
    'corners': 0.75,   # best performer
    'dc':      0.55,   # solid
    'btts':    0.54,   # solid
    'ou_goals': 0.43,  # weakest
    '1x2':     0.35,   # avoid
}

def _score_prediction(pred):
    """
    Score a prediction for acca selection.
    Uses market win rate + edge as primary signals rather than just confidence
    (which is capped at 67% for everything and creates no separation).
    """
    market_wr = MARKET_WIN_RATES.get(pred.market, 0.45)
    edge      = float(pred.edge or 0)
    conf      = float(pred.confidence or 0)

    # Base score from market historical win rate (0-1 scale, ×100 for readability)
    score = market_wr * 100

    # Edge bonus — real bookmaker edge adds value
    score += edge * 50

    # Small confidence contribution — tie-breaker only
    score += conf * 0.1

    return score


def _leg_decimal(pred):
    """Real odds when available, otherwise fair decimal from confidence."""
    if pred.bookie_decimal and pred.bookie_decimal > 1.0:
        return pred.bookie_decimal
    conf = float(pred.confidence or 65) / 100
    if conf <= 0:
        return 2.0
    fair = round(1.0 / conf, 3)
    return min(fair, 5.0)


def _ranked_unique(all_preds, min_conf, exclude_markets=None):
    """
    One tip per fixture, best-scored, filtered by min_conf.
    exclude_markets: set of (fixture_id, market) pairs already used in higher tiers.
    This allows Shaya to pick a different market from the same fixture than Faka.
    """
    exclude_markets = exclude_markets or set()
    seen_fixtures   = set()
    ranked          = []

    for pred in sorted(all_preds, key=_score_prediction, reverse=True):
        if float(pred.confidence or 0) < min_conf:
            continue
        fid    = pred.fixture_id
        market = pred.market

        # Skip if this exact fixture+market was used in a higher tier
        if (fid, market) in exclude_markets:
            continue
        # Skip if this fixture already has a leg in this tier
        if fid in seen_fixtures:
            continue

        seen_fixtures.add(fid)
        ranked.append(pred)

    return ranked


def _build_acca(legs, size_min, size_max):
    selected = []
    market_counts = {}
    league_counts = {}

    for pred in legs:
        if len(selected) >= size_max:
            break
        market    = pred.market
        league_id = pred.fixture.league_id

        if market_counts.get(market, 0) >= MAX_SAME_MARKET_PER_ACCA:
            continue
        if league_counts.get(league_id, 0) >= MAX_SAME_LEAGUE_PER_ACCA:
            continue

        selected.append(pred)
        market_counts[market]    = market_counts.get(market, 0) + 1
        league_counts[league_id] = league_counts.get(league_id, 0) + 1

    if len(selected) < size_min:
        return []

    return selected


def _combined_odds(legs):
    total = 1.0
    for pred in legs:
        total *= _leg_decimal(pred)
    return round(total, 2)


class Command(BaseCommand):
    help = "Snapshot today's three accumulator tiers to the DB for honest tracking"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date", type=str, default=None,
            help="Date to snapshot (YYYY-MM-DD). Defaults to today.",
        )

    def handle(self, *args, **options):
        target_date = date.today()
        if options.get("date"):
            try:
                target_date = date.fromisoformat(options["date"])
            except ValueError:
                self.stdout.write("Invalid date format. Use YYYY-MM-DD.")
                return

        self.stdout.write(f"=== Saving Accumulators: {target_date} ===")

        all_preds = list(
            Prediction.objects.filter(
                published=True,
                result="pending",
                fixture__kickoff__date=target_date,
            ).select_related(
                "fixture", "fixture__home_team",
                "fixture__away_team", "fixture__league",
            ).order_by("-confidence")
        )

        if not all_preds:
            self.stdout.write("No published pending predictions found.")
            return

        self.stdout.write(f"Pool: {len(all_preds)} published tips")

        # Track which (fixture_id, market) pairs are used per tier
        # so each tier can pick a different market from the same fixture.
        # Faka picks best market, Shaya picks next best, Istimela picks third.
        used_markets: set = set()

        tier_configs = [
            ("faka_yonke",  FAKA_MIN_CONF,     4, 5),
            ("shaya_zonke", SHAYA_MIN_CONF,     5, 8),
            ("istimela",    ISTIMELA_MIN_CONF,  6, 12),
        ]

        for tier_key, min_conf, size_min, size_max in tier_configs:
            pool = _ranked_unique(all_preds, min_conf, exclude_markets=used_markets)
            legs = _build_acca(pool, size_min, size_max)

            if not legs:
                self.stdout.write(f"  {tier_key}: insufficient legs (need {size_min}+, got {len(pool)} in pool)")
                continue

            # Record which fixture+market combinations this tier used
            for leg in legs:
                used_markets.add((leg.fixture_id, leg.market))

            odds = _combined_odds(legs)

            # Upsert — replace if already exists for this date/tier
            acca, created = Accumulator.objects.update_or_create(
                date=target_date,
                tier=tier_key,
                defaults={
                    "combined_odds": odds,
                    "legs_count":    len(legs),
                    "result":        "pending",
                },
            )

            # Rebuild legs — delete old ones if recreating
            if not created:
                AccumulatorLeg.objects.filter(accumulator=acca).delete()

            for pred in legs:
                AccumulatorLeg.objects.create(
                    accumulator=acca,
                    prediction=pred,
                    leg_odds=_leg_decimal(pred),
                )

            action = "Created" if created else "Updated"
            tier_label = acca.get_tier_display()
            self.stdout.write(
                f"  {action} {tier_label}: {len(legs)} legs @ {odds}x combined"
            )
            for pred in legs:
                self.stdout.write(
                    f"    └ {pred.fixture.home_team} vs {pred.fixture.away_team}"
                    f" | {pred.market} | {pred.tip} ({pred.confidence:.0f}%)"
                    f" @ {_leg_decimal(pred)}"
                )

        self.stdout.write(self.style.SUCCESS("Accumulators saved ✅"))
