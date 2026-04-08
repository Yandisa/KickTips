"""
run_predictions — Prediction command (v2)
==========================================
Changes from v1:
  - Fetches live bookmaker odds per fixture before running markets
  - Passes odds dict into every market function for value gating
  - Only publishes tips with positive expected value vs bookmaker
  - Logs edge (our_prob - bookie_implied) for every published tip
"""

import logging
import random
import time
from datetime import date

from django.core.management.base import BaseCommand

from fixtures.models import Fixture
from fixtures import api_client
from fixtures.api_client import fetch_head_to_head, fetch_match_odds
from predictions.engine import (
    predict_1x2,
    predict_double_chance,
    predict_goals,
    predict_btts,
)
from predictions.publisher import publish_predictions
from predictions.reasoner import generate_reasoning

from django.db.models import Avg, Count

logger = logging.getLogger(__name__)

# Only publish tips where the fixture has at least this confidence
PRIMARY_CONFIDENCE  = 65   # Strong tips — publish prominently
FALLBACK_CONFIDENCE = 65   # Minimum — nothing below 65% ever published

# Cache league computed stats for the duration of this command run
# so we don't recompute for every fixture from the same league
_LEAGUE_STATS_CACHE: dict = {}


def _compute_league_stats(league) -> dict:
    """
    Compute actual average goals and team count from finished fixtures
    stored in the DB for this league. Falls back to League model defaults
    if insufficient data (< 10 finished matches).
    """
    lid = league.id
    if lid in _LEAGUE_STATS_CACHE:
        return _LEAGUE_STATS_CACHE[lid]

    from fixtures.models import Fixture as Fix
    finished = Fix.objects.filter(
        league=league,
        status="finished",
        home_score__isnull=False,
        away_score__isnull=False,
    )
    count = finished.count()

    if count >= 10:
        agg = finished.aggregate(
            avg_h=Avg("home_score"),
            avg_a=Avg("away_score"),
        )
        avg_goals  = round((agg["avg_h"] or 0) + (agg["avg_a"] or 0), 3)
        team_count = (
            Fix.objects.filter(league=league, status="finished")
            .values("home_team_id")
            .distinct()
            .count()
        )
    else:
        avg_goals  = league.avg_goals or 2.65
        team_count = None

    stats = {"avg_goals": avg_goals, "team_count": team_count, "n_matches": count}
    _LEAGUE_STATS_CACHE[lid] = stats
    logger.debug("[LeagueStats] %s: %.2f goals/game from %d matches, %s teams",
                 league.name, avg_goals, count, team_count)
    return stats


class Command(BaseCommand):
    help = "Run value-filtered predictions for today's fixtures"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date", type=str, default=None,
            help="Date to predict (YYYY-MM-DD). Defaults to today.",
        )

    def handle(self, *args, **kwargs):
        if kwargs.get("date"):
            try:
                target = date.fromisoformat(kwargs["date"])
            except ValueError:
                self.stdout.write("Invalid date format. Use YYYY-MM-DD.")
                return
        else:
            target = date.today()

        self.stdout.write(f"=== Prediction Engine v2: {target} ===")

        fixtures = list(
            Fixture.objects.filter(
                kickoff__date=target,
                league__active=True,
                status="scheduled",
            ).select_related("home_team", "away_team", "league", "referee")
        )

        if not fixtures:
            self.stdout.write("No fixtures to predict today.")
            return

        random.shuffle(fixtures)
        self.stdout.write(f"Scoring {len(fixtures)} fixtures...")

        strong, fallback, total_markets = [], [], 0

        for fixture in fixtures:
            candidate = _build_candidate(fixture)
            if not candidate:
                continue

            total_markets += len(candidate["scored"])
            conf = candidate["confidence"]
            tips_with_value = candidate["value_count"]

            if tips_with_value == 0:
                # No valid tips — but still persist skipped markets so
                # match_detail shows the skip reasons for transparency.
                publish_predictions(fixture, candidate["scored"])
                continue   # no valid tips at all — skip

            if conf >= PRIMARY_CONFIDENCE:
                strong.append(candidate)
            elif conf >= FALLBACK_CONFIDENCE:
                fallback.append(candidate)

        strong.sort(key=lambda x: x["confidence"], reverse=True)
        fallback.sort(key=lambda x: x["confidence"], reverse=True)

        final = strong + fallback
        total_published = 0

        self.stdout.write(f"Strong ({PRIMARY_CONFIDENCE}%+): {len(strong)}  Fallback: {len(fallback)}")
        self.stdout.write(f"Total valid markets: {total_markets}")
        self.stdout.write(f"Fixtures with value tips: {len(final)}")

        for item in final:
            fixture = item["fixture"]
            n = publish_predictions(fixture, item["scored"])
            total_published += n
            if n:
                markets = ", ".join(item["scored"].keys())
                edges = " | ".join(
                    f"{m}={item['scored'][m].get('edge', 'n/a')}"
                    for m in item["scored"]
                    if item["scored"][m].get("edge") is not None
                )
                self.stdout.write(
                    f"  {item['home']} vs {item['away']} ({item['league']}) "
                    f"→ {n} tips | conf {item['confidence']:.1f}% | {markets}"
                    + (f" | edges: {edges}" if edges else "")
                )

        self.stdout.write(self.style.SUCCESS(
            f"Done: {total_published} value tips from {len(final)} fixtures"
        ))


def _build_candidate(fixture):
    home    = fixture.home_team
    away    = fixture.away_team
    league  = fixture.league
    referee = fixture.referee

    # Inject dynamically computed league stats so engine uses real averages
    # rather than the static League.avg_goals field
    league_stats = _compute_league_stats(league)
    league.computed_avg_goals = league_stats["avg_goals"]
    if league_stats["team_count"]:
        league.team_count = league_stats["team_count"]

    h2h_results = _load_h2h(fixture)

    # ── Fetch live bookmaker odds ──────────────────────────────────────────────
    odds = {}
    venue = fixture.venue or ""
    if venue.startswith("fs:"):
        match_id = venue[3:]
        try:
            odds = fetch_match_odds(match_id) or {}
            time.sleep(0.8)   # avoid 429 — odds + h2h = 2 calls per fixture
        except Exception as exc:
            logger.warning("Odds fetch failed for %s: %s", fixture, exc)

    # ── Clean ou_goals — keep only standard .5 lines, drop Asian quarter lines ──
    # FlashScore returns Asian handicap lines (0.75, 1.25, 1.75, 2.25 etc.)
    # mixed into ou_goals. These are split-stake markets — Poisson probabilities
    # don't apply to them. Keep only clean x.5 lines: 1.5, 2.5, 3.5, 4.5.
    # Also drop 0.5 (always ~1.05, no value) and 5.5 (too rare).
    VALID_GOAL_LINES = {"1.5", "2.5", "3.5", "4.5"}
    if "ou_goals" in odds:
        odds["ou_goals"] = {
            k: v for k, v in odds["ou_goals"].items()
            if k in VALID_GOAL_LINES
        }

    # ── Corners: disabled — no usable odds from FlashScore or SoccerInfo ───────
    # FlashScore only returns a junk 6.5 line (over=31.0, under=1.0 — not real
    # prices). SoccerInfo fallback returns empty for all tested fixtures.
    # Corners will be re-enabled when a working odds source is found.

    # ── Score all markets ──────────────────────────────────────────────────────
    scored_raw = {
        "1x2":      predict_1x2(home, away, h2h_results, league, odds),
        "dc":       predict_double_chance(home, away, h2h_results, league, odds),
        "ou_goals": predict_goals(home, away, h2h_results, league, odds),
        "btts":     predict_btts(home, away, h2h_results, league, odds),
    }

    valid_scored = {}
    highest_conf = 0.0
    value_count  = 0

    for market, result in scored_raw.items():
        if not result or result.get("skip_reason"):
            continue
        tip        = (result.get("tip") or "").strip()
        confidence = float(result.get("confidence") or 0)
        if not tip or confidence <= 0:
            continue

        result["reasoning"] = generate_reasoning(
            market=market,
            tip=tip,
            expected_value=result.get("expected_value", 0),
            home=home,
            away=away,
            referee=referee,
            h2h=h2h_results,
            league=league,
            bookie_decimal=result.get("bookie_decimal"),
            edge=result.get("edge"),
        )
        result["market"] = market
        result["tip"]    = tip

        valid_scored[market] = result
        if confidence > highest_conf:
            highest_conf = confidence
        value_count += 1  # count every valid tip; edge=None means no odds available

    if not valid_scored:
        return None

    return {
        "fixture":     fixture,
        "home":        home.name,
        "away":        away.name,
        "league":      league.name,
        "confidence":  highest_conf,
        "scored":      valid_scored,
        "value_count": value_count,
    }


def _load_h2h(fixture):
    from fixtures.models import Fixture as Fix
    from django.utils import timezone as tz

    home = fixture.home_team
    away = fixture.away_team
    now  = tz.now()

    db_h2h = Fix.objects.filter(
        home_team__in=[home, away],
        away_team__in=[home, away],
        status="finished",
        home_score__isnull=False,
        away_score__isnull=False,
    ).order_by("-kickoff")[:8]

    results = [
        {
            "home_score":    f.home_score,
            "away_score":    f.away_score,
            "winner":        f.result,
            "total_corners": f.total_corners,
            # days_ago enables time decay in the engine — recent H2H weighted more
            "days_ago": max(0, (now - f.kickoff).days) if f.kickoff else 365,
        }
        for f in db_h2h
    ]

    if len(results) >= 4:
        return results

    venue = fixture.venue or ""
    if venue.startswith("fs:"):
        try:
            time.sleep(0.8)
            api_h2h = fetch_head_to_head(venue[3:], last=8)
            for i, item in enumerate(api_h2h):
                h = item.get("home_score")
                a = item.get("away_score")
                if h is None or a is None:
                    continue
                results.append({
                    "home_score":    h,
                    "away_score":    a,
                    "winner":        "home" if h > a else ("away" if a > h else "draw"),
                    "total_corners": None,
                    # API h2h has no timestamp — assume roughly 3 months apart
                    "days_ago": 90 * (i + 1),
                })
        except Exception as exc:
            logger.warning("H2H API failed: %s", exc)

    return results[:8]
