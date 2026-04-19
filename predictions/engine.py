"""
KickTips Prediction Engine — Dixon-Coles Poisson v4
====================================================
Philosophy:
  Every published tip must answer two questions:
    1. What does our model say the probability is?
    2. Does the bookmaker agree, or are they mispricing it?

  Confidence is built from THREE components:
    A. Model probability — how likely is this outcome based on team data
    B. Bookmaker edge — how much does our probability exceed the bookie implied prob
    C. Data quality — penalise if sample size is small or lineups unknown

Markets published (in priority order via publisher.py):
  1. BTTS        — most consistent performer
  2. Over/Under  — goals lines 1.5 / 2.5 / 3.5
  3. DC          — safe market, two outcomes covered
  4. Corners     — tier 1/2 leagues only, both teams need real data
  5. 1X2         — restricted to 68%+ confidence only

Minimum published confidence: 65% global / 68% 1X2 / 60% corners
Minimum bookmaker edge:       5%
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ── Global thresholds ─────────────────────────────────────────────────────────
MIN_EDGE           = 0.05    # Min edge over bookmaker implied prob
MIN_GAMES          = 6       # Min games for a team stat to be trusted
MIN_BOOKIE_DECIMAL = 1.25    # Below this bookmaker implies >80% — skip
                              # Lowered from 1.50 to allow DC + Over 1.5
MIN_FAIR_DECIMAL   = 1.30    # Below this our model implies >77% — skip (1X2/goals/btts)
MIN_CONFIDENCE     = 65.0    # Global publish floor
REQUIRE_ODDS       = True    # Goals and 1X2 require bookmaker odds for value gate.
                              # Corners and BTTS have their own no-odds paths.

# ── Market-specific thresholds ────────────────────────────────────────────────
MIN_1X2_CONFIDENCE    = 66.0  # Lowered from 68.0 — MAX_DISPLAY_CONFIDENCE=67.0
                               # means 68 was unreachable. 66 allows 1X2 to fire
                               # on strong model signals while staying selective.
# 1X2 max odds cap — calibration data shows every 1X2 tip at odds > 2.50
# lost except one fluke. The model consistently mislabels underdogs as high
# confidence. Cap at 2.50 to kill long-shot 1X2 tips.
MAX_1X2_BOOKIE_DECIMAL = 2.50
MIN_CORNER_CONFIDENCE = 60.0  # Corners — permissive floor, tier+data gates do the work
MIN_CORNER_DECIMAL    = 1.50  # Corners need decent bookie prices to be worth publishing
MIN_CORNER_DATA_PTS   = 7     # Both teams need this many games before corners fires
MIN_DC_FAIR_DECIMAL   = 1.18  # DC covers 2 outcomes — fair price below 1.20 means
                               # model says >83% prob, bookmakers also well-priced there
MIN_GOALS_CONFIDENCE = 63.0  # Goals market — slightly more permissive than global 65%
MIN_PUBLISHABLE_DECIMAL = 1.40  # Any bookie price below this is not bettable —
                                 # Over 0.5, Under 4.5 etc. fail this gate automatically.
# ── Confidence display cap ────────────────────────────────────────────────────
# Calibration data (179 tips) shows the model is overconfident in every band.
# Temperature scaling T=8 collapses all confidence to ~52-54% — rank ordering
# is broken. Cap displayed confidence at 67% to avoid misleading punters until
# enough data exists to fit a proper calibration.
MAX_DISPLAY_CONFIDENCE = 67.0
# ── Empirical calibration table ───────────────────────────────────────────────
# Win rates derived from 604 graded tips (Apr 5 - Apr 13 2026).
# Used as a publish gate — tip types with historical win rate below
# MIN_EMPIRICAL_WR are blocked regardless of model confidence.
# Only applied when sample size is >= 10 (marked reliable).
# Update this table as more data accumulates.
MIN_EMPIRICAL_WR = 0.44   # Minimum acceptable historical win rate to publish

EMPIRICAL_WIN_RATES = {
    # Corners by line — most specific, highest confidence data
    "corners_Under_12.5": 0.913,   # 23 tips — publish freely
    "corners_Under_11.5": 0.786,   # 28 tips — publish freely
    "corners_Under_9.5":  0.769,   # 13 tips — publish freely
    "corners_Over_7.5":   0.812,   # 16 tips — publish freely
    "corners_Under_8.5":  0.667,   # 6 tips  — too small, allow
    "corners_Under_10.5": 0.375,   # 16 tips — BLOCK
    "corners_Under_7.5":  0.273,   # 11 tips — BLOCK
    # DC by type
    "dc_home_or_draw":    0.667,   # 63 tips — publish freely
    "dc_home_or_away":    0.684,   # 19 tips — publish freely
    "dc_away_or_draw":    0.489,   # 45 tips — borderline, allow for now
    # Goals by direction
    "ou_goals_over":      0.405,   # 84 tips — BLOCK
    "ou_goals_under":     0.453,   # 117 tips — BLOCK
    # BTTS
    "btts_no":            0.491,   # 55 tips — allow (CLV positive)
    "btts_yes":           0.489,   # 45 tips — allow (CLV neutral)
    # 1X2
    "1x2_home":           0.450,   # 40 tips — allow (only viable 1X2)
    "1x2_away":           0.308,   # 13 tips — BLOCK
}

def _empirical_key(market: str, tip: str) -> str:
    """Map a market + tip string to the calibration table key."""
    if market == "corners":
        return f"corners_{tip.replace(' ', '_')}"
    if market == "dc":
        if "Home or Draw" in tip: return "dc_home_or_draw"
        if "Home or Away" in tip: return "dc_home_or_away"
        if "Away or Draw" in tip: return "dc_away_or_draw"
    if market == "ou_goals":
        return "ou_goals_over" if "Over" in tip else "ou_goals_under"
    if market == "btts":
        return "btts_no" if "No" in tip else "btts_yes"
    if market == "1x2":
        if "Home Win" in tip: return "1x2_home"
        if "Away Win" in tip: return "1x2_away"
        return "1x2_draw"
    return ""

def _passes_calibration(market: str, tip: str) -> bool:
    """
    Returns True if this tip type has acceptable historical win rate.
    Tips with win rates below MIN_EMPIRICAL_WR are blocked.
    Unknown tip types (not in table) pass by default.
    """
    key = _empirical_key(market, tip)
    if not key or key not in EMPIRICAL_WIN_RATES:
        return True
    return EMPIRICAL_WIN_RATES[key] >= MIN_EMPIRICAL_WR
# ── No-odds penalty ───────────────────────────────────────────────────────────
NO_ODDS_PENALTY   = 15.0    # pp knocked off confidence when no bookmaker odds

# ── Dixon-Coles correction ────────────────────────────────────────────────────
# RHO is negative: 0-0 and 1-1 scorelines occur MORE than independent Poisson
# predicts (Dixon & Coles 1997 found rho ≈ -0.13). Positive RHO runs the
# correction backwards and systematically suppresses draws and clean sheets.
RHO      = -0.10
MAX_GOALS = 8

# ── Lines ─────────────────────────────────────────────────────────────────────
GOAL_LINES   = [1.5, 2.5, 3.5]
CORNER_LINES = [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]


# ── Poisson CDF inversion ─────────────────────────────────────────────────────

def _implied_mu_from_ou25_rate(rate: float) -> float:
    """
    Invert the Poisson CDF: find mu such that P(goals > 2.5) = rate.
    Replaces the old `combined_rate * 5.0` magic number which was wrong
    at the tails. Binary search converges in 50 steps to < 0.001 error.

    P(X > 2.5) = 1 - P(X <= 2) = 1 - e^{-mu}(1 + mu + mu^2/2)
    """
    rate = max(0.05, min(rate, 0.95))
    lo, hi = 0.1, 9.0
    for _ in range(50):
        mid = (lo + hi) / 2
        p_under = math.exp(-mid) * (1 + mid + mid ** 2 / 2)
        if (1 - p_under) < rate:
            lo = mid
        else:
            hi = mid
    return round((lo + hi) / 2, 4)


# ── Poisson engine ────────────────────────────────────────────────────────────

def _dc_tau(h, a, mu_h, mu_a, rho):
    if h == 0 and a == 0: return 1 - mu_h * mu_a * rho
    if h == 0 and a == 1: return 1 + mu_h * rho
    if h == 1 and a == 0: return 1 + mu_a * rho
    if h == 1 and a == 1: return 1 - rho
    return 1.0

def _pmf(k, lam):
    if lam <= 0: return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)

def _build_matrix(mu_h, mu_a):
    matrix = []
    for h in range(MAX_GOALS + 1):
        row = []
        for a in range(MAX_GOALS + 1):
            p = _pmf(h, mu_h) * _pmf(a, mu_a) * _dc_tau(h, a, mu_h, mu_a, RHO)
            row.append(p)
        matrix.append(row)
    total = sum(matrix[h][a] for h in range(MAX_GOALS+1) for a in range(MAX_GOALS+1))
    if total > 0:
        matrix = [[p/total for p in row] for row in matrix]
    return matrix

def _matrix_probs(matrix):
    hw = dr = aw = btts = 0.0
    over = {line: 0.0 for line in GOAL_LINES}
    for h in range(MAX_GOALS+1):
        for a in range(MAX_GOALS+1):
            p = matrix[h][a]
            if h > a:    hw += p
            elif h == a: dr += p
            else:        aw += p
            for line in GOAL_LINES:
                if h + a > line: over[line] += p
            if h > 0 and a > 0: btts += p
    return {"home": hw, "draw": dr, "away": aw, "over": over, "btts": btts}

def _derive_lambdas(home, away, league):
    def g(obj, attr, default=0.0):
        return getattr(obj, attr, default) or default

    # Use dynamically computed league average when available (injected by
    # run_predictions from actual finished fixtures). Falls back to the
    # static League.avg_goals field which is a reasonable league-level default.
    avg   = g(league, "computed_avg_goals", None) or g(league, "avg_goals", 2.65)
    # Tier-based home advantage — lower leagues have weaker home effect
    league_tier = getattr(league, "tier", 1) or 1
    h_split = 0.52 if league_tier >= 2 else 0.55
    a_split = 1 - h_split
    h_avg = avg * h_split
    a_avg = avg * a_split

    hgf = g(home, "rw_home_goals_for", 0) or g(home, "home_avg_goals_for", 1.4)
    hga = g(home, "rw_home_goals_against", 0) or g(home, "home_avg_goals_against", 1.1)
    agf = g(away, "rw_away_goals_for", 0) or g(away, "away_avg_goals_for", 1.1)
    aga = g(away, "rw_away_goals_against", 0) or g(away, "away_avg_goals_against", 1.4)

    mu_h = (hgf / h_avg) * (aga / h_avg) * h_avg if h_avg > 0 else 1.4
    mu_a = (agf / a_avg) * (hga / a_avg) * a_avg if a_avg > 0 else 1.1

    # xG blend — xG is a fundamentally better predictor than raw goals because
    # it strips out finishing luck. When real xG data is available, weight it
    # 70% xG vs 30% raw Poisson — not equal weight.
    hxg = g(home, "home_xg_for", 0)
    axg = g(away, "away_xg_for", 0)
    if hxg > 0 and axg > 0:
        mu_h = mu_h * 0.30 + hxg * 0.70
        mu_a = mu_a * 0.30 + axg * 0.70

    # O/U 2.5 rate blend — real match history more reliable than pure Poisson
    # for teams with consistent styles (e.g. low-block sides)
    h_ou25 = g(home, "home_ou25_over_rate", 0)
    a_ou25 = g(away, "away_ou25_over_rate", 0)
    if h_ou25 > 0 and a_ou25 > 0:
        combined_rate    = (h_ou25 + a_ou25) / 2
        # Proper Poisson inversion — replaces the old * 5.0 magic number
        # which was wrong at the tails (e.g. 80% rate → 4.0 goals, correct
        # answer is ~3.3). Binary search on the CDF is exact.
        implied_mu_total = _implied_mu_from_ou25_rate(combined_rate)
        current_total    = mu_h + mu_a
        if current_total > 0:
            scale = implied_mu_total / current_total
            scale = max(0.88, min(scale, 1.12))
            mu_h *= scale
            mu_a *= scale

    # League position quality adjustment
    # Use actual league size if stored, otherwise fall back to sensible defaults
    # by tier (Tier 1 top flights typically 18-20 teams, Tier 2 varies).
    h_pos = getattr(home, "league_position", None)
    a_pos = getattr(away, "league_position", None)
    league_size = (
        getattr(league, "team_count", None)
        or (20 if league_tier == 1 else 18)
    )
    if h_pos and a_pos and league_size > 0:
        h_rank_norm  = h_pos / league_size
        a_rank_norm  = a_pos / league_size
        quality_diff = a_rank_norm - h_rank_norm
        adjustment   = max(-0.06, min(quality_diff * 0.10, 0.06))
        mu_h = mu_h * (1 + adjustment)
        mu_a = mu_a * (1 - adjustment)

    return max(0.3, min(mu_h, 5.0)), max(0.3, min(mu_a, 5.0))


# ── Per-market confidence priors ──────────────────────────────────────────────
# Shrinkage target should reflect the natural base rate for each market,
# not a single universal 55%. Shrinking BTTS toward 55% is fine; shrinking
# DC toward 55% is wrong — DC covers 2 outcomes so ~65% is the natural base.
MARKET_PRIORS = {
    "btts":     50.0,   # binary, roughly 50/50 split across all matches
    "ou_goals": 52.0,   # O/U 2.5 goes over ~52% of matches on average
    "1x2":      45.0,   # home/draw/away — home wins ~45%, so 45% is midpoint
    "dc":       65.0,   # covers 2/3 outcomes — natural base ~65%
    "corners":  52.0,   # symmetric market
}
PRIOR_DEFAULT = 55.0    # fallback if market not in table
SHRINK        = 0.25    # reduced from 0.30 — less shrinkage once priors are correct

# ── Confidence builder ────────────────────────────────────────────────────────

def _build_confidence(model_prob: float, bookie_decimal: Optional[float], edge: Optional[float], market: str = "") -> dict:
    """
    Build confidence from model probability + bookmaker edge.
    Shrinks toward a market-specific prior rather than a universal 55%.
    """
    base = model_prob * 100

    if edge is not None:
        edge_bonus = min(edge * 100 * 0.20, 6.0)
        raw        = base + edge_bonus
        has_odds   = True
    else:
        raw      = base - NO_ODDS_PENALTY
        has_odds = False

    prior = MARKET_PRIORS.get(market, PRIOR_DEFAULT)
    conf  = (1 - SHRINK) * raw + SHRINK * prior

    return {
        "confidence": round(min(max(conf, 0), 82), 1),
        "has_odds":   has_odds,
    }


def _value_check(model_prob: float, bookie_decimal: Optional[float]) -> dict:
    """Standard value check — used by 1X2, Goals, BTTS, Corners."""
    fair_decimal = round(1.0 / model_prob, 2) if model_prob > 0 else 99.0

    if fair_decimal < MIN_FAIR_DECIMAL:
        return {"has_value": False, "edge": None, "bookie_decimal": bookie_decimal,
                "fair_decimal": fair_decimal, "bookie_implied": None}

    if bookie_decimal is None:
        if REQUIRE_ODDS:
            return {"has_value": False, "edge": None, "bookie_decimal": None,
                    "fair_decimal": fair_decimal, "bookie_implied": None}
        return {"has_value": True, "edge": None, "bookie_decimal": None,
                "fair_decimal": fair_decimal, "bookie_implied": None}

    if bookie_decimal <= MIN_BOOKIE_DECIMAL:
        return {"has_value": False, "edge": None, "bookie_decimal": bookie_decimal,
                "fair_decimal": fair_decimal, "bookie_implied": round(1/bookie_decimal, 4)}

    # Economic viability gate — below 1.40 the market is pricing near-certainty.
    # No edge calculation can save a tip at 1.08: even at 95% model probability
    # the EV is negative after bookmaker margin. Kill it here before edge check.
    if bookie_decimal < MIN_PUBLISHABLE_DECIMAL:
        return {"has_value": False, "edge": None, "bookie_decimal": bookie_decimal,
                "fair_decimal": fair_decimal, "bookie_implied": round(1/bookie_decimal, 4)}

    bookie_implied = 1.0 / bookie_decimal
    edge           = model_prob - bookie_implied

    return {
        "has_value":      edge >= MIN_EDGE,
        "edge":           round(edge, 4),
        "bookie_decimal": bookie_decimal,
        "fair_decimal":   fair_decimal,
        "bookie_implied": round(bookie_implied, 4),
    }


# ── Adjustments ───────────────────────────────────────────────────────────────

def _form_factor(form_str: str) -> float:
    if not form_str: return 1.0
    chars   = list(form_str.upper()[:6])
    weights = [1.0, 0.85, 0.72, 0.61, 0.52, 0.44]
    score = total = 0.0
    for i, ch in enumerate(chars):
        w      = weights[i] if i < len(weights) else 0.3
        total += w
        if ch == "W":   score += w
        elif ch == "L": score -= w
    return max(0.88, min(1.12, 1.0 + (score/total)*0.12)) if total else 1.0

def _sample_penalty(home, away) -> float:
    mg = min(getattr(home, "games_played", 0) or 0,
             getattr(away, "games_played", 0) or 0)
    if mg >= 8:
        return 0.0
    rw_h  = getattr(home, "rw_home_goals_for", 0) or 0
    rw_a  = getattr(away, "rw_away_goals_for",  0) or 0
    avg_h = getattr(home, "home_avg_goals_for", 1.5) or 1.5
    avg_a = getattr(away, "away_avg_goals_for", 1.2) or 1.2
    if (rw_h > 0 and rw_a > 0) or (avg_h != 1.5 and avg_a != 1.2):
        return 0.0
    if mg < 3: return 20.0
    if mg < 5: return 10.0
    return 3.0

def _lineup_penalty(home, away) -> float:
    m = (getattr(home, "key_players_missing", 0) or 0) + \
        (getattr(away, "key_players_missing", 0) or 0)
    return min(m * 4.0, 15.0)

def _skip(reason): return {"skip_reason": reason, "tip": "", "confidence": 0}


# ── H2H time-decay weighting ──────────────────────────────────────────────────

def _h2h_weights(h2h_results: list) -> list:
    """
    Return exponential decay weights for H2H results based on days_ago.
    Half-life = 365 days — a match from 1 year ago gets weight ~0.37,
    2 years ago ~0.14, so recent meetings dominate the blend.
    If days_ago is not in the result dict (API h2h), use position-based fallback.
    """
    import math as _math
    HALF_LIFE = 365.0
    weights = []
    for i, r in enumerate(h2h_results):
        days = r.get("days_ago", 90 * (i + 1))
        w = _math.exp(-days * _math.log(2) / HALF_LIFE)
        weights.append(max(w, 0.01))
    return weights


# ══════════════════════════════════════════════════════════════════════════════
# MARKET 1: 1X2
# ══════════════════════════════════════════════════════════════════════════════

def predict_1x2(home, away, h2h_results, league, odds=None):
    try:
        def _has_data(t):
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix     = _build_matrix(mu_h, mu_a)
        probs      = _matrix_probs(matrix)
        hp, dp, ap = probs["home"], probs["draw"], probs["away"]

        # Form adjustment
        hp *= _form_factor(getattr(home, "form_home", "") or "")
        ap *= _form_factor(getattr(away, "form_away", "") or "")

        # H2H proportional blend — time-decayed so recent meetings dominate
        if h2h_results:
            weights  = _h2h_weights(h2h_results)
            total_w  = sum(weights)
            hw_rate  = sum(w for r, w in zip(h2h_results, weights) if r.get("winner") == "home") / total_w
            aw_rate  = sum(w for r, w in zip(h2h_results, weights) if r.get("winner") == "away") / total_w
            dr_rate  = 1 - hw_rate - aw_rate
            # Weight by effective sample size (sum of weights), capped at 0.25
            h2h_weight = min(total_w / 20, 0.25)
            hp = hp * (1 - h2h_weight) + hw_rate * h2h_weight
            dp = dp * (1 - h2h_weight) + dr_rate * h2h_weight
            ap = ap * (1 - h2h_weight) + aw_rate * h2h_weight

        total = hp + dp + ap
        hp /= total; dp /= total; ap /= total

        # Signal agreement — blend Poisson probs with empirical win/draw rates.
        # When both signals agree confidence rises; when they disagree it falls.
        # Only apply when team has real historical data (not model defaults).
        h_wr = getattr(home, "home_win_rate",  0.40) or 0.40
        a_wr = getattr(away, "away_win_rate",  0.28) or 0.28
        h_dr = getattr(home, "home_draw_rate", 0.28) or 0.28
        a_dr = getattr(away, "away_draw_rate", 0.30) or 0.30
        has_real_rates = (
            h_wr != 0.40 and a_wr != 0.28   # differ from model defaults
        )
        if has_real_rates:
            # Empirical draw rate = average of both teams home/away draw rates
            emp_dr = (h_dr + a_dr) / 2
            # Normalise empirical win rates to sum to 1 with draw
            total_wr = h_wr + a_wr + emp_dr
            emp_hp = h_wr / total_wr
            emp_dp = emp_dr / total_wr
            emp_ap = a_wr / total_wr
            # 30% empirical, 70% Poisson — empirical corrects but doesn't dominate
            EMPIRICAL_WEIGHT = 0.30
            hp = hp * (1 - EMPIRICAL_WEIGHT) + emp_hp * EMPIRICAL_WEIGHT
            dp = dp * (1 - EMPIRICAL_WEIGHT) + emp_dp * EMPIRICAL_WEIGHT
            ap = ap * (1 - EMPIRICAL_WEIGHT) + emp_ap * EMPIRICAL_WEIGHT
            # Renormalise
            total = hp + dp + ap
            hp /= total; dp /= total; ap /= total

            # Signal agreement bonus/penalty — if Poisson and empirical agree
            # on the winner, add a small confidence boost later
            poisson_winner = max(
                [("home", probs["home"]), ("draw", probs["draw"]), ("away", probs["away"])],
                key=lambda x: x[1])[0]
            emp_winner = max(
                [("home", emp_hp), ("draw", emp_dp), ("away", emp_ap)],
                key=lambda x: x[1])[0]
            signals_agree = (poisson_winner == emp_winner)
        else:
            signals_agree = None

        best_prob, tip, odds_key = max(
            [(hp, "Home Win", "home"), (dp, "Draw", "draw"), (ap, "Away Win", "away")],
            key=lambda x: x[0])

        bookie_dec = (odds.get("1x2", {}).get(odds_key)) if odds else None

        # Kill long-shot 1X2 tips — calibration data shows every tip at
        # odds > 2.50 lost except one. The model mislabels underdogs.
        if bookie_dec and bookie_dec > MAX_1X2_BOOKIE_DECIMAL:
            return _skip("no_value")

        vc = _value_check(best_prob, bookie_dec)
        if not vc["has_value"]:
            return _skip("no_value")

        cb         = _build_confidence(best_prob, bookie_dec, vc["edge"], market="1x2")
        confidence = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)

        # Signal agreement adjustment — ±2pp based on whether Poisson and
        # empirical win rates point to the same outcome
        if signals_agree is True:
            confidence += 2.0   # both models agree — small boost
        elif signals_agree is False:
            confidence -= 3.0   # models disagree — penalise more than boost

        # Apply display cap — confidence above this is not meaningful given
        # current calibration data. Keeps displayed numbers honest.
        confidence = round(max(0, min(confidence, MAX_DISPLAY_CONFIDENCE)), 1)

        if confidence < MIN_1X2_CONFIDENCE:
            return _skip("low_confidence")

        if not _passes_calibration("1x2", tip):
            return _skip("low_confidence")

        return {
            "tip": tip, "confidence": confidence, "skip_reason": "",
            "expected_value": round(best_prob * 100, 1),
            "bookie_decimal": vc["bookie_decimal"],
            "edge":           vc["edge"],
            "home_prob":  round(hp, 4),
            "draw_prob":  round(dp, 4),
            "away_prob":  round(ap, 4),
        }
    except Exception as exc:
        logger.error("1X2 error: %s", exc)
        return _skip("insufficient_data")


# ══════════════════════════════════════════════════════════════════════════════
# MARKET 2: Over/Under Goals
# ══════════════════════════════════════════════════════════════════════════════
def predict_goals(home, away, h2h_results, league, odds=None):
    try:
        def _has_data(t):
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")
        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix     = _build_matrix(mu_h, mu_a)
        probs      = _matrix_probs(matrix)
        expected   = mu_h + mu_a
        # H2H goals blend — time-decayed weighted average
        if h2h_results:
            weights = _h2h_weights(h2h_results)
            h2h_pairs = [
                (r, w) for r, w in zip(h2h_results, weights)
                if r.get("home_score") is not None
            ]
            if h2h_pairs:
                total_w = sum(w for _, w in h2h_pairs)
                h2h_avg = sum(
                    ((r.get("home_score") or 0) + (r.get("away_score") or 0)) * w
                    for r, w in h2h_pairs
                ) / total_w
                expected = expected * 0.75 + h2h_avg * 0.25
        best = None
        for line in GOAL_LINES:
            gap = abs(expected - line)
            # Tightened upper bound from 1.8 to 1.4 — stops borderline Over 3.5
            # tips on matches where expected goals is only marginally above the line
            if gap < 0.15 or gap > 1.4:
                continue
            over_prob  = probs["over"].get(line, 0)
            under_prob = 1 - over_prob
            if over_prob >= under_prob:
                model_prob, side, odds_key = over_prob,  "Over",  "over"
            else:
                model_prob, side, odds_key = under_prob, "Under", "under"
            bookie_dec = None
            if odds and "ou_goals" in odds:
                bookie_dec = odds["ou_goals"].get(str(line), {}).get(odds_key)
            vc = _value_check(model_prob, bookie_dec)
            if not vc["has_value"]:
                continue
            cb   = _build_confidence(model_prob, bookie_dec, vc["edge"], market="ou_goals")
            conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
            conf = round(max(0, min(conf, MAX_DISPLAY_CONFIDENCE)), 1)
            if conf < MIN_GOALS_CONFIDENCE:
                continue
            tip_str = f"{side} {line}"
            if not _passes_calibration("ou_goals", tip_str):
                continue
            candidate = {
                "tip": tip_str, "confidence": conf, "skip_reason": "",
                "expected_value": round(expected, 2),
                "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
            }
            # Select by highest confidence — safer tip wins over highest edge tip
            if best is None or conf > best["confidence"]:
                best = candidate
        return best if best else _skip("no_value")
    except Exception as exc:
        logger.error("Goals error: %s", exc)
        return _skip("insufficient_data")

# ══════════════════════════════════════════════════════════════════════════════
# MARKET 3: BTTS
# ══════════════════════════════════════════════════════════════════════════════

def predict_btts(home, away, h2h_results, league, odds=None):
    try:
        def _has_data(t):
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix     = _build_matrix(mu_h, mu_a)
        probs      = _matrix_probs(matrix)
        btts       = probs["btts"]

        # Blend with historical BTTS rates
        hb = getattr(home, "home_btts_rate", 0) or 0
        ab = getattr(away, "away_btts_rate", 0) or 0
        if hb > 0 and ab > 0:
            btts = btts * 0.60 + ((hb + ab) / 2) * 0.40

        # O/U 2.5 rate hint — teams that go over 2.5 frequently tend to have
        # BTTS in most matches. Small 10% nudge when both rates are available.
        h_ou25 = getattr(home, "home_ou25_over_rate", 0) or 0
        a_ou25 = getattr(away, "away_ou25_over_rate", 0) or 0
        if h_ou25 > 0 and a_ou25 > 0:
            ou_btts_hint = (h_ou25 + a_ou25) / 2   # avg O/U 2.5 over rate
            btts = btts * 0.90 + ou_btts_hint * 0.10

        # H2H blend — time-decayed
        if h2h_results:
            weights = _h2h_weights(h2h_results)
            total_w = sum(weights)
            h2h_btts_rate = sum(
                w for r, w in zip(h2h_results, weights)
                if (r.get("home_score") or 0) > 0 and (r.get("away_score") or 0) > 0
            ) / total_w
            btts = btts * 0.80 + h2h_btts_rate * 0.20

        no_btts = 1 - btts

        if abs(btts - 0.5) < 0.06:
            return _skip("dead_zone")

        if btts >= no_btts:
            model_prob, tip, odds_key = btts,    "BTTS Yes", "yes"
        else:
            model_prob, tip, odds_key = no_btts, "BTTS No",  "no"

        bookie_dec = (odds.get("btts", {}).get(odds_key)) if odds else None
        vc = _value_check(model_prob, bookie_dec)
        if not vc["has_value"]:
            return _skip("no_value")

        # BTTS dead zone — odds between 1.70 and 2.00 perform at only 40.4%
        # historically (100 tips). Below 1.70 wins 56.1%, above 2.00 wins 58.3%.
        # Skip the middle band where neither side has clear conviction.
        if bookie_dec and 1.70 <= bookie_dec <= 2.00:
            return _skip("no_value")

        cb   = _build_confidence(model_prob, bookie_dec, vc["edge"], market="btts")
        conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
        conf = round(max(0, min(conf, MAX_DISPLAY_CONFIDENCE)), 1)

        if conf < MIN_CONFIDENCE:
            return _skip("low_confidence")

        if not _passes_calibration("btts", tip):
            return _skip("low_confidence")

        return {
            "tip": tip, "confidence": conf, "skip_reason": "",
            "expected_value": round(model_prob * 100, 1),
            "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
        }
    except Exception as exc:
        logger.error("BTTS error: %s", exc)
        return _skip("insufficient_data")


# ══════════════════════════════════════════════════════════════════════════════
# MARKET 4: Double Chance
# ══════════════════════════════════════════════════════════════════════════════

def predict_double_chance(home, away, h2h_results, league, odds=None):
    try:
        def _has_data(t):
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix     = _build_matrix(mu_h, mu_a)
        probs      = _matrix_probs(matrix)
        hw, dr, aw = probs["home"], probs["draw"], probs["away"]

        combos = {
            "Home or Draw": hw + dr,
            "Away or Draw": aw + dr,
            "Home or Away": hw + aw,
        }

        # H2H blend — time-decayed, same approach as 1X2
        if h2h_results:
            weights  = _h2h_weights(h2h_results)
            total_w  = sum(weights)
            hw_rate  = sum(w for r, w in zip(h2h_results, weights) if r.get("winner") == "home") / total_w
            aw_rate  = sum(w for r, w in zip(h2h_results, weights) if r.get("winner") == "away") / total_w
            dr_rate  = 1 - hw_rate - aw_rate
            h2h_weight = min(total_w / 20, 0.20)  # max 20% H2H influence on DC
            hw = hw * (1 - h2h_weight) + hw_rate * h2h_weight
            dr = dr * (1 - h2h_weight) + dr_rate * h2h_weight
            aw = aw * (1 - h2h_weight) + aw_rate * h2h_weight
            # Renormalise
            total = hw + dr + aw
            hw /= total; dr /= total; aw /= total
            combos = {
                "Home or Draw": hw + dr,
                "Away or Draw": aw + dr,
                "Home or Away": hw + aw,
            }

        tip, model_prob = max(combos.items(), key=lambda x: x[1])

        # DC needs clear dominance — 72% model floor
        if model_prob < 0.72:
            return _skip("low_confidence")

        # DC-specific fair decimal floor.
        # DC covers 2 of 3 outcomes so 80%+ model prob is completely normal.
        # MIN_DC_FAIR_DECIMAL = 1.20 means only block when model says >83%
        # (fair price below 1.20) — at that level bookmakers are also very short.
        fair_decimal = round(1.0 / model_prob, 2) if model_prob > 0 else 99.0
        if fair_decimal < MIN_DC_FAIR_DECIMAL:
            return _skip("no_value")

        # Confirmed DC API mapping:
        #   null participant = 12 (Home or Away)
        #   home participant = 1x (Home or Draw)
        #   away participant = x2 (Away or Draw)
        dc_odds_map = {"Home or Draw": "1x", "Away or Draw": "x2", "Home or Away": "12"}
        odds_key   = dc_odds_map.get(tip)
        bookie_dec = (odds.get("dc", {}).get(odds_key)) if (odds and odds_key) else None

        # Without bookie odds require stronger model signal
        if bookie_dec is None and model_prob < 0.76:
            return _skip("no_value")

        # DC-specific edge check (bypasses global MIN_FAIR_DECIMAL)
        if bookie_dec is None:
            vc = {"has_value": True, "edge": None, "bookie_decimal": None,
                  "fair_decimal": fair_decimal, "bookie_implied": None}
        else:
            bookie_implied = 1.0 / bookie_dec
            edge           = model_prob - bookie_implied
            vc = {
                "has_value":      edge >= MIN_EDGE,
                "edge":           round(edge, 4),
                "bookie_decimal": bookie_dec,
                "fair_decimal":   fair_decimal,
                "bookie_implied": round(bookie_implied, 4),
            }
            if not vc["has_value"]:
                return _skip("no_value")

        cb   = _build_confidence(model_prob, bookie_dec, vc["edge"], market="dc")
        # Cap DC at 78 — safety market, not a high-confidence call
        conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
        conf = round(max(0, min(conf, MAX_DISPLAY_CONFIDENCE)), 1)

        if conf < MIN_CONFIDENCE:
            return _skip("low_confidence")

        if not _passes_calibration("dc", tip):
            return _skip("low_confidence")

        return {
            "tip": tip, "confidence": conf, "skip_reason": "",
            "expected_value": round(model_prob * 100, 1),
            "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
        }
    except Exception as exc:
        logger.error("DC error: %s", exc)
        return _skip("insufficient_data")


# ══════════════════════════════════════════════════════════════════════════════
# MARKET 5: Corners
# ══════════════════════════════════════════════════════════════════════════════

def predict_corners(home, away, referee, h2h_results, league, odds=None):
    try:
        # Tier 1 and 2 only — lower tiers have thin markets and unreliable data
        league_tier = getattr(league, "tier", 3) or 3
        if league_tier > 2:
            return _skip("insufficient_data")

        # Both teams need sufficient match history
        def _has_data(t):
            return (getattr(t, "games_played", 0) or 0) >= MIN_CORNER_DATA_PTS
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        hcf = getattr(home, "home_avg_corners_for",     0) or 0
        hca = getattr(home, "home_avg_corners_against", 0) or 0
        acf = getattr(away, "away_avg_corners_for",     0) or 0
        aca = getattr(away, "away_avg_corners_against", 0) or 0

        # Both teams must have real corner data — no league average fallback
        _HOME_DEFAULTS = (5.0, 4.5)
        _AWAY_DEFAULTS = (4.5, 5.2)
        home_corners_real = (round(hcf, 1), round(hca, 1)) != _HOME_DEFAULTS
        away_corners_real = (round(acf, 1), round(aca, 1)) != _AWAY_DEFAULTS

        if not home_corners_real or not away_corners_real:
            return _skip("insufficient_data")

        # Expected total corners for the match
        expected = (hcf + aca) / 2 + (acf + hca) / 2
        if expected < 4.0:
            return _skip("insufficient_data")

        # Referee adjustment
        if referee and (getattr(referee, "games_officiated", 0) or 0) >= 8:
            ref_y     = getattr(referee, "avg_yellows_per_game", 0) or 0
            avg_cards = getattr(league, "avg_cards", 3.5) or 3.5
            expected *= 1 + ((ref_y / avg_cards) - 1) * 0.08

        # H2H corner history
        if h2h_results:
            hc = [r["total_corners"] for r in h2h_results if r.get("total_corners")]
            if hc:
                expected = expected * 0.80 + (sum(hc)/len(hc)) * 0.20

        best = None
        for line in CORNER_LINES:
            gap = abs(expected - line)
            if gap < 0.25 or gap > 3.5:
                continue

            side     = "Over" if expected > line else "Under"
            odds_key = side.lower()

            # Normal approximation for corner total probability
            sigma = math.sqrt(expected)
            z     = (line + 0.5 - expected) / sigma if sigma > 0 else 0

            def _ncdf(z):
                t = 1/(1+0.2316419*abs(z))
                p = 1-(0.31938153*t - 0.356563782*t**2 + 1.781477937*t**3
                       - 1.821255978*t**4 + 1.330274429*t**5) * math.exp(-z**2/2) / math.sqrt(2*math.pi)
                return p if z >= 0 else 1-p

            over_prob  = 1 - _ncdf(z)
            model_prob = over_prob if side == "Over" else 1 - over_prob

            # Corners: never require bookie odds — fire on model data alone
            bookie_dec = None
            if odds and "ou_corners" in odds:
                bookie_dec = odds["ou_corners"].get(str(line), {}).get(odds_key)
                if bookie_dec is not None and bookie_dec < MIN_CORNER_DECIMAL:
                    continue

            edge = None
            if bookie_dec is not None:
                bookie_implied = 1.0 / bookie_dec
                edge = round(model_prob - bookie_implied, 4)
                if edge < MIN_EDGE:
                    continue

            cb   = _build_confidence(model_prob, bookie_dec, edge, market="corners")
            vc   = {"has_value": True, "edge": edge, "bookie_decimal": bookie_dec}
            conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
            conf = round(max(0, min(conf, MAX_DISPLAY_CONFIDENCE)), 1)

            if conf < MIN_CORNER_CONFIDENCE:
                continue

            corner_tip = f"{side} {line}"
            if not _passes_calibration("corners", corner_tip):
                continue

            candidate = {
                "tip": corner_tip, "confidence": conf, "skip_reason": "",
                "expected_value": round(expected, 2),
                "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
            }
            if best is None or (vc["edge"] or 0) > (best.get("edge") or 0):
                best = candidate

        return best if best else _skip("no_value")
    except Exception as exc:
        logger.error("Corners error: %s", exc)
        return _skip("insufficient_data")
