"""
KickTips Prediction Engine — Dixon-Coles Poisson v3
====================================================
Philosophy:
  Every published tip must answer two questions:
    1. What does our model say the probability is?
    2. Does the bookmaker agree, or are they mispricing it?

  Confidence is built from THREE components:
    A. Model probability — how likely is this outcome based on team data
    B. Bookmaker edge — how much does our probability exceed the bookie's implied prob
    C. Data quality — penalise if sample size is small or lineups unknown

  A punter seeing 72% confidence should know: our model gives this outcome ~72%
  probability AND the bookie is pricing it lower, meaning there is mathematical value.

Markets published (in priority order):
  1. 1X2          — most punter-friendly, highest trust
  2. Over/Under   — goals lines 1.5 / 2.5 / 3.5
  3. BTTS Yes/No  — simple binary, widely available
  4. Double Chance — safe, but only when strongly supported
  5. Corners      — only when team has real corner data

Minimum published confidence: 60%
Minimum bookmaker edge:       2% (MIN_EDGE)
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_EDGE           = 0.05    # Min edge over bookmaker implied prob (raised from 0.03 — tighter quality gate)
MIN_GAMES          = 6       # Min games for a team stat to be trusted
MIN_BOOKIE_DECIMAL = 1.25    # Below this the bookmaker implies >80% — skip (lowered from 1.50 to allow DC + Over 1.5)
MIN_FAIR_DECIMAL   = 1.30    # Below this our model implies >77% — skip
MIN_CONFIDENCE     = 65.0    # Never publish below this (raised from 63 — fewer but better tips)
REQUIRE_ODDS       = True    # Never publish without real bookmaker odds — no odds = no edge

# Corners-specific floor — corners markets are sharp and low-liquidity.
# A corners tip at 1.40 decimal odds returns R0.40 per R1 — not worth the risk.
# Only publish corners when the bookmaker is offering real value.
MIN_CORNER_DECIMAL   = 1.65   # Minimum decimal odds for any corners tip
MIN_CORNER_DATA_PTS  = 5      # Minimum matches with real corner data per team
MIN_1X2_CONFIDENCE  = 68.0    # 1X2 is weakest market — stricter floor than global 65%
MIN_CORNER_CONFIDENCE = 68.0  # Corners market — strict floor until model is proven

# No-odds fallback: publish but knock confidence
NO_ODDS_PENALTY   = 15.0    # pp knocked off confidence when no bookmaker odds available

# Dixon-Coles correction
RHO      = 0.10
MAX_GOALS = 8

# Lines
GOAL_LINES   = [1.5, 2.5, 3.5]
CORNER_LINES = [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]


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
            if h > a:   hw += p
            elif h == a: dr += p
            else:        aw += p
            for line in GOAL_LINES:
                if h + a > line: over[line] += p
            if h > 0 and a > 0: btts += p
    return {"home": hw, "draw": dr, "away": aw, "over": over, "btts": btts}

def _derive_lambdas(home, away, league):
    def g(obj, attr, default=0.0):
        return getattr(obj, attr, default) or default

    avg = g(league, "avg_goals", 2.65)
    h_avg = avg * 0.55
    a_avg = avg * 0.45

    hgf = g(home, "rw_home_goals_for", 0) or g(home, "home_avg_goals_for", 1.4)
    hga = g(home, "rw_home_goals_against", 0) or g(home, "home_avg_goals_against", 1.1)
    agf = g(away, "rw_away_goals_for", 0) or g(away, "away_avg_goals_for", 1.1)
    aga = g(away, "rw_away_goals_against", 0) or g(away, "away_avg_goals_against", 1.4)

    mu_h = (hgf / h_avg) * (aga / h_avg) * h_avg if h_avg > 0 else 1.4
    mu_a = (agf / a_avg) * (hga / a_avg) * a_avg if a_avg > 0 else 1.1

    # xG blend — only when real xG data is available
    hxg = g(home, "home_xg_for", 0)
    axg = g(away, "away_xg_for", 0)
    if hxg > 0 and axg > 0:
        mu_h = mu_h * 0.5 + hxg * 0.5
        mu_a = mu_a * 0.5 + axg * 0.5

    # O/U 2.5 rate blend — use league standing O/U rates when available.
    # These represent actual match history at home/away and are more reliable
    # than pure Poisson when teams have consistent style (e.g. low-block sides).
    h_ou25 = g(home, "home_ou25_over_rate", 0)
    a_ou25 = g(away, "away_ou25_over_rate", 0)
    if h_ou25 > 0 and a_ou25 > 0:
        # Derive an implied mu blend: if both teams go Over 2.5 70% of the time,
        # the implied match lambda is higher. Scale: 50% rate ≈ 2.5 expected goals.
        combined_rate = (h_ou25 + a_ou25) / 2
        implied_mu_total = max(1.5, min(combined_rate * 5.0, 5.5))  # rough mapping
        current_total = mu_h + mu_a
        if current_total > 0:
            scale = implied_mu_total / current_total
            scale = max(0.88, min(scale, 1.12))  # cap at ±12% adjustment
            mu_h *= scale
            mu_a *= scale

    # League position penalty/bonus — top-6 home team vs bottom-6 away team
    # gets a small attacking boost; reverse degrades the home side slightly.
    h_pos = getattr(home, "league_position", None)
    a_pos = getattr(away, "league_position", None)
    league_size = 18  # safe default; actual size doesn't matter much here
    if h_pos and a_pos:
        h_rank_norm = h_pos / league_size   # 0 = top, 1 = bottom
        a_rank_norm = a_pos / league_size
        # Home team quality vs away team quality
        quality_diff = a_rank_norm - h_rank_norm  # positive = home team higher ranked
        adjustment = max(-0.06, min(quality_diff * 0.10, 0.06))
        mu_h = mu_h * (1 + adjustment)
        mu_a = mu_a * (1 - adjustment)

    return max(0.3, min(mu_h, 5.0)), max(0.3, min(mu_a, 5.0))


# ── Confidence builder ────────────────────────────────────────────────────────

def _build_confidence(model_prob: float, bookie_decimal: Optional[float], edge: Optional[float]) -> dict:
    """
    Build confidence score from model probability + bookmaker edge.

    Calibration fix (v4):
      The previous formula inflated confidence by adding up to +20pp edge bonus
      on top of model_prob%, causing the 75-80%+ bands to publish at confidence
      levels they could not actually achieve.

      New formula:
        1. Start from model_prob% (0-100 scale)
        2. Add a SMALL edge bonus: each 1% of edge adds 0.2pp (cap +6pp)
           Edge signals value but cannot substitute for model accuracy.
        3. Apply Platt-style shrinkage toward a 55% prior:
           conf = 0.70 * raw + 0.30 * 55
           Pulls overconfident scores toward reality.
        4. No-odds penalty: -15pp before shrinkage if no bookmaker odds.
        5. Clamp to [0, 82] — never claim more than 82% certainty.

    Examples:
      model=70%, edge=10%  -> raw=72 -> shrunk=66.9%
      model=75%, edge=15%  -> raw=78 -> shrunk=71.1%
      model=65%, no odds   -> raw=50 -> shrunk=49.5% (below MIN, skipped)
    """
    base = model_prob * 100

    if edge is not None:
        edge_bonus = min(edge * 100 * 0.20, 6.0)   # was 0.5/cap 20 — massively reduced
        raw = base + edge_bonus
        has_odds = True
    else:
        raw = base - NO_ODDS_PENALTY
        has_odds = False

    # Platt-style shrinkage toward 55% prior — prevents overconfidence
    PRIOR = 55.0
    SHRINK = 0.30
    conf = (1 - SHRINK) * raw + SHRINK * PRIOR

    return {
        "confidence": round(min(max(conf, 0), 82), 1),
        "has_odds":   has_odds,
    }


def _value_check(model_prob: float, bookie_decimal: Optional[float]) -> dict:
    """Compare model probability against bookmaker implied probability."""
    fair_decimal = round(1.0 / model_prob, 2) if model_prob > 0 else 99.0

    # Block near-certain outcomes — no edge possible
    if fair_decimal < MIN_FAIR_DECIMAL:
        return {"has_value": False, "edge": None, "bookie_decimal": bookie_decimal,
                "fair_decimal": fair_decimal, "bookie_implied": None}

    if bookie_decimal is None:
        # No odds — cannot validate edge against bookmaker, do not publish
        if REQUIRE_ODDS:
            return {"has_value": False, "edge": None, "bookie_decimal": None,
                    "fair_decimal": fair_decimal, "bookie_implied": None}
        # REQUIRE_ODDS=False fallback: publish with confidence penalty
        return {"has_value": True, "edge": None, "bookie_decimal": None,
                "fair_decimal": fair_decimal, "bookie_implied": None}

    if bookie_decimal <= MIN_BOOKIE_DECIMAL:
        return {"has_value": False, "edge": None, "bookie_decimal": bookie_decimal,
                "fair_decimal": fair_decimal, "bookie_implied": round(1/bookie_decimal, 4)}

    bookie_implied = 1.0 / bookie_decimal
    edge = model_prob - bookie_implied

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
    chars = list(form_str.upper()[:6])
    weights = [1.0, 0.85, 0.72, 0.61, 0.52, 0.44]
    score = total = 0.0
    for i, ch in enumerate(chars):
        w = weights[i] if i < len(weights) else 0.3
        total += w
        if ch == "W": score += w
        elif ch == "L": score -= w
    return max(0.88, min(1.12, 1.0 + (score/total)*0.12)) if total else 1.0

def _sample_penalty(home, away) -> float:
    mg = min(getattr(home, "games_played", 0) or 0,
             getattr(away, "games_played", 0) or 0)
    if mg >= 8:
        return 0.0
    # If rw_ data or non-default avg goals exist, treat as sufficient
    rw_h = (getattr(home, "rw_home_goals_for", 0) or 0)
    rw_a = (getattr(away, "rw_away_goals_for",  0) or 0)
    avg_h = (getattr(home, "home_avg_goals_for", 1.5) or 1.5)
    avg_a = (getattr(away, "away_avg_goals_for", 1.2) or 1.2)
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


# ══════════════════════════════════════════════════════════════════════════════
# MARKET 1: 1X2
# ══════════════════════════════════════════════════════════════════════════════

def predict_1x2(home, away, h2h_results, league, odds=None):
    try:
        def _has_data(t):
            # Require real match history — loose fallbacks caused bad predictions
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix = _build_matrix(mu_h, mu_a)
        probs  = _matrix_probs(matrix)

        hp, dp, ap = probs["home"], probs["draw"], probs["away"]

        # Form
        hp *= _form_factor(getattr(home, "form_home", "") or "")
        ap *= _form_factor(getattr(away, "form_away", "") or "")

        # H2H — apply proportional weighting, not a hard 65% threshold
        if h2h_results:
            n = len(h2h_results)
            hw_rate = sum(1 for r in h2h_results if r.get("winner") == "home") / n
            aw_rate = sum(1 for r in h2h_results if r.get("winner") == "away") / n
            dr_rate = 1 - hw_rate - aw_rate
            # Blend model probs with H2H rates (20% H2H weight, increases with sample)
            h2h_weight = min(n / 20, 0.25)  # max 25% H2H influence at n=20
            hp = hp * (1 - h2h_weight) + hw_rate * h2h_weight
            dp = dp * (1 - h2h_weight) + dr_rate * h2h_weight
            ap = ap * (1 - h2h_weight) + aw_rate * h2h_weight

        total = hp + dp + ap
        hp /= total; dp /= total; ap /= total

        best_prob, tip, odds_key = max(
            [(hp, "Home Win", "home"), (dp, "Draw", "draw"), (ap, "Away Win", "away")],
            key=lambda x: x[0])

        bookie_dec = (odds.get("1x2", {}).get(odds_key)) if odds else None
        vc = _value_check(best_prob, bookie_dec)
        if not vc["has_value"]:
            return _skip("no_value")

        cb = _build_confidence(best_prob, bookie_dec, vc["edge"])
        confidence = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
        confidence = round(max(0, min(confidence, 82)), 1)

        if confidence < MIN_1X2_CONFIDENCE:
            return _skip("low_confidence")

        return {
            "tip": tip, "confidence": confidence, "skip_reason": "",
            "expected_value": round(best_prob * 100, 1),
            "bookie_decimal": vc["bookie_decimal"],
            "edge": vc["edge"],
            "home_prob": round(hp, 4), "draw_prob": round(dp, 4), "away_prob": round(ap, 4),
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
            # Require real match history — loose fallbacks caused bad predictions
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix = _build_matrix(mu_h, mu_a)
        probs  = _matrix_probs(matrix)
        expected = mu_h + mu_a

        # H2H blend
        if h2h_results:
            h2h_g = [(r.get("home_score") or 0) + (r.get("away_score") or 0)
                     for r in h2h_results if r.get("home_score") is not None]
            if h2h_g:
                expected = expected * 0.75 + (sum(h2h_g)/len(h2h_g)) * 0.25

        best = None
        for line in GOAL_LINES:
            gap = abs(expected - line)
            if gap < 0.20 or gap > 1.8:
                continue

            over_prob  = probs["over"].get(line, 0)
            under_prob = 1 - over_prob

            if over_prob >= under_prob:
                model_prob, side, odds_key = over_prob, "Over", "over"
            else:
                model_prob, side, odds_key = under_prob, "Under", "under"

            bookie_dec = None
            if odds and "ou_goals" in odds:
                bookie_dec = odds["ou_goals"].get(str(line), {}).get(odds_key)

            vc = _value_check(model_prob, bookie_dec)
            if not vc["has_value"]:
                continue

            cb = _build_confidence(model_prob, bookie_dec, vc["edge"])
            conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
            conf = round(max(0, min(conf, 82)), 1)

            if conf < MIN_CONFIDENCE:
                continue

            candidate = {
                "tip": f"{side} {line}", "confidence": conf, "skip_reason": "",
                "expected_value": round(expected, 2),
                "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
            }
            if best is None or (vc["edge"] or 0) > (best.get("edge") or 0):
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
            # Require real match history — loose fallbacks caused bad predictions
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix = _build_matrix(mu_h, mu_a)
        probs  = _matrix_probs(matrix)

        btts = probs["btts"]

        # Blend with historical BTTS rates
        hb = getattr(home, "home_btts_rate", 0) or 0
        ab = getattr(away, "away_btts_rate", 0) or 0
        if hb > 0 and ab > 0:
            btts = btts * 0.60 + ((hb + ab) / 2) * 0.40

        # H2H blend
        if h2h_results:
            h2h_btts = sum(1 for r in h2h_results
                          if (r.get("home_score") or 0) > 0 and (r.get("away_score") or 0) > 0)
            btts = btts * 0.80 + (h2h_btts / len(h2h_results)) * 0.20

        no_btts = 1 - btts

        # Dead zone
        if abs(btts - 0.5) < 0.06:
            return _skip("dead_zone")

        if btts >= no_btts:
            model_prob, tip, odds_key = btts, "BTTS Yes", "yes"
        else:
            model_prob, tip, odds_key = no_btts, "BTTS No", "no"

        bookie_dec = (odds.get("btts", {}).get(odds_key)) if odds else None
        vc = _value_check(model_prob, bookie_dec)
        if not vc["has_value"]:
            return _skip("no_value")

        cb = _build_confidence(model_prob, bookie_dec, vc["edge"])
        conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
        conf = round(max(0, min(conf, 82)), 1)

        if conf < MIN_CONFIDENCE:
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
            # Require real match history — loose fallbacks caused bad predictions
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        mu_h, mu_a = _derive_lambdas(home, away, league)
        matrix = _build_matrix(mu_h, mu_a)
        probs  = _matrix_probs(matrix)

        hw, dr, aw = probs["home"], probs["draw"], probs["away"]

        combos = {
            "Home or Draw": hw + dr,
            "Away or Draw": aw + dr,
            "Home or Away": hw + aw,
        }
        tip, model_prob = max(combos.items(), key=lambda x: x[1])

        # DC only useful if clearly dominant — raised from 0.68 to 0.72
        if model_prob < 0.72:
            return _skip("low_confidence")

        # Try to get DC odds from bookmaker; require higher floor without them.
        dc_odds_map = {"Home or Draw": "1x", "Away or Draw": "x2", "Home or Away": "12"}
        odds_key = dc_odds_map.get(tip)
        bookie_dec = (odds.get("dc", {}).get(odds_key)) if (odds and odds_key) else None

        # Without bookie odds we can't validate edge — require high model confidence
        if bookie_dec is None and model_prob < 0.72:
            return _skip("no_value")

        vc = _value_check(model_prob, bookie_dec)
        if not vc["has_value"]:
            return _skip("no_value")

        cb = _build_confidence(model_prob, bookie_dec, vc["edge"])
        # Cap DC at 78 — it is a safety market, not a high-confidence call
        conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
        conf = round(max(0, min(conf, 78)), 1)

        if conf < MIN_CONFIDENCE:
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
        def _has_data(t):
            # Require real match history — loose fallbacks caused bad predictions
            return (getattr(t, "games_played", 0) or 0) >= MIN_GAMES
        if not (_has_data(home) and _has_data(away)):
            return _skip("insufficient_data")

        hcf = getattr(home, "home_avg_corners_for",     0) or 0
        hca = getattr(home, "home_avg_corners_against", 0) or 0
        acf = getattr(away, "away_avg_corners_for",     0) or 0
        aca = getattr(away, "away_avg_corners_against", 0) or 0

        # Skip only if BOTH teams have ALL four corner values still at exact defaults.
        # A team that had corner data fetched will have at least one value differ.
        # We check each team independently — if either has real data, proceed.
        _HOME_DEFAULTS = (5.0, 4.5)  # (hcf, hca)
        _AWAY_DEFAULTS = (4.5, 5.2)  # (acf, aca)
        home_corners_real = (round(hcf,1), round(hca,1)) != _HOME_DEFAULTS
        away_corners_real = (round(acf,1), round(aca,1)) != _AWAY_DEFAULTS

        if not home_corners_real and not away_corners_real:
            return _skip("insufficient_data")

        # If only one side has real data, use league avg for the other side
        league_avg_corners = getattr(league, "avg_corners", 10.0) or 10.0
        if not home_corners_real:
            hcf = league_avg_corners * 0.52  # home teams tend to win more corners
            hca = league_avg_corners * 0.48
        if not away_corners_real:
            acf = league_avg_corners * 0.45
            aca = league_avg_corners * 0.55

        # Require minimum games played (loose guard — corner data itself is the main gate)
        home_gp = getattr(home, "games_played", 0) or 0
        away_gp = getattr(away, "games_played", 0) or 0
        if home_gp < MIN_CORNER_DATA_PTS or away_gp < MIN_CORNER_DATA_PTS:
            return _skip("insufficient_data")

        expected = (hcf + aca) / 2 + (acf + hca) / 2
        if expected < 4.0:
            return _skip("insufficient_data")

        # Referee adjustment
        if referee and (getattr(referee, "games_officiated", 0) or 0) >= 8:
            ref_y = getattr(referee, "avg_yellows_per_game", 0) or 0
            avg_cards = getattr(league, "avg_cards", 3.5) or 3.5
            expected *= 1 + ((ref_y / avg_cards) - 1) * 0.08

        # H2H
        if h2h_results:
            hc = [r["total_corners"] for r in h2h_results if r.get("total_corners")]
            if hc:
                expected = expected * 0.80 + (sum(hc)/len(hc)) * 0.20

        best = None
        for line in CORNER_LINES:
            gap = abs(expected - line)
            if gap < 0.25 or gap > 2.5:
                continue

            side = "Over" if expected > line else "Under"
            odds_key = side.lower()

            sigma = math.sqrt(expected)
            z = (line + 0.5 - expected) / sigma if sigma > 0 else 0

            def _ncdf(z):
                t = 1/(1+0.2316419*abs(z))
                p = 1-(0.31938153*t - 0.356563782*t**2 + 1.781477937*t**3
                       - 1.821255978*t**4 + 1.330274429*t**5) * math.exp(-z**2/2) / math.sqrt(2*math.pi)
                return p if z >= 0 else 1-p

            over_prob = 1 - _ncdf(z)
            model_prob = over_prob if side == "Over" else 1 - over_prob

            bookie_dec = None
            if odds and "ou_corners" in odds:
                bookie_dec = odds["ou_corners"].get(str(line), {}).get(odds_key)

            # Enforce corners-specific minimum decimal odds.
            # Corners markets are sharp — if the bookmaker is offering below
            # MIN_CORNER_DECIMAL the punter is risking R1 for cents in return.
            if bookie_dec is not None and bookie_dec < MIN_CORNER_DECIMAL:
                continue

            vc = _value_check(model_prob, bookie_dec)
            if not vc["has_value"]:
                continue

            cb = _build_confidence(model_prob, bookie_dec, vc["edge"])
            conf = cb["confidence"] - _sample_penalty(home, away) - _lineup_penalty(home, away)
            conf = round(max(0, min(conf, 82)), 1)

            if conf < MIN_CORNER_CONFIDENCE:
                continue

            candidate = {
                "tip": f"{side} {line}", "confidence": conf, "skip_reason": "",
                "expected_value": round(expected, 2),
                "bookie_decimal": vc["bookie_decimal"], "edge": vc["edge"],
            }
            if best is None or (vc["edge"] or 0) > (best.get("edge") or 0):
                best = candidate

        return best if best else _skip("no_value")
    except Exception as exc:
        logger.error("Corners error: %s", exc)
        return _skip("insufficient_data")
