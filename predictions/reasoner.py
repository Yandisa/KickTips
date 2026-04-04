"""
Reasoner
========
Generates a plain-English explanation for each published tip.

Rules:
- Always reference the actual tip line (e.g. "Over 2.5", "Under 10.5")
  not just the raw model expected value. A reader who sees "expected 9.6
  corners — Verdict: Under 12.5" is rightly confused.
- Never expose raw model internals (expected totals, probabilities) as
  the primary information. Use them only to frame the argument.
- Write like a punter talking to a punter. Short sentences. Plain language.
- Each function receives `tip` as a string like "Over 2.5" or "Home Win"
  and must use it consistently throughout the text.
"""

import re


# ── Helpers ───────────────────────────────────────────────────────────────────

def _g(obj, attr, default=0.0):
    """Safe getattr that also treats None as the default."""
    return getattr(obj, attr, default) or default


def _parse_line(tip):
    """Extract the numeric line from a tip string like 'Over 2.5'."""
    m = re.search(r"(\d+\.?\d*)", tip)
    return float(m.group(1)) if m else None


def _ref_has_enough_data(referee):
    if hasattr(referee, "has_enough_data"):
        return bool(referee.has_enough_data)
    return (_g(referee, "games_officiated") or 0) >= 8


def _h2h_summary(h2h_results):
    """Return a one-line H2H goals summary, or None if no data."""
    goals = [
        (r.get("home_score") or 0) + (r.get("away_score") or 0)
        for r in h2h_results
        if r.get("home_score") is not None and r.get("away_score") is not None
    ]
    if not goals:
        return None
    avg = sum(goals) / len(goals)
    return f"Their last {len(goals)} meetings averaged {avg:.1f} goals."


# ── Public entry point ────────────────────────────────────────────────────────

def generate_reasoning(
    market,
    tip,
    expected_value,
    home,
    away,
    referee=None,
    h2h=None,
    league=None,
    **kwargs,
):
    h2h_results = h2h or []

    if market == "corners":
        return _corners_reasoning(tip, expected_value, home, away, referee, h2h_results)
    if market == "ou_goals":
        return _goals_reasoning(tip, expected_value, home, away, h2h_results, league)
    if market == "1x2":
        return _1x2_reasoning(tip, expected_value, home, away, h2h_results)
    if market == "btts":
        return _btts_reasoning(tip, expected_value, home, away, h2h_results)
    if market == "dc":
        return _dc_reasoning(tip, expected_value, home, away, h2h_results)
    return ""


# ── Corners ───────────────────────────────────────────────────────────────────

def _corners_reasoning(tip, expected, home, away, referee, h2h_results):
    line = _parse_line(tip)
    side = "Over" if tip.startswith("Over") else "Under"

    home_for = _g(home, "home_avg_corners_for")
    home_agt = _g(home, "home_avg_corners_against")
    away_for = _g(away, "away_avg_corners_for")
    away_agt = _g(away, "away_avg_corners_against")

    lines = [
        f"{home.name} win around {home_for:.1f} corners per home game and give away {home_agt:.1f}.",
        f"{away.name} earn around {away_for:.1f} away and concede {away_agt:.1f}.",
    ]

    if line is not None:
        margin = abs(expected - line)
        direction = "above" if side == "Over" else "below"
        lines.append(
            f"That puts the projected total around {expected:.0f} corners — "
            f"{margin:.0f} {direction} the {line} line, which supports {tip}."
        )
    else:
        lines.append(f"Our model projects approximately {expected:.0f} total corners.")

    if referee and _ref_has_enough_data(referee):
        avg_y = _g(referee, "avg_yellows_per_game")
        if avg_y > 0:
            tone = "a strict referee" if avg_y >= 4.5 else "a lenient referee"
            lines.append(
                f"{referee.name} is {tone} ({avg_y:.1f} cards/game), "
                f"which factors slightly into this projection."
            )

    h2h_c = [r["total_corners"] for r in h2h_results if r.get("total_corners") is not None]
    if h2h_c:
        avg_c = sum(h2h_c) / len(h2h_c)
        lines.append(
            f"The last {len(h2h_c)} meetings between these sides averaged {avg_c:.0f} corners."
        )

    return " ".join(lines)


# ── Goals Over/Under ──────────────────────────────────────────────────────────

def _goals_reasoning(tip, expected, home, away, h2h_results, league=None):
    line = _parse_line(tip)
    side = "Over" if tip.startswith("Over") else "Under"

    home_gf = _g(home, "home_avg_goals_for")
    home_ga = _g(home, "home_avg_goals_against")
    away_gf = _g(away, "away_avg_goals_for")
    away_ga = _g(away, "away_avg_goals_against")

    lines = [
        f"{home.name} score {home_gf:.1f} and concede {home_ga:.1f} per home game.",
        f"{away.name} score {away_gf:.1f} and concede {away_ga:.1f} away.",
    ]

    if line is not None:
        margin = expected - line
        if side == "Over":
            lines.append(
                f"Combined, we project around {expected:.1f} goals — "
                f"{margin:.1f} above the {line} line."
            )
        else:
            lines.append(
                f"Combined, we project around {expected:.1f} goals — "
                f"{abs(margin):.1f} below the {line} line."
            )
    else:
        lines.append(f"Combined expected goals: {expected:.1f}.")

    if league and _g(league, "avg_goals"):
        league_avg = _g(league, "avg_goals")
        tone = "a higher-scoring game than the league average" if expected > league_avg else "a tighter game than the league average"
        lines.append(f"This league averages {league_avg:.1f} goals per match, pointing to {tone}.")

    h2h_text = _h2h_summary(h2h_results)
    if h2h_text:
        lines.append(h2h_text)

    return " ".join(lines)


# ── 1X2 ───────────────────────────────────────────────────────────────────────

def _1x2_reasoning(tip, expected, home, away, h2h_results):
    home_wr = _g(home, "home_win_rate")
    away_wr = _g(away, "away_win_rate")

    lines = [
        f"{home.name} win {home_wr * 100:.0f}% of their home games.",
        f"{away.name} win {away_wr * 100:.0f}% of their away games.",
    ]

    if tip == "Home Win":
        lines.append(f"The home advantage and form make {home.name} the clear pick.")
    elif tip == "Away Win":
        lines.append(f"{away.name}'s away form makes them the stronger side on current numbers.")
    else:
        lines.append("Neither side holds a clear edge — a draw is the most likely outcome.")

    if h2h_results:
        h2h_hw = sum(1 for r in h2h_results if r.get("winner") == "home")
        h2h_aw = sum(1 for r in h2h_results if r.get("winner") == "away")
        h2h_d  = sum(1 for r in h2h_results if r.get("winner") == "draw")
        n = len(h2h_results)
        lines.append(
            f"Last {n} meetings: {h2h_hw} home win{'s' if h2h_hw != 1 else ''}, "
            f"{h2h_aw} away, {h2h_d} draw{'s' if h2h_d != 1 else ''}."
        )

    return " ".join(lines)


# ── Double Chance ─────────────────────────────────────────────────────────────

def _dc_reasoning(tip, expected, home, away, h2h_results=None):
    home_wr = _g(home, "home_win_rate")
    away_wr = _g(away, "away_win_rate")

    lines = [
        f"{home.name} home win rate: {home_wr * 100:.0f}%. "
        f"{away.name} away win rate: {away_wr * 100:.0f}%.",
    ]

    if tip == "Home or Draw":
        lines.append(
            f"{home.name} have the stronger home record. "
            f"The Double Chance covers them whether they win or draw."
        )
    elif tip == "Away or Draw":
        lines.append(
            f"{away.name} are capable of a result on the road. "
            f"The Double Chance covers a win or a share of the spoils."
        )
    else:
        lines.append(
            f"Both sides have strong winning records — a draw looks unlikely, "
            f"so this covers either winner."
        )

    if h2h_results:
        non_draws = sum(1 for r in h2h_results if r.get("winner") in {"home", "away"})
        if non_draws >= 2:
            lines.append(
                f"Recent H2H has produced a decisive result {non_draws} of {len(h2h_results)} times."
            )

    return " ".join(lines)


# ── BTTS ──────────────────────────────────────────────────────────────────────

def _btts_reasoning(tip, expected, home, away, h2h_results):
    is_yes    = "Yes" in tip
    home_btts = _g(home, "home_btts_rate")
    away_btts = _g(away, "away_btts_rate")

    if is_yes:
        if home_btts > 0 and away_btts > 0:
            lines = [
                f"Both teams score in {home_btts * 100:.0f}% of {home.name}'s home games.",
                f"{away.name} score in {away_btts * 100:.0f}% of their away games — they don't park the bus.",
                f"Both sides look likely to find the net.",
            ]
        else:
            home_gf = _g(home, "home_avg_goals_for")
            away_gf = _g(away, "away_avg_goals_for")
            lines = [
                f"{home.name} average {home_gf:.1f} goals at home, {away.name} average {away_gf:.1f} away.",
                f"Both sides carry enough of an attacking threat for goals at both ends.",
            ]
    else:
        if home_btts > 0 and away_btts > 0:
            lines = [
                f"BTTS only lands in {home_btts * 100:.0f}% of {home.name}'s home games "
                f"and {away_btts * 100:.0f}% of {away.name}'s away games.",
                f"At least one side looks likely to keep a clean sheet.",
            ]
        else:
            home_ga = _g(home, "home_avg_goals_against")
            away_ga = _g(away, "away_avg_goals_against")
            lines = [
                f"{home.name} concede {home_ga:.1f} per home game, "
                f"{away.name} concede {away_ga:.1f} away.",
                f"One side should manage a clean sheet here.",
            ]

    if h2h_results:
        btts_hits = sum(
            1 for r in h2h_results
            if r.get("home_score") is not None and r.get("away_score") is not None
            and r["home_score"] > 0 and r["away_score"] > 0
        )
        n = len(h2h_results)
        lines.append(f"Both scored in {btts_hits} of their last {n} meetings.")

    return " ".join(lines)