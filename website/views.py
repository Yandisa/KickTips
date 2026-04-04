import math
import random
from datetime import date

from django.conf import settings
from django.db.models import Prefetch
from django.shortcuts import get_object_or_404, render

from fixtures.models import Fixture, League
from predictions.models import Prediction
from results.models import PerformanceRecord


# ── Accumulator helpers ───────────────────────────────────────────────────────

MARKET_PREFERENCE = {'1x2': 1, 'btts': 2, 'ou_goals': 3, 'dc': 4, 'corners': 5}

# Confidence floors per acca tier — raised from previous values.
# Faka Yonke is the flagship product — only the very best tips go in.
FAKA_MIN_CONF     = 70.0   # was 63 — tightest, only genuinely strong tips
SHAYA_MIN_CONF    = 65.0   # was 57 — solid mid-tier
ISTIMELA_MIN_CONF = 60.0   # was 50 — minimum publishable confidence

# Variety caps — prevent any single market or league dominating an acca.
# A 10-leg acca with 7 "BTTS No" legs looks like padding and loses trust.
MAX_SAME_MARKET_PER_ACCA = 2   # max legs of the same market type
MAX_SAME_LEAGUE_PER_ACCA = 3   # max legs from the same league


def _score_prediction(pred):
    conf  = float(pred.confidence or 0)
    mrank = MARKET_PREFERENCE.get(pred.market, 9)
    return conf - (mrank * 0.5)


def _leg_decimal(pred):
    """
    Return the best available decimal odds for a leg.
    Uses real bookmaker decimal when available, falls back to fair decimal
    derived from confidence.  Fair decimal = 1 / (confidence / 100).
    """
    bd = getattr(pred, "bookie_decimal", None)
    if bd and float(bd) >= 1.10:
        return float(bd)
    conf = float(pred.confidence or 50)
    return round(1.0 / max(conf / 100, 0.01), 2)


def _ranked_unique(predictions, min_conf=0.0):
    """
    One prediction per real-world match (home+away team pair),
    filtered to min_conf, sorted best-scored first.
    """
    by_match = {}
    for pred in predictions:
        if float(pred.confidence or 0) < min_conf:
            continue
        key = (pred.fixture.home_team_id, pred.fixture.away_team_id)
        if key not in by_match or _score_prediction(pred) > _score_prediction(by_match[key]):
            by_match[key] = pred
    return sorted(by_match.values(), key=_score_prediction, reverse=True)


def _build_acca(legs, size_min, size_max):
    """
    Build an accumulator from a ranked pool of legs, enforcing:
      - Max MAX_SAME_MARKET_PER_ACCA legs of the same market type
      - Max MAX_SAME_LEAGUE_PER_ACCA legs from the same league
      - Between size_min and size_max total legs
      - Combined odds use real bookmaker decimals where available

    Iterates the full ranked pool and selects qualifying legs in order,
    so variety is enforced without sacrificing overall confidence.
    """
    selected = []
    market_counts = {}
    league_counts = {}

    for pred in legs:
        if len(selected) >= size_max:
            break

        market = pred.market
        league_id = pred.fixture.league_id

        # Enforce variety caps
        if market_counts.get(market, 0) >= MAX_SAME_MARKET_PER_ACCA:
            continue
        if league_counts.get(league_id, 0) >= MAX_SAME_LEAGUE_PER_ACCA:
            continue

        selected.append(pred)
        market_counts[market] = market_counts.get(market, 0) + 1
        league_counts[league_id] = league_counts.get(league_id, 0) + 1

    if len(selected) < size_min:
        return None

    combined = round(
        math.prod(_leg_decimal(p) for p in selected), 2
    )
    avg_conf = round(
        sum(float(p.confidence or 0) for p in selected) / len(selected), 1
    )
    return {
        'legs':           selected,
        'count':          len(selected),
        'combined_odds':  combined,
        'avg_confidence': avg_conf,
    }


# ── Views ─────────────────────────────────────────────────────────────────────

def home(request):
    today = date.today()

    published_today = Prediction.objects.filter(
        published=True,
        result="pending",
        fixture__kickoff__date=today,
    ).select_related(
        "fixture",
        "fixture__home_team",
        "fixture__away_team",
        "fixture__league",
    ).order_by("publish_rank")

    top_matches = (
        Fixture.objects.filter(
            kickoff__date=today,
            predictions__published=True,
            predictions__result="pending",
        )
        .select_related("home_team", "away_team", "league", "referee")
        .prefetch_related(
            Prefetch(
                "predictions",
                queryset=published_today,
                to_attr="published_predictions",
            )
        )
        .distinct()
        .order_by("kickoff", "league__name")[:6]
    )

    for fixture in top_matches:
        preds = list(getattr(fixture, "published_predictions", []))
        rng = random.Random(fixture.pk)
        rng.shuffle(preds)
        fixture.published_predictions = preds
        fixture.prediction_count = len(preds)
        fixture.best_prediction = preds[0] if preds else None
        fixture.best_confidence = max(
            [float(p.confidence or 0) for p in preds], default=0,
        )

    alltime = PerformanceRecord.get_alltime()

    return render(request, "website/home.html", {
        "top_matches": top_matches,
        "today": today,
        "alltime": alltime,
        "paypal": settings.PAYPAL_LINK,
        "yoco": settings.YOCO_LINK,
    })


def matches(request):
    today_str = request.GET.get("date", date.today().isoformat())
    try:
        selected_date = date.fromisoformat(today_str)
    except ValueError:
        selected_date = date.today()

    league_filter = request.GET.get("league", "")
    min_conf = request.GET.get("confidence", "0")
    tip_filter = request.GET.get("tips", "only")  # default: show only matches with tips

    try:
        min_conf = float(min_conf)
    except ValueError:
        min_conf = 0

    published_for_page = Prediction.objects.filter(
        published=True,
        confidence__gte=min_conf,
    ).order_by("publish_rank")

    skipped_for_page = Prediction.objects.filter(
        published=False
    ).order_by("market", "-confidence")

    fixtures_qs = (
        Fixture.objects.filter(
            kickoff__date=selected_date,
            league__active=True,
        )
        .select_related("home_team", "away_team", "league", "referee")
        .prefetch_related(
            Prefetch("predictions", queryset=published_for_page, to_attr="published_predictions"),
            Prefetch("predictions", queryset=skipped_for_page,  to_attr="skipped_predictions"),
        )
        .order_by("league__tier", "league__name", "kickoff")
    )

    if league_filter:
        fixtures_qs = fixtures_qs.filter(league__api_id=league_filter)

    raw_fixtures = list(fixtures_qs)

    # Deduplicate: same home+away team pair may appear twice if fetch_fixtures
    # ran in the morning (status=scheduled) and again later (status=finished).
    # Keep the most complete version: finished > live > scheduled > other.
    STATUS_RANK = {"finished": 4, "live": 3, "scheduled": 2, "postponed": 1, "cancelled": 0}
    seen = {}
    for fixture in raw_fixtures:
        key = (fixture.home_team_id, fixture.away_team_id)
        if key not in seen:
            seen[key] = fixture
        else:
            existing_rank = STATUS_RANK.get(seen[key].status, 0)
            this_rank     = STATUS_RANK.get(fixture.status, 0)
            if this_rank > existing_rank:
                seen[key] = fixture
    fixtures = list(seen.values())
    # Re-sort after dedup (dict ordering is insertion order, not league order)
    fixtures.sort(key=lambda f: (
        f.league.tier,
        f.league.name,
        f.kickoff,
    ))

    for fixture in fixtures:
        preds = list(getattr(fixture, "published_predictions", []))
        rng = random.Random(fixture.pk)
        rng.shuffle(preds)
        fixture.published_predictions = preds
        fixture.prediction_count = len(preds)
        fixture.best_prediction = preds[0] if preds else None
        fixture.best_confidence = max([float(p.confidence or 0) for p in preds], default=0)
        fixture.has_tip = fixture.prediction_count > 0

    if tip_filter == "only":
        fixtures = [f for f in fixtures if f.has_tip]

    leagues = League.objects.filter(active=True).order_by("tier", "name")

    # Pre-group fixtures by league for the template.
    # Using regroup in the template splits on object identity, not league name —
    # two League DB records with the same name appear as two groups.
    # Pre-grouping here collapses them correctly.
    from collections import OrderedDict
    league_groups = OrderedDict()
    for fixture in fixtures:
        key = f"{fixture.league.country} · {fixture.league.name}"
        if key not in league_groups:
            league_groups[key] = {"label": key, "fixtures": [], "league": fixture.league}
        league_groups[key]["fixtures"].append(fixture)

    return render(request, "website/matches.html", {
        "fixtures":      fixtures,
        "league_groups": list(league_groups.values()),
        "selected_date": selected_date,
        "leagues":       leagues,
        "league_filter": league_filter,
        "min_conf":      min_conf,
        "tip_filter":    tip_filter,
    })


def _get_team_history(team, is_home_role, limit=5):
    """
    Fetch recent results for a team from stored fixtures in the DB,
    enriched from the API if needed. Returns list of dicts for template.
    """
    from fixtures.models import Fixture as FixtureModel
    from django.db.models import Q

    qs = FixtureModel.objects.filter(
        Q(home_team=team) | Q(away_team=team),
        status="finished",
        home_score__isnull=False,
    ).select_related("home_team", "away_team").order_by("-kickoff")[:limit]

    results = []
    for f in qs:
        is_home = f.home_team_id == team.pk
        hs, as_ = f.home_score, f.away_score
        gf = hs if is_home else as_
        ga = as_ if is_home else hs
        won  = gf > ga
        draw = gf == ga
        results.append({
            "home_name":  f.home_team.name,
            "away_name":  f.away_team.name,
            "home_score": hs,
            "away_score": as_,
            "is_home":    is_home,
            "won":        won,
            "draw":       draw,
        })
    return results


def match_detail(request, fixture_id):
    fixture = get_object_or_404(
        Fixture.objects.select_related("home_team", "away_team", "league", "referee"),
        pk=fixture_id,
    )
    published = Prediction.objects.filter(fixture=fixture, published=True).order_by("publish_rank")
    skipped   = Prediction.objects.filter(fixture=fixture, published=False).order_by("market", "-confidence")

    # Team history from DB (recent finished matches)
    home_history = _get_team_history(fixture.home_team, is_home_role=True)
    away_history = _get_team_history(fixture.away_team, is_home_role=False)

    # Form strings from Team model
    home_form_chars = list(fixture.home_team.form_home[:5]) if fixture.home_team.form_home else []
    away_form_chars = list(fixture.away_team.form_away[:5]) if fixture.away_team.form_away else []

    # H2H from DB — find fixtures where these two teams met
    from django.db.models import Q as Q2
    h2h_qs = Fixture.objects.filter(
        Q2(home_team=fixture.home_team, away_team=fixture.away_team) |
        Q2(home_team=fixture.away_team, away_team=fixture.home_team),
        status="finished",
        home_score__isnull=False,
    ).exclude(pk=fixture.pk).order_by("-kickoff")[:8]

    h2h_results = []
    h2h_home_wins = h2h_away_wins = h2h_draws = 0
    for f in h2h_qs:
        hs, as_ = f.home_score, f.away_score
        if hs > as_:
            winner = "home"
            if f.home_team_id == fixture.home_team_id:
                h2h_home_wins += 1
            else:
                h2h_away_wins += 1
        elif as_ > hs:
            winner = "away"
            if f.away_team_id == fixture.away_team_id:
                h2h_away_wins += 1
            else:
                h2h_home_wins += 1
        else:
            winner = "draw"
            h2h_draws += 1
        h2h_results.append({"home_score": hs, "away_score": as_, "winner": winner})

    return render(request, "website/match_detail.html", {
        "fixture":        fixture,
        "published":      published,
        "skipped":        skipped,
        "home_history":   home_history,
        "away_history":   away_history,
        "home_form_chars": home_form_chars,
        "away_form_chars": away_form_chars,
        "h2h_results":    h2h_results,
        "h2h_home_wins":  h2h_home_wins,
        "h2h_away_wins":  h2h_away_wins,
        "h2h_draws":      h2h_draws,
    })


def record(request):
    alltime = PerformanceRecord.get_alltime()
    history = PerformanceRecord.objects.order_by("-date")[:30]
    return render(request, "website/record.html", {"alltime": alltime, "history": history})


def donate(request):
    alltime = PerformanceRecord.get_alltime()
    recent_wins = (
        Prediction.objects.filter(published=True, result="won")
        .select_related("fixture__home_team", "fixture__away_team")
        .order_by("-fixture__kickoff")[:10]
    )
    return render(request, "website/donate.html", {
        "alltime":     alltime,
        "recent_wins": recent_wins,
        "paypal":      settings.PAYPAL_LINK,
        "yoco":        settings.YOCO_LINK,
    })


def accumulators(request):
    """
    Build today's three accumulator slates from published predictions.

    Faka Yonke  — 4-5 legs, tightest selection, maximum trust
    Shaya Zonke — 6-10 legs, solid mid-tier (our name for the middle)
    Istimela    — 10-15 legs, the train, maximum legs

    Selection:
      - Only today's published, pending tips
      - One tip per fixture (best-scored by confidence + market preference)
      - Sorted highest confidence first
      - Each acca type slices its range from the same ranked pool
    """
    today = date.today()

    all_preds = (
        Prediction.objects.filter(
            published=True,
            result="pending",
            fixture__kickoff__date=today,
        )
        .select_related(
            "fixture",
            "fixture__home_team",
            "fixture__away_team",
            "fixture__league",
        )
        .order_by("-confidence")
    )

    # Each tier has its own confidence floor and independent ranked pool.
    # _build_acca enforces market and league variety within each pool —
    # so Shaya Zonke won't have 5 BTTS legs from the same league.
    faka_legs  = _ranked_unique(all_preds, min_conf=FAKA_MIN_CONF)
    shaya_legs = _ranked_unique(all_preds, min_conf=SHAYA_MIN_CONF)
    istim_legs = _ranked_unique(all_preds, min_conf=ISTIMELA_MIN_CONF)

    total_available = len(istim_legs)

    # Size ranges: Faka tight (4-5), Shaya medium (5-8), Istimela long (8-12).
    # Reduced from previous maximums — a focused 8-leg acca beats a padded 15-leg one.
    faka_yonke  = _build_acca(faka_legs,  size_min=4, size_max=5)
    shaya_zonke = _build_acca(shaya_legs, size_min=5, size_max=8)
    istimela    = _build_acca(istim_legs, size_min=8, size_max=12)

    return render(request, "website/accumulators.html", {
        "today":           today,
        "total_available": total_available,
        "faka_yonke":      faka_yonke,
        "shaya_zonke":     shaya_zonke,
        "istimela":        istimela,
    })