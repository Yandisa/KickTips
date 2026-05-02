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

MARKET_WIN_RATES = {
    'corners': 0.75,
    'dc':      0.55,
    'btts':    0.54,
    'ou_goals': 0.43,
    '1x2':     0.35,
}

# Confidence floors per acca tier
FAKA_MIN_CONF     = 66.5
SHAYA_MIN_CONF    = 65.0
ISTIMELA_MIN_CONF = 60.0

def _score_prediction(pred):
    market_wr = MARKET_WIN_RATES.get(pred.market, 0.45)
    edge      = float(pred.edge or 0)
    conf      = float(pred.confidence or 0)
    score     = market_wr * 100
    score    += edge * 50
    score    += conf * 0.1
    return score

# Variety caps — prevent any single market or league dominating an acca.
# A 10-leg acca with 7 "BTTS No" legs looks like padding and loses trust.
MAX_SAME_MARKET_PER_ACCA = 3   # raised from 2 — DC-heavy days need more slots
MAX_SAME_LEAGUE_PER_ACCA = 4   # raised from 3 — fewer fixtures per day need more flexibility


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


def _ranked_unique(predictions, min_conf=0.0, exclude_markets=None):
    """
    One tip per fixture, best-scored, filtered by min_conf.
    exclude_markets: set of (fixture_id, market) already used in higher tiers.
    Allows Shaya/Istimela to pick different markets from same fixture as Faka.
    """
    exclude_markets = exclude_markets or set()
    seen_fixtures   = set()
    ranked          = []

    for pred in sorted(predictions, key=_score_prediction, reverse=True):
        if float(pred.confidence or 0) < min_conf:
            continue
        fid    = pred.fixture_id
        market = pred.market

        if (fid, market) in exclude_markets:
            continue
        if fid in seen_fixtures:
            continue

        seen_fixtures.add(fid)
        ranked.append(pred)

    return ranked


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

    # Admin context — only computed when logged in
    admin_ctx = {}
    if request.user.is_authenticated:
        from collections import Counter
        tips_today = Prediction.objects.filter(published=True, fixture__kickoff__date=today)
        graded = tips_today.exclude(result='pending')
        won = graded.filter(result='won').count()
        lost = graded.filter(result='lost').count()
        admin_ctx = {
            'admin_tips_today':    tips_today.count(),
            'admin_won_today':     won,
            'admin_lost_today':    lost,
            'admin_wr_today':      round(won/(won+lost)*100,1) if (won+lost) else 0,
            'admin_market_counts': dict(Counter(tips_today.values_list('market', flat=True))),
        }

    return render(request, "website/home.html", {
        "top_matches": top_matches,
        "today": today,
        "alltime": alltime,
        "paypal": settings.PAYPAL_LINK,
        "yoco": settings.YOCO_LINK,
        **admin_ctx,
    })


def matches(request):
    today_str = request.GET.get("date", date.today().isoformat())
    try:
        selected_date = date.fromisoformat(today_str)
    except ValueError:
        selected_date = date.today()

    league_filter = request.GET.get("league", "")
    min_conf = request.GET.get("confidence", "0")
    tip_filter = request.GET.get("tips", "only")
    result_filter = request.GET.get("result", "")
    sort_by = request.GET.get("sort", "time")

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

    # Result filter
    if result_filter == "pending":
        fixtures = [f for f in fixtures if f.status == "scheduled" and f.has_tip]
    elif result_filter == "won":
        fixtures = [f for f in fixtures
                    if f.best_prediction and f.best_prediction.result == "won"]
    elif result_filter == "lost":
        fixtures = [f for f in fixtures
                    if f.best_prediction and f.best_prediction.result == "lost"]
    elif result_filter == "live":
        fixtures = [f for f in fixtures if f.status == "live"]

    # Sort
    if sort_by == "confidence":
        fixtures = sorted(fixtures, key=lambda f: f.best_confidence, reverse=True)
    elif sort_by == "time":
        fixtures = sorted(fixtures, key=lambda f: f.kickoff)
    # league sort keeps existing league_groups order

    leagues = League.objects.filter(active=True).order_by("tier", "name")

    # Rebuild league_groups after filtering + sorting
    from collections import OrderedDict
    league_groups = OrderedDict()

    if sort_by == "time":
        # Single flat group sorted by kickoff time
        fixtures_by_time = sorted(fixtures, key=lambda f: f.kickoff)
        league_groups["all"] = {
            "label": f"{len(fixtures_by_time)} matches · sorted by time",
            "fixtures": fixtures_by_time,
            "league": None,
        }
    else:
        for fixture in fixtures:
            key = f"{fixture.league.country} · {fixture.league.name}"
            if key not in league_groups:
                league_groups[key] = {"label": key, "fixtures": [], "league": fixture.league}
            league_groups[key]["fixtures"].append(fixture)
        if sort_by == "confidence":
            for group in league_groups.values():
                group["fixtures"].sort(key=lambda f: f.best_confidence, reverse=True)

    return render(request, "website/matches.html", {
        "fixtures":       fixtures,
        "league_groups":  list(league_groups.values()),
        "selected_date":  selected_date,
        "leagues":        leagues,
        "league_filter":  league_filter,
        "min_conf":       min_conf,
        "tip_filter":     tip_filter,
        "result_filter":  result_filter,
        "sort_by":        sort_by,
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


def _build_combo_tip(published):
    """
    Detect DC + O/U Goals combination on the same fixture.
    Returns a combo tip dict if alignment is logical, None otherwise.

    Logical alignments:
      Home or Draw + Under X  — defensive, home side expected to control
      Away or Draw + Under X  — defensive, away side expected to control
      Home or Away + Over X   — open game, both sides attacking

    Contradictory (not returned):
      Home or Draw + Over 3.5 — unlikely to hold a high-scoring game
    """
    dc_pred   = next((p for p in published if p.market == "dc"), None)
    ou_pred   = next((p for p in published if p.market == "ou_goals"), None)

    if not dc_pred or not ou_pred:
        return None

    dc_tip = dc_pred.tip
    ou_tip = ou_pred.tip

    # Check logical alignment
    is_under = "Under" in ou_tip
    is_over  = "Over" in ou_tip
    is_home_or_draw  = "Home or Draw"  in dc_tip
    is_away_or_draw  = "Away or Draw"  in dc_tip
    is_home_or_away  = "Home or Away"  in dc_tip

    aligned = (
        (is_under and (is_home_or_draw or is_away_or_draw)) or
        (is_over  and is_home_or_away)
    )

    if not aligned:
        return None

    # Combined odds
    dc_odds = dc_pred.bookie_decimal or round(1 / (float(dc_pred.confidence or 67) / 100), 2)
    ou_odds = ou_pred.bookie_decimal or round(1 / (float(ou_pred.confidence or 67) / 100), 2)
    combined_odds = round(dc_odds * ou_odds, 2)

    # Reasoning
    if is_under and is_home_or_draw:
        reasoning = f"Home side expected to control — {dc_tip} suits a tight, low-scoring game ({ou_tip})."
    elif is_under and is_away_or_draw:
        reasoning = f"Away side likely to sit deep — {dc_tip} fits a defensive, low-scoring contest ({ou_tip})."
    elif is_over and is_home_or_away:
        reasoning = f"Open attacking game expected — {dc_tip} suggests both sides going for it ({ou_tip})."
    else:
        reasoning = f"{dc_tip} + {ou_tip} — both markets point the same direction."

    return {
        "dc_tip":        dc_tip,
        "ou_tip":        ou_tip,
        "dc_odds":       dc_odds,
        "ou_odds":       ou_odds,
        "combined_odds": combined_odds,
        "reasoning":     reasoning,
        "dc_pred":       dc_pred,
        "ou_pred":       ou_pred,
    }


def match_detail(request, fixture_id):
    fixture = get_object_or_404(
        Fixture.objects.select_related("home_team", "away_team", "league", "referee"),
        pk=fixture_id,
    )
    published = list(Prediction.objects.filter(fixture=fixture, published=True).order_by("publish_rank"))
    skipped   = Prediction.objects.filter(fixture=fixture, published=False).order_by("market", "-confidence")

    # Team history from DB (recent finished matches)
    home_history = _get_team_history(fixture.home_team, is_home_role=True)
    away_history = _get_team_history(fixture.away_team, is_home_role=False)

    # Form strings from Team model
    home_form_chars = list(fixture.home_team.form_home[:5]) if fixture.home_team.form_home else []
    away_form_chars = list(fixture.away_team.form_away[:5]) if fixture.away_team.form_away else []

    # Combo tip — DC + O/U Goals alignment
    combo_tip = _build_combo_tip(published)

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
        "fixture":         fixture,
        "published":       published,
        "skipped":         skipped,
        "combo_tip":       combo_tip,
        "home_history":    home_history,
        "away_history":    away_history,
        "home_form_chars": home_form_chars,
        "away_form_chars": away_form_chars,
        "h2h_results":     h2h_results,
        "h2h_home_wins":   h2h_home_wins,
        "h2h_away_wins":   h2h_away_wins,
        "h2h_draws":       h2h_draws,
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

def winners(request):
    return render(request, 'website/winners.html')

def review(request):
    return render(request, 'website/review.html')


from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.http import JsonResponse


@login_required
def dashboard(request):
    """Admin dashboard — only accessible after login."""
    from datetime import date as dt
    from collections import Counter

    today = dt.today()
    tips_today = Prediction.objects.filter(
        published=True, fixture__kickoff__date=today
    )
    graded_today = tips_today.exclude(result='pending')
    won_today = graded_today.filter(result='won').count()
    lost_today = graded_today.filter(result='lost').count()
    pending_today = tips_today.filter(result='pending').count()
    wr_today = round(won_today / (won_today + lost_today) * 100, 1) if (won_today + lost_today) else 0

    market_counts = Counter(tips_today.values_list('market', flat=True))

    alltime = PerformanceRecord.get_alltime()

    return render(request, 'website/dashboard.html', {
        'today': today,
        'tips_today': tips_today.count(),
        'won_today': won_today,
        'lost_today': lost_today,
        'pending_today': pending_today,
        'wr_today': wr_today,
        'market_counts': dict(market_counts),
        'alltime': alltime,
    })


@login_required
def admin_grade(request):
    """
    Trigger grade_results in a background thread.
    Returns immediately so the worker doesn't time out.
    """
    import threading
    import io
    import logging
    from django.core.management import call_command

    log = logging.getLogger(__name__)

    def _run():
        try:
            out = io.StringIO()
            call_command('grade_results', stdout=out)
            log.info("grade_results completed: %s", out.getvalue()[:500])
        except Exception as exc:
            log.error("grade_results failed: %s", exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return JsonResponse({
        'status': 'started',
        'output': 'Grading started in background. Check results in a few minutes.'
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

    # Build tiers sequentially, tracking which fixture+market pairs are used
    # so each tier picks a different market from fixtures with multiple predictions.
    # Arsenal with DC + corners + BTTS: Faka gets DC, Shaya gets corners, Istimela gets BTTS.
    used_markets: set = set()

    faka_legs   = _ranked_unique(all_preds, min_conf=FAKA_MIN_CONF, exclude_markets=used_markets)
    faka_yonke  = _build_acca(faka_legs, size_min=4, size_max=5)
    if faka_yonke:
        for leg in faka_yonke['legs']:
            used_markets.add((leg.fixture_id, leg.market))

    shaya_legs  = _ranked_unique(all_preds, min_conf=SHAYA_MIN_CONF, exclude_markets=used_markets)
    shaya_zonke = _build_acca(shaya_legs, size_min=5, size_max=8)
    if shaya_zonke:
        for leg in shaya_zonke['legs']:
            used_markets.add((leg.fixture_id, leg.market))

    istim_legs  = _ranked_unique(all_preds, min_conf=ISTIMELA_MIN_CONF, exclude_markets=used_markets)
    istimela    = _build_acca(istim_legs, size_min=6, size_max=12)

    total_available = len(istim_legs)

    return render(request, "website/accumulators.html", {
        "today":           today,
        "total_available": total_available,
        "faka_yonke":      faka_yonke,
        "shaya_zonke":     shaya_zonke,
        "istimela":        istimela,
    })
