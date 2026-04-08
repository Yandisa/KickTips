"""
Scheduler — KickTips
====================
Schedule (all times SAST / Africa/Johannesburg):

  20:00 (evening, day before):
    fetch_fixtures --tomorrow  → fetch next day's fixtures + team stats + enrichment
    run_predictions --date <tomorrow>  → score and publish tips for tomorrow

  Grade pipeline (5 runs across match day):
    06:00 → pick up any early overnight scores, grade finished matches
    10:00 → morning matches (Asia/Australia) settling
    14:00 → midday matches settling
    18:00 → afternoon matches settling
    23:00 → final run — almost everything finished, catch stragglers

  Each grade run:
    fetch_fixtures          → refresh scores/status for today's fixtures
    grade_results           → grade any newly-finished, ungraded predictions

API budget per day:
  Evening pipeline:  ~166 calls  (1 list + 60 team stats + 90 enrichment + 15 h2h)
  Grade pipeline:    ~60 calls   (~12/run × 5 runs)
  Total:             ~226 calls/day → ~6,780/month
  PRO plan budget:   20,000/month   → 34% utilisation, 13,220 calls headroom
"""
import logging
from datetime import date, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from django_apscheduler.jobstores import DjangoJobStore
from django.core.management import call_command

logger = logging.getLogger(__name__)


def evening_pipeline():
    """Fetch history + tomorrow's fixtures + publish predictions + snapshot accas."""
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    logger.info("=== Scheduler: Evening pipeline starting (target: %s) ===", tomorrow)

    try:
        call_command("fetch_history")
        logger.info("fetch_history complete")
    except Exception as exc:
        logger.error("fetch_history failed: %s", exc)

    try:
        call_command("fetch_fixtures", tomorrow=True)
        logger.info("fetch_fixtures complete")
    except Exception as exc:
        logger.error("fetch_fixtures --tomorrow failed: %s", exc)
        return

    try:
        call_command("run_predictions", date=tomorrow)
        logger.info("run_predictions complete for %s", tomorrow)
    except Exception as exc:
        logger.error("run_predictions failed: %s", exc)
        return  # no point snapshotting accas if predictions failed

    # Snapshot accumulators immediately after predictions are published
    # so they're stored as a fixed record — not rebuilt dynamically each view
    try:
        call_command("save_accumulators", date=tomorrow)
        logger.info("save_accumulators complete for %s", tomorrow)
    except Exception as exc:
        logger.error("save_accumulators failed: %s", exc)


def closing_odds_pipeline():
    """Capture closing bookmaker lines ~1.5hr before main European kickoffs."""
    logger.info("=== Scheduler: Closing odds refresh ===")
    try:
        call_command("refresh_odds")
        logger.info("refresh_odds complete")
    except Exception as exc:
        logger.error("refresh_odds failed: %s", exc)


def grade_pipeline():
    """Refresh today's scores then grade any newly-finished predictions."""
    logger.info("=== Scheduler: Grade pipeline starting ===")
    try:
        call_command("fetch_fixtures")   # today — picks up latest scores/status
    except Exception as exc:
        logger.error("fetch_fixtures (grade) failed: %s", exc)
    try:
        call_command("grade_results")
    except Exception as exc:
        logger.error("grade_results failed: %s", exc)


def start():
    scheduler = BackgroundScheduler(timezone="Africa/Johannesburg")
    scheduler.add_jobstore(DjangoJobStore(), "default")

    # ── Evening: fetch tomorrow's fixtures + publish tips + snapshot accas ──
    scheduler.add_job(
        evening_pipeline, "cron", hour=20, minute=0,
        id="evening_pipeline", replace_existing=True,
    )

    # ── Closing odds: capture bookmaker lines before kickoff ─────────────
    # Two runs cover both afternoon and evening European matches (SAST):
    #   13:30 → European 14:00+ kickoffs (15:00 CET / 13:00 UTC)
    #   18:30 → European 19:00+ kickoffs (20:00 CET / 18:00 UTC)
    scheduler.add_job(
        closing_odds_pipeline, "cron", hour=13, minute=30,
        id="closing_odds_afternoon", replace_existing=True,
    )
    scheduler.add_job(
        closing_odds_pipeline, "cron", hour=18, minute=30,
        id="closing_odds_evening", replace_existing=True,
    )

    # ── Grade pipeline: 5 runs across the match day ──────────────────────
    for run_id, (h, m) in enumerate([
        (6,  0),   # 06:00 — overnight/Asian markets
        (10, 0),   # 10:00 — morning matches
        (14, 0),   # 14:00 — midday matches
        (18, 0),   # 18:00 — afternoon matches
        (23, 0),   # 23:00 — final sweep
    ], start=1):
        scheduler.add_job(
            grade_pipeline, "cron", hour=h, minute=m,
            id=f"grade_pipeline_{run_id}", replace_existing=True,
        )

    scheduler.start()
    logger.info(
        "Scheduler started — evening: 20:00 | closing odds: 13:30, 18:30 | "
        "grade: 06:00, 10:00, 14:00, 18:00, 23:00 SAST"
    )
