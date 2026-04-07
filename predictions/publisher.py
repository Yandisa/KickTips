"""
Publisher
=========
Takes scored predictions for a fixture and saves the final picks selected
by the prediction command.

Design:
- run_predictions.py decides which fixtures/picks qualify
- publisher.py should not apply a conflicting global threshold
- allow multiple published markets per fixture
- save non-published markets for transparency
"""

import logging
import random
from predictions.models import Prediction

logger = logging.getLogger(__name__)

MAX_PER_FIXTURE = 3
CONFIDENCE_THRESHOLD = 65.0  # Safety net — matches MIN_CONFIDENCE in engine

MARKET_ROTATION = ["btts", "ou_goals", "dc", "corners", "1x2"]

CONFIDENCE_BAND = 3.0


def publish_predictions(fixture, scored_predictions):
    candidates = []
    skipped = []

    for market, result in scored_predictions.items():
        result = result or {}
        skip_reason = (result.get("skip_reason") or "").strip()
        confidence = float(result.get("confidence", 0) or 0)
        tip = (result.get("tip") or "").strip()

        if skip_reason:
            skipped.append((market, result, skip_reason))
            continue

        if not tip:
            skipped.append((market, result, "no_tip"))
            continue

        if confidence >= CONFIDENCE_THRESHOLD:
            result["tip"] = tip
            candidates.append((market, result))
        else:
            skipped.append((market, result, "low_confidence"))

    if candidates:
        max_conf = max(float(r.get("confidence", 0) or 0) for _, r in candidates)

        top_band = [
            (m, r) for m, r in candidates
            if max_conf - float(r.get("confidence", 0) or 0) <= CONFIDENCE_BAND
        ]
        lower = [
            (m, r) for m, r in candidates
            if max_conf - float(r.get("confidence", 0) or 0) > CONFIDENCE_BAND
        ]

        top_band.sort(key=lambda item: (
            MARKET_ROTATION.index(item[0]) if item[0] in MARKET_ROTATION else 99
        ))

        lower.sort(key=lambda item: -float(item[1].get("confidence", 0) or 0))

        candidates = top_band + lower

    published_markets = set()
    published_count = 0

    for rank, (market, result) in enumerate(candidates[:MAX_PER_FIXTURE], start=1):
        Prediction.objects.update_or_create(
            fixture=fixture,
            market=market,
            defaults={
                "tip":            result.get("tip", ""),
                "expected_value": result.get("expected_value", 0),
                "confidence":     result.get("confidence", 0),
                "reasoning":      result.get("reasoning", ""),
                "bookie_decimal": result.get("bookie_decimal"),
                "edge":           result.get("edge"),
                "published":      True,
                "publish_rank":   rank,
                "skipped_reason": "",
                "result":         "pending",
            },
        )
        published_markets.add(market)
        published_count += 1

        logger.info(
            "Published [%s] %s | %s | %s (%.1f%%)",
            rank,
            fixture,
            market,
            result.get("tip", ""),
            float(result.get("confidence", 0) or 0),
        )

    for market, result in candidates[MAX_PER_FIXTURE:]:
        Prediction.objects.update_or_create(
            fixture=fixture,
            market=market,
            defaults={
                "tip":            result.get("tip", ""),
                "expected_value": result.get("expected_value", 0),
                "confidence":     result.get("confidence", 0),
                "reasoning":      result.get("reasoning", ""),
                "bookie_decimal": result.get("bookie_decimal"),
                "edge":           result.get("edge"),
                "published":      False,
                "publish_rank":   None,
                "skipped_reason": "ranked_out",
                "result":         "pending",
            },
        )

    for market, result, reason in skipped:
        if market in published_markets:
            continue

        Prediction.objects.update_or_create(
            fixture=fixture,
            market=market,
            defaults={
                "tip": result.get("tip", ""),
                "expected_value": result.get("expected_value", 0),
                "confidence": result.get("confidence", 0),
                "reasoning": result.get("reasoning", ""),
                "published": False,
                "publish_rank": None,
                "skipped_reason": reason,
                "result": "pending",
            },
        )

    logger.info(
        "%s: %s published, %s skipped, %s ranked out",
        fixture,
        published_count,
        len(skipped),
        max(0, len(candidates) - MAX_PER_FIXTURE),
    )

    return published_count
