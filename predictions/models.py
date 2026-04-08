from django.db import models
from fixtures.models import Fixture


class Prediction(models.Model):
    MARKET_CHOICES = [
        ('1x2',      '1X2 Match Result'),
        ('dc',       'Double Chance'),
        ('ou_goals', 'Over/Under Goals'),
        ('btts',     'Both Teams to Score'),   # NEW
        ('corners',  'Corners Over/Under'),
    ]

    RESULT_CHOICES = [
        ('pending', 'Pending'),
        ('won',     'Won'),
        ('lost',    'Lost'),
        ('void',    'Void'),
    ]

    SKIP_REASONS = [
        ('low_confidence',    'Below confidence threshold'),
        ('dead_zone',         'Expected value too close to the line'),
        ('small_sample',      'Not enough team data'),
        ('insufficient_data', 'Insufficient data'),
        ('no_referee_data',   'Referee data unavailable'),
        ('contradicting',     'Stats and H2H contradict each other'),
        ('ranked_out',        'Ranked out by stronger market'),
    ]

    fixture        = models.ForeignKey(Fixture, on_delete=models.CASCADE, related_name='predictions')
    market         = models.CharField(max_length=20, choices=MARKET_CHOICES)
    tip            = models.CharField(max_length=100)
    expected_value = models.FloatField()
    confidence     = models.FloatField()
    reasoning      = models.TextField()

    # Bookmaker odds stored at prediction time — used for acca combined odds
    # and for displaying real value to the punter.
    bookie_decimal = models.FloatField(null=True, blank=True)
    edge           = models.FloatField(null=True, blank=True)   # our_prob - bookie_implied

    published      = models.BooleanField(default=False)
    publish_rank   = models.IntegerField(null=True, blank=True)
    skipped_reason = models.CharField(max_length=50, choices=SKIP_REASONS, blank=True)

    result     = models.CharField(max_length=10, choices=RESULT_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)

    # ── Closing Line Value tracking ───────────────────────────────────────────
    # closing_decimal: the bookmaker price captured ~1hr before kickoff.
    # clv: our published price vs closing price.
    #   clv > 0 means we got better odds than closing → positive edge signal.
    #   clv < 0 means market moved away from us → negative edge signal.
    # Formula: clv = (bookie_decimal / closing_decimal) - 1
    closing_decimal = models.FloatField(null=True, blank=True)
    clv             = models.FloatField(null=True, blank=True)

    def __str__(self):
        status = "✅" if self.published else "⏭"
        return f"{status} {self.fixture} | {self.get_market_display()} | {self.tip} ({self.confidence:.0f}%)"

    class Meta:
        ordering = ['-confidence']
        unique_together = ['fixture', 'market']


class Accumulator(models.Model):
    """
    A daily accumulator snapshot — one record per tier per day.
    Legs are stored as AccumulatorLeg records so we can grade each one
    independently and derive the overall acca result honestly.
    """
    TIER_CHOICES = [
        ('faka_yonke',  'Faka Yonke'),
        ('shaya_zonke', 'Shaya Zonke'),
        ('istimela',    'Istimela'),
    ]
    RESULT_CHOICES = [
        ('pending', 'Pending'),
        ('won',     'Won'),
        ('lost',    'Lost'),
        ('void',    'Void'),
    ]

    date          = models.DateField()
    tier          = models.CharField(max_length=20, choices=TIER_CHOICES)
    combined_odds = models.FloatField(null=True, blank=True)
    legs_count    = models.IntegerField(default=0)
    result        = models.CharField(max_length=10, choices=RESULT_CHOICES, default='pending')
    created_at    = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['date', 'tier']
        ordering = ['date', 'tier']

    def __str__(self):
        return f"{self.get_tier_display()} {self.date} ({self.legs_count} legs @ {self.combined_odds}x) → {self.result}"

    def grade(self):
        """
        Grade this accumulator from its legs.
        Won only if every leg won. Lost if any leg lost. Void if all remaining void.
        """
        legs = list(self.legs.select_related('prediction').all())
        if not legs:
            return

        results = [leg.prediction.result for leg in legs]

        if 'pending' in results:
            return  # not all settled yet

        if any(r == 'lost' for r in results):
            self.result = 'lost'
        elif all(r == 'won' for r in results):
            self.result = 'won'
        else:
            self.result = 'void'

        self.save(update_fields=['result'])


class AccumulatorLeg(models.Model):
    """One tip inside an accumulator."""
    accumulator = models.ForeignKey(Accumulator, on_delete=models.CASCADE, related_name='legs')
    prediction  = models.ForeignKey(Prediction,  on_delete=models.CASCADE, related_name='acca_legs')
    leg_odds    = models.FloatField(null=True, blank=True)

    class Meta:
        unique_together = ['accumulator', 'prediction']
        ordering = ['id']

    def __str__(self):
        return f"{self.accumulator.get_tier_display()} | {self.prediction.fixture} | {self.prediction.tip}"
