from django.db import models


class League(models.Model):
    TIER_CHOICES = [(1, 'Deep'), (2, 'Basic'), (3, 'Skip')]

    api_id       = models.IntegerField(unique=True)
    name         = models.CharField(max_length=100)
    country      = models.CharField(max_length=100)
    tier         = models.IntegerField(choices=TIER_CHOICES, default=1)
    active       = models.BooleanField(default=True)
    season       = models.IntegerField(default=2025)
    avg_goals    = models.FloatField(default=2.5)
    avg_corners  = models.FloatField(default=10.0)
    avg_cards    = models.FloatField(default=3.5)
    last_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.country})"

    class Meta:
        ordering = ['tier', 'name']


class Referee(models.Model):
    name                 = models.CharField(max_length=100, unique=True)
    avg_yellows_per_game = models.FloatField(default=3.5)
    avg_reds_per_game    = models.FloatField(default=0.1)
    avg_booking_points   = models.FloatField(default=38.0)
    games_officiated     = models.IntegerField(default=0)
    last_updated         = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    @property
    def has_enough_data(self):
        return self.games_officiated >= 8


class Team(models.Model):
    api_id       = models.IntegerField(unique=True)
    name         = models.CharField(max_length=100)
    league       = models.ForeignKey(League, on_delete=models.SET_NULL, null=True)
    games_played = models.IntegerField(default=0)

    # ── Goals (season averages) ───────────────────────────────────────────
    home_avg_goals_for       = models.FloatField(default=1.5)
    home_avg_goals_against   = models.FloatField(default=1.2)
    away_avg_goals_for       = models.FloatField(default=1.2)
    away_avg_goals_against   = models.FloatField(default=1.5)

    # ── Corners ───────────────────────────────────────────────────────────
    home_avg_corners_for     = models.FloatField(default=5.0)
    home_avg_corners_against = models.FloatField(default=4.5)
    away_avg_corners_for     = models.FloatField(default=4.5)
    away_avg_corners_against = models.FloatField(default=5.2)

    # ── Cards ─────────────────────────────────────────────────────────────
    home_avg_cards           = models.FloatField(default=1.8)
    away_avg_cards           = models.FloatField(default=2.0)

    # ── Win / draw rates ──────────────────────────────────────────────────
    home_win_rate            = models.FloatField(default=0.40)
    home_draw_rate           = models.FloatField(default=0.28)
    away_win_rate            = models.FloatField(default=0.28)
    away_draw_rate           = models.FloatField(default=0.30)

    # ── xG + BTTS ─────────────────────────────────────────────────────────
    home_xg_for              = models.FloatField(default=0.0)
    home_xg_against          = models.FloatField(default=0.0)
    away_xg_for              = models.FloatField(default=0.0)
    away_xg_against          = models.FloatField(default=0.0)
    home_btts_rate           = models.FloatField(default=0.0)
    away_btts_rate           = models.FloatField(default=0.0)

    scraper_source           = models.CharField(max_length=30, blank=True, default='')
    scraper_updated_at       = models.DateTimeField(null=True, blank=True)
    last_updated             = models.DateTimeField(auto_now=True)

    # ── Added by migration 0003 — deep enrichment ─────────────────────────

    # Recent form strings (e.g. "WWDLW")
    form_home                = models.CharField(max_length=20, blank=True, default='')
    form_away                = models.CharField(max_length=20, blank=True, default='')
    form_overall             = models.CharField(max_length=20, blank=True, default='')

    # League table position
    league_position          = models.IntegerField(null=True, blank=True)
    league_points            = models.IntegerField(null=True, blank=True)
    league_gf                = models.IntegerField(null=True, blank=True)
    league_ga                = models.IntegerField(null=True, blank=True)

    # Over/Under rates from standings endpoint (per x.5 line)
    home_ou15_over_rate      = models.FloatField(default=0.0)
    home_ou25_over_rate      = models.FloatField(default=0.0)
    home_ou35_over_rate      = models.FloatField(default=0.0)
    away_ou15_over_rate      = models.FloatField(default=0.0)
    away_ou25_over_rate      = models.FloatField(default=0.0)
    away_ou35_over_rate      = models.FloatField(default=0.0)

    # HT/FT pattern rates
    htft_ww_rate             = models.FloatField(default=0.0)
    htft_wd_rate             = models.FloatField(default=0.0)
    htft_wl_rate             = models.FloatField(default=0.0)
    htft_dw_rate             = models.FloatField(default=0.0)
    htft_ll_rate             = models.FloatField(default=0.0)

    # Recency-weighted goal averages (exponential decay on last 20 results)
    rw_home_goals_for        = models.FloatField(default=0.0)
    rw_home_goals_against    = models.FloatField(default=0.0)
    rw_away_goals_for        = models.FloatField(default=0.0)
    rw_away_goals_against    = models.FloatField(default=0.0)

    # Lineup availability (updated morning of match day)
    key_players_missing      = models.IntegerField(default=0)
    lineup_checked_at        = models.DateTimeField(null=True, blank=True)

    # Timestamp of last deep enrichment run
    enriched_at              = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.name

    @property
    def has_enough_data(self):
        return self.games_played >= 5

    @property
    def has_xg_data(self):
        return self.home_xg_for > 0 or self.away_xg_for > 0

    @property
    def has_btts_data(self):
        return self.home_btts_rate > 0 or self.away_btts_rate > 0

    @property
    def has_form_data(self):
        return bool(self.form_overall)


class Fixture(models.Model):
    STATUS_CHOICES = [
        ('scheduled', 'Scheduled'), ('live', 'Live'),
        ('finished',  'Finished'),  ('postponed', 'Postponed'),
        ('cancelled', 'Cancelled'),
    ]

    api_id    = models.IntegerField(unique=True)
    league    = models.ForeignKey(League, on_delete=models.CASCADE)
    home_team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name='home_fixtures')
    away_team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name='away_fixtures')
    kickoff   = models.DateTimeField()
    referee   = models.ForeignKey(Referee, on_delete=models.SET_NULL, null=True, blank=True)
    venue     = models.CharField(max_length=150, blank=True)
    status    = models.CharField(max_length=20, choices=STATUS_CHOICES, default='scheduled')

    home_score    = models.IntegerField(null=True, blank=True)
    away_score    = models.IntegerField(null=True, blank=True)
    total_corners = models.IntegerField(null=True, blank=True)
    total_cards   = models.IntegerField(null=True, blank=True)
    graded        = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.home_team} vs {self.away_team} — {self.kickoff.date()}"

    @property
    def result(self):
        if self.home_score is None:
            return None
        if self.home_score > self.away_score:
            return 'home'
        elif self.away_score > self.home_score:
            return 'away'
        return 'draw'

    @property
    def total_goals(self):
        if self.home_score is None:
            return None
        return self.home_score + self.away_score

    class Meta:
        ordering = ['kickoff']