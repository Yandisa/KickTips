"""
Migration 0003 — Team deep enrichment fields
=============================================
Adds fields populated by the new enrichment endpoints:
  - Form (last 6 results as string)
  - League position & points
  - O/U rates per standard line
  - HT/FT patterns
  - Lineup key-player tracking
  - Recency-weighted goal/corner averages
  - Value edge cache (populated by engine after odds fetch)
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("fixtures", "0002_team_deep_stats"),
    ]

    operations = [
        migrations.AddField("Team", "form_home",        models.CharField(max_length=20, blank=True, default="")),
        migrations.AddField("Team", "form_away",        models.CharField(max_length=20, blank=True, default="")),
        migrations.AddField("Team", "form_overall",     models.CharField(max_length=20, blank=True, default="")),

        migrations.AddField("Team", "league_position",  models.IntegerField(null=True, blank=True)),
        migrations.AddField("Team", "league_points",    models.IntegerField(null=True, blank=True)),
        migrations.AddField("Team", "league_gf",        models.IntegerField(null=True, blank=True)),
        migrations.AddField("Team", "league_ga",        models.IntegerField(null=True, blank=True)),

        # Over/Under rates — home and away, for lines 1.5 / 2.5 / 3.5
        migrations.AddField("Team", "home_ou15_over_rate",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "home_ou25_over_rate",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "home_ou35_over_rate",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "away_ou15_over_rate",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "away_ou25_over_rate",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "away_ou35_over_rate",  models.FloatField(default=0.0)),

        # HT/FT patterns (fraction of games in each outcome pair)
        migrations.AddField("Team", "htft_ww_rate",  models.FloatField(default=0.0)),  # winning at HT & FT
        migrations.AddField("Team", "htft_wd_rate",  models.FloatField(default=0.0)),  # winning at HT, draw FT
        migrations.AddField("Team", "htft_wl_rate",  models.FloatField(default=0.0)),  # winning at HT, lose FT
        migrations.AddField("Team", "htft_dw_rate",  models.FloatField(default=0.0)),  # drawing at HT, win FT
        migrations.AddField("Team", "htft_ll_rate",  models.FloatField(default=0.0)),  # losing both halves

        # Recency-weighted (exponential decay, last 10 games)
        migrations.AddField("Team", "rw_home_goals_for",      models.FloatField(default=0.0)),
        migrations.AddField("Team", "rw_home_goals_against",  models.FloatField(default=0.0)),
        migrations.AddField("Team", "rw_away_goals_for",      models.FloatField(default=0.0)),
        migrations.AddField("Team", "rw_away_goals_against",  models.FloatField(default=0.0)),

        # Lineup availability (updated morning of match)
        migrations.AddField("Team", "key_players_missing", models.IntegerField(default=0)),
        migrations.AddField("Team", "lineup_checked_at",   models.DateTimeField(null=True, blank=True)),

        migrations.AddField("Team", "enriched_at", models.DateTimeField(null=True, blank=True)),
    ]