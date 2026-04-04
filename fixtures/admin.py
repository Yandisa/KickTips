from django.contrib import admin
from .models import League, Team, Fixture, Referee

@admin.register(League)
class LeagueAdmin(admin.ModelAdmin):
    list_display = ['name', 'country', 'tier', 'active', 'season']
    list_filter  = ['tier', 'active', 'country']
    list_editable = ['active', 'tier']

@admin.register(Referee)
class RefereeAdmin(admin.ModelAdmin):
    list_display = ['name', 'avg_yellows_per_game', 'avg_reds_per_game', 'games_officiated']
    ordering     = ['-games_officiated']

@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    list_display = ['name', 'league', 'games_played', 'home_win_rate', 'away_win_rate', 'last_updated']
    list_filter  = ['league']
    search_fields = ['name']

@admin.register(Fixture)
class FixtureAdmin(admin.ModelAdmin):
    list_display  = ['__str__', 'league', 'kickoff', 'status', 'graded']
    list_filter   = ['status', 'graded', 'league']
    search_fields = ['home_team__name', 'away_team__name']
    ordering      = ['-kickoff']
