from django.contrib import admin
from .models import Prediction

@admin.register(Prediction)
class PredictionAdmin(admin.ModelAdmin):
    list_display  = ['fixture', 'market', 'tip', 'confidence', 'published', 'result', 'created_at']
    list_filter   = ['market', 'published', 'result']
    search_fields = ['fixture__home_team__name', 'fixture__away_team__name']
    ordering      = ['-created_at']
    readonly_fields = ['reasoning']
