from django.contrib import admin
from .models import PerformanceRecord

@admin.register(PerformanceRecord)
class PerformanceRecordAdmin(admin.ModelAdmin):
    list_display = ['date', 'total_published', 'total_won', 'total_lost', 'win_rate']
    ordering     = ['-date']
    readonly_fields = [f.name for f in PerformanceRecord._meta.fields]
