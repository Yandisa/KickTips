from django.db import models


class PerformanceRecord(models.Model):
    date = models.DateField(unique=True)

    # Overall
    total_published = models.IntegerField(default=0)
    total_won       = models.IntegerField(default=0)
    total_lost      = models.IntegerField(default=0)
    total_void      = models.IntegerField(default=0)
    win_rate        = models.FloatField(default=0.0)

    # By market
    won_1x2      = models.IntegerField(default=0)
    total_1x2    = models.IntegerField(default=0)
    won_dc       = models.IntegerField(default=0)
    total_dc     = models.IntegerField(default=0)
    won_goals    = models.IntegerField(default=0)
    total_goals  = models.IntegerField(default=0)
    won_corners  = models.IntegerField(default=0)
    total_corners = models.IntegerField(default=0)
    won_btts     = models.IntegerField(default=0)
    total_btts   = models.IntegerField(default=0)

    # By confidence band
    won_65_70    = models.IntegerField(default=0)
    total_65_70  = models.IntegerField(default=0)
    won_70_75    = models.IntegerField(default=0)
    total_70_75  = models.IntegerField(default=0)
    won_75_80    = models.IntegerField(default=0)
    total_75_80  = models.IntegerField(default=0)
    won_80_plus  = models.IntegerField(default=0)
    total_80_plus = models.IntegerField(default=0)

    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Record {self.date}: {self.win_rate:.1f}% ({self.total_won}/{self.total_published})"

    @classmethod
    def get_alltime(cls):
        """Aggregate all-time stats across all records."""
        from django.db.models import Sum
        agg = cls.objects.aggregate(
            tp=Sum('total_published'),
            tw=Sum('total_won'),
            tl=Sum('total_lost'),
            tv=Sum('total_void'),
            w1=Sum('won_1x2'),   t1=Sum('total_1x2'),
            w2=Sum('won_dc'),    t2=Sum('total_dc'),
            w3=Sum('won_goals'), t3=Sum('total_goals'),
            w4=Sum('won_corners'), t4=Sum('total_corners'),
            w5=Sum('won_btts'),    t5=Sum('total_btts'),
            w65=Sum('won_65_70'),   tot65=Sum('total_65_70'),
            w70=Sum('won_70_75'),   tot70=Sum('total_70_75'),
            w75=Sum('won_75_80'),   tot75=Sum('total_75_80'),
            w80=Sum('won_80_plus'), tot80=Sum('total_80_plus'),
        )
        tp = agg['tp'] or 0
        tw = agg['tw'] or 0
        return {
            'total_published': tp,
            'total_won':       tw,
            'total_lost':      agg['tl'] or 0,
            'total_void':      agg['tv'] or 0,
            'win_rate':        round(tw / tp * 100, 1) if tp else 0,
            'markets': {
                '1X2':     {'won': agg['w1'] or 0, 'total': agg['t1'] or 0},
                'Double Chance': {'won': agg['w2'] or 0, 'total': agg['t2'] or 0},
                'Over/Under Goals': {'won': agg['w3'] or 0, 'total': agg['t3'] or 0},
                'Corners': {'won': agg['w4'] or 0, 'total': agg['t4'] or 0},
                'BTTS':    {'won': agg['w5'] or 0, 'total': agg['t5'] or 0},
            },
            'bands': {
                '65-70%': {'won': agg['w65'] or 0, 'total': agg['tot65'] or 0},
                '70-75%': {'won': agg['w70'] or 0, 'total': agg['tot70'] or 0},
                '75-80%': {'won': agg['w75'] or 0, 'total': agg['tot75'] or 0},
                '80%+':   {'won': agg['w80'] or 0, 'total': agg['tot80'] or 0},
            }
        }
