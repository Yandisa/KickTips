from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('fixtures', '0001_initial'),
    ]

    operations = [
        migrations.AddField(model_name='team', name='home_xg_for',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='home_xg_against',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='away_xg_for',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='away_xg_against',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='home_btts_rate',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='away_btts_rate',
            field=models.FloatField(default=0.0)),
        migrations.AddField(model_name='team', name='scraper_source',
            field=models.CharField(blank=True, default='', max_length=30)),
        migrations.AddField(model_name='team', name='scraper_updated_at',
            field=models.DateTimeField(null=True, blank=True)),
    ]
