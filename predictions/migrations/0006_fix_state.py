"""
State-only migration — database_operations is empty because bookie_decimal,
edge, closing_decimal and clv already exist in the DB (added by migration
0004 via RunSQL). This operation purely updates Django's migration state so
that makemigrations stops detecting them as missing and generating new files
on every deploy.

The auto-generated 0006 that Coolify kept creating is replaced by this one.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('predictions', '0005_accumulator'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            # DB: do nothing — all four columns already exist
            database_operations=[],
            # State: inform Django's migration framework they exist
            state_operations=[
                migrations.AddField(
                    model_name='prediction',
                    name='bookie_decimal',
                    field=models.FloatField(null=True, blank=True),
                ),
                migrations.AddField(
                    model_name='prediction',
                    name='edge',
                    field=models.FloatField(null=True, blank=True),
                ),
                migrations.AddField(
                    model_name='prediction',
                    name='closing_decimal',
                    field=models.FloatField(null=True, blank=True),
                ),
                migrations.AddField(
                    model_name='prediction',
                    name='clv',
                    field=models.FloatField(null=True, blank=True),
                ),
            ],
        ),
    ]
