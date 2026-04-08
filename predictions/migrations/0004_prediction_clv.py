"""
Safe migration — uses IF NOT EXISTS for every column so it doesn't
matter whether the server auto-generated its own 0004 or not.
Covers: bookie_decimal, edge (may already exist), closing_decimal, clv (new).
"""
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('predictions', '0003_alter_prediction_skipped_reason'),
    ]

    operations = [
        # Raw SQL with IF NOT EXISTS — idempotent regardless of what columns
        # already exist from any previous auto-generated migration.
        migrations.RunSQL(
            sql="""
                ALTER TABLE predictions_prediction
                    ADD COLUMN IF NOT EXISTS bookie_decimal double precision NULL;
                ALTER TABLE predictions_prediction
                    ADD COLUMN IF NOT EXISTS edge double precision NULL;
                ALTER TABLE predictions_prediction
                    ADD COLUMN IF NOT EXISTS closing_decimal double precision NULL;
                ALTER TABLE predictions_prediction
                    ADD COLUMN IF NOT EXISTS clv double precision NULL;
            """,
            reverse_sql="""
                ALTER TABLE predictions_prediction
                    DROP COLUMN IF EXISTS closing_decimal;
                ALTER TABLE predictions_prediction
                    DROP COLUMN IF EXISTS clv;
            """,
        ),
    ]
