from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('predictions', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='prediction',
            name='market',
            field=models.CharField(max_length=20, choices=[
                ('1x2', '1X2 Match Result'),
                ('dc', 'Double Chance'),
                ('ou_goals', 'Over/Under Goals'),
                ('btts', 'Both Teams to Score'),
                ('corners', 'Corners Over/Under'),
            ]),
        ),
    ]
