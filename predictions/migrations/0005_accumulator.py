from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('predictions', '0004_prediction_clv'),
    ]

    operations = [
        migrations.CreateModel(
            name='Accumulator',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField()),
                ('tier', models.CharField(
                    choices=[
                        ('faka_yonke', 'Faka Yonke'),
                        ('shaya_zonke', 'Shaya Zonke'),
                        ('istimela', 'Istimela'),
                    ],
                    max_length=20,
                )),
                ('combined_odds', models.FloatField(blank=True, null=True)),
                ('legs_count', models.IntegerField(default=0)),
                ('result', models.CharField(
                    choices=[
                        ('pending', 'Pending'),
                        ('won', 'Won'),
                        ('lost', 'Lost'),
                        ('void', 'Void'),
                    ],
                    default='pending',
                    max_length=10,
                )),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'ordering': ['date', 'tier'],
                'unique_together': {('date', 'tier')},
            },
        ),
        migrations.CreateModel(
            name='AccumulatorLeg',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('leg_odds', models.FloatField(blank=True, null=True)),
                ('accumulator', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='legs',
                    to='predictions.accumulator',
                )),
                ('prediction', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='acca_legs',
                    to='predictions.prediction',
                )),
            ],
            options={
                'ordering': ['id'],
                'unique_together': {('accumulator', 'prediction')},
            },
        ),
    ]
