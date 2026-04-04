from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('results', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='performancerecord',
            name='won_btts',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='performancerecord',
            name='total_btts',
            field=models.IntegerField(default=0),
        ),
    ]
