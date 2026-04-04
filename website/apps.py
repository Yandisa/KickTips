import sys
import os
from django.apps import AppConfig


class WebsiteConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'website'

    def ready(self):
        skip_commands = {'migrate', 'makemigrations', 'test', 'collectstatic', 'check', 'shell'}
        if any(cmd in sys.argv for cmd in skip_commands):
            return

        run_main = os.environ.get('RUN_MAIN')
        if run_main == 'false':
            return

        try:
            from scheduler import scheduler
            scheduler.start()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Scheduler failed to start: {e}")