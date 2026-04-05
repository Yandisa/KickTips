import sys
import os
from django.apps import AppConfig


class WebsiteConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'website'

    def ready(self):
        # Only start the scheduler when running as a web server (gunicorn/runserver).
        # Skip for ALL management commands (migrate, collectstatic, shell, etc.)
        # to avoid the DB-during-init warning and double-scheduling.

        is_management_command = (
            len(sys.argv) > 1 and sys.argv[0].endswith("manage.py")
        )
        if is_management_command:
            return

        # Gunicorn spawns a master + workers. We only want ONE scheduler
        # running — in the master process. Gunicorn sets no RUN_MAIN, but
        # Django dev server sets RUN_MAIN=true on the reloader child.
        # Guard: skip the reloader parent process (RUN_MAIN not set means
        # we're either gunicorn master or dev server parent — both fine).
        run_main = os.environ.get('RUN_MAIN')
        if run_main == 'false':
            return

        try:
            from scheduler import scheduler
            scheduler.start()
            import logging
            logging.getLogger(__name__).info("Scheduler started successfully")
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Scheduler failed to start: {e}")
