from decouple import config
from pathlib import Path
import dj_database_url

BASE_DIR = Path(__file__).resolve().parent.parent

# =========================
# CORE SETTINGS
# =========================
SECRET_KEY = config("SECRET_KEY")

DEBUG = config("DEBUG", default=False, cast=bool)

ALLOWED_HOSTS = [
    host.strip()
    for host in config("ALLOWED_HOSTS", default="localhost").split(",")
    if host.strip()
]

# =========================
# INSTALLED APPS
# =========================
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_apscheduler",
    "fixtures",
    "predictions",
    "results",
    "website",
]

# =========================
# MIDDLEWARE
# =========================
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "core.urls"

# =========================
# TEMPLATES
# =========================
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "website" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "core.wsgi.application"

# =========================
# DATABASE
# =========================
DATABASES = {
    "default": dj_database_url.config(
        default=config("DATABASE_URL", default="sqlite:///db.sqlite3"),
        conn_max_age=600,
    )
}

# =========================
# PASSWORD VALIDATION
# =========================
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# =========================
# INTERNATIONALIZATION
# =========================
LANGUAGE_CODE = "en-us"
TIME_ZONE = config("TIME_ZONE", default="Africa/Johannesburg")
USE_I18N = True
USE_TZ = True

# =========================
# STATIC FILES
# =========================
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# =========================
# APSCHEDULER
# =========================
APSCHEDULER_DATETIME_FORMAT = "N j, Y, f:s a"
APSCHEDULER_RUN_NOW_TIMEOUT = 25

# =========================
# PAYMENTS / LINKS
# =========================
PAYPAL_LINK = config("PAYPAL_LINK", default="")
YOCO_LINK = config("YOCO_LINK", default="")

# =========================
# GLOBAL RAPID API KEY
# =========================
RAPID_API_KEY = config("RAPID_API_KEY", default="")
# =========================
# FLASHSCORE API
# =========================
FLASHSCORE_API_HOST = config(
    "FLASHSCORE_API_HOST",
    default="flashscore4.p.rapidapi.com",
)
FLASHSCORE_API_BASE_URL = config(
    "FLASHSCORE_API_BASE_URL",
    default="https://flashscore4.p.rapidapi.com",
)
FLASHSCORE_API_TIMEOUT = config(
    "FLASHSCORE_API_TIMEOUT",
    default=50,
    cast=int,
)
FLASHSCORE_MATCH_DETAILS_PATH = config(
    "FLASHSCORE_MATCH_DETAILS_PATH",
    default="/api/flashscore/v2/matches/details",
)
FLASHSCORE_MATCH_LIST_PATH = config(
    "FLASHSCORE_MATCH_LIST_PATH",
    default="",
)
FLASHSCORE_MARKETS_PATH = config(
    "FLASHSCORE_MARKETS_PATH",
    default="",
)
# =========================
# SOCCER FOOTBALL INFO API
# =========================
SOCCER_INFO_API_HOST = config(
    "SOCCER_INFO_API_HOST",
    default="soccer-football-info.p.rapidapi.com",
)
SOCCER_INFO_API_BASE_URL = config(
    "SOCCER_INFO_API_BASE_URL",
    default="https://soccer-football-info.p.rapidapi.com",
)
SOCCER_INFO_API_TIMEOUT = config(
    "SOCCER_INFO_API_TIMEOUT",
    default=50,
    cast=int,
)

LOGIN_URL = '/login/'
LOGIN_REDIRECT_URL = '/dashboard/'
