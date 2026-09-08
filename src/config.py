"""Runtime configuration, read once from environment variables."""
import os


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


ENV = os.environ.get("ENV", "development").strip().lower()
IS_PRODUCTION = ENV in ("production", "prod")

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

# HS256 tokens are only as strong as this key; RFC 7518 asks for at least 32 bytes.
MIN_SECRET_KEY_LENGTH = 32

SECRET_KEY = os.environ.get("SECRET_KEY", "").strip()
if IS_PRODUCTION:
    if not SECRET_KEY:
        raise RuntimeError(
            "SECRET_KEY is not set. Refusing to start with a known signing key in "
            f"production — set it to a random string of at least {MIN_SECRET_KEY_LENGTH} characters."
        )
    if len(SECRET_KEY) < MIN_SECRET_KEY_LENGTH:
        raise RuntimeError(
            f"SECRET_KEY is only {len(SECRET_KEY)} characters; "
            f"use at least {MIN_SECRET_KEY_LENGTH}."
        )
elif not SECRET_KEY:
    SECRET_KEY = "dev-secret-not-for-production-000000"

TOKEN_TTL_DAYS = _int_env("TOKEN_TTL_DAYS", 30)

# Upload limits — a single .mtreplay is normally well under 1 MB.
MAX_FILES_PER_UPLOAD = _int_env("MAX_FILES_PER_UPLOAD", 200)
MAX_FILE_SIZE = _int_env("MAX_FILE_SIZE", 10 * 1024 * 1024)
MAX_UPLOAD_SIZE = _int_env("MAX_UPLOAD_SIZE", 150 * 1024 * 1024)
# Parsed battles are small, so this only bounds one save request.
MAX_BATTLES_PER_SAVE = _int_env("MAX_BATTLES_PER_SAVE", 500)

# Connection pool. Neon's free tier is happiest with a small pool.
DB_POOL_MIN = _int_env("DB_POOL_MIN", 1)
DB_POOL_MAX = _int_env("DB_POOL_MAX", 5)

# Failed logins per username+IP before the pair is locked out for a while.
LOGIN_MAX_ATTEMPTS = _int_env("LOGIN_MAX_ATTEMPTS", 10)
LOGIN_LOCKOUT_SECONDS = _int_env("LOGIN_LOCKOUT_SECONDS", 300)
