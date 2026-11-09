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

# User ids (comma-separated) that see the whole database and the admin page. Ids, not
# usernames: a name listed before its owner registers could be claimed by anyone.
ADMIN_USER_IDS = frozenset(
    int(part) for part in os.environ.get("ADMIN_USER_IDS", "").replace(" ", "").split(",") if part.isdigit()
)

# Teams: how many a user may own, and invite links.
MAX_TEAMS_PER_USER = _int_env("MAX_TEAMS_PER_USER", 5)
MAX_TEAM_MEMBERS = _int_env("MAX_TEAM_MEMBERS", 50)
INVITE_MAX_DAYS = _int_env("INVITE_MAX_DAYS", 30)

# A parsed battle can be saved this long after upload (see src/tickets.py).
BATTLE_TICKET_TTL_HOURS = _int_env("BATTLE_TICKET_TTL_HOURS", 24)

# How many reverse proxies stand in front of the app (the hosting's load balancer is one).
# The client's address is then read from X-Forwarded-For, counting that many hops from the
# right; entries further left are whatever the client chose to send and are ignored.
# 0 trusts nothing and uses the TCP peer. Behind a proxy, 0 makes every visitor look like
# the proxy: per-IP limits would then lump everyone together.
TRUSTED_PROXY_HOPS = _int_env("TRUSTED_PROXY_HOPS", 0)

# Per-IP and per-user rate limits (in-memory, per instance).
REGISTRATIONS_PER_HOUR = _int_env("REGISTRATIONS_PER_HOUR", 5)
LOGIN_FAILURES_PER_IP = _int_env("LOGIN_FAILURES_PER_IP", 50)  # any usernames, per lockout window
UPLOAD_FILES_PER_HOUR = _int_env("UPLOAD_FILES_PER_HOUR", 1000)

# Version of the personal-data consent text (static/consent.html). Bump it when the text
# changes: everyone is then asked to accept the new one on their next visit.
CONSENT_VERSION = os.environ.get("CONSENT_VERSION", "2026-11-09").strip() or "2026-11-09"
