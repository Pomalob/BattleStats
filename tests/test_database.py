import pytest

from database import build_dsn


def test_heroku_style_prefix_is_normalised():
    dsn = build_dsn("postgres://user:pw@ep-cool.neon.tech/db")
    assert dsn.startswith("postgresql://")


def test_tls_is_required_for_a_managed_host():
    assert "sslmode=require" in build_dsn("postgresql://user:pw@ep-cool.neon.tech/db")


def test_tls_is_appended_to_an_existing_query_string():
    dsn = build_dsn("postgresql://user:pw@ep-cool.neon.tech/db?connect_timeout=5")
    assert dsn.endswith("&sslmode=require")


def test_an_explicit_sslmode_is_left_alone():
    url = "postgresql://user:pw@ep-cool.neon.tech/db?sslmode=verify-full"
    assert build_dsn(url) == url


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "db"])
def test_local_databases_are_not_forced_onto_tls(host):
    """A docker-compose Postgres has no certificate, so requiring TLS would break it."""
    assert "sslmode" not in build_dsn(f"postgresql://user:pw@{host}:5432/battlestats")


def test_a_missing_url_is_an_explicit_error():
    with pytest.raises(RuntimeError, match="DATABASE_URL not set"):
        build_dsn("")
