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


@pytest.mark.parametrize("host", [
    "localhost", "127.0.0.1", "db",
    "db-team-cmw34wok8009epa07kpn9l52i", "10.0.0.5", "192.168.1.20", "172.16.3.4",
])
def test_local_and_internal_databases_connect_without_tls(host):
    """A docker-compose Postgres or one on a platform's private network serves no TLS."""
    assert build_dsn(f"postgresql://user:pw@{host}:5432/battlestats") == (
        f"postgresql://user:pw@{host}:5432/battlestats?sslmode=disable"
    )


def test_tls_options_are_stripped_from_an_internal_url():
    dsn = build_dsn(
        "postgresql://user:pw@db-team-x:5432/db"
        "?sslmode=require&ssl=true&channel_binding=require&connect_timeout=5"
    )
    assert dsn == "postgresql://user:pw@db-team-x:5432/db?connect_timeout=5&sslmode=disable"


def test_a_public_ip_still_requires_tls():
    assert "sslmode=require" in build_dsn("postgresql://user:pw@85.193.80.10:5432/db")


def test_a_missing_url_is_an_explicit_error():
    with pytest.raises(RuntimeError, match="DATABASE_URL not set"):
        build_dsn("")
