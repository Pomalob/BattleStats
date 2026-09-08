import importlib

import pytest

import config


@pytest.fixture(autouse=True)
def restore_config():
    """config is read at import time, so put the module back the way the suite found it."""
    yield
    importlib.reload(config)


def test_production_refuses_to_start_without_a_secret_key(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    with pytest.raises(RuntimeError, match="SECRET_KEY is not set"):
        importlib.reload(config)


def test_production_refuses_a_short_secret_key(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "too-short")
    with pytest.raises(RuntimeError, match="at least 32"):
        importlib.reload(config)


def test_production_accepts_a_long_secret_key(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "k" * 32)
    importlib.reload(config)
    assert config.IS_PRODUCTION and config.SECRET_KEY == "k" * 32


def test_development_falls_back_to_a_placeholder_key(monkeypatch):
    monkeypatch.setenv("ENV", "development")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    importlib.reload(config)
    assert "not-for-production" in config.SECRET_KEY


def test_numeric_settings_ignore_junk_values(monkeypatch):
    monkeypatch.setenv("MAX_FILE_SIZE", "not-a-number")
    importlib.reload(config)
    assert config.MAX_FILE_SIZE == 10 * 1024 * 1024
