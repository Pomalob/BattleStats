"""scripts/export_to_table.py without a file: it logs in to the site and downloads /api/export."""
import gzip
import importlib.util
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from replay_factory import build_replay

ROOT = Path(__file__).parent.parent


def _script():
    spec = importlib.util.spec_from_file_location("export_to_table", ROOT / "scripts" / "export_to_table.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _route_to(client, seen):
    """Stands in for the network: the script's requests go to the app through the test client."""
    def http(url, data=None, headers=None):
        parts = urlsplit(url)
        path = parts.path + (f"?{parts.query}" if parts.query else "")
        seen.append((path, dict(headers or {})))
        res = client.post(path, content=data, headers=headers) if data is not None else client.get(path, headers=headers)
        body = res.content
        out_headers = dict(res.headers)
        # httpx has already unpacked gzip; pack it again so the script's own unpacking is exercised
        if "gzip" in (headers or {}).get("Accept-Encoding", ""):
            body, out_headers["content-encoding"] = gzip.compress(body), "gzip"
        return res.status_code, out_headers, body
    return http


def _saved_battle(client, auth, username="ivan"):
    headers = auth(username)
    files = {"files": ("a.mtreplay", build_replay(), "application/octet-stream")}
    battle = client.post("/api/upload", files=files, headers=headers).json()["battles"][0]
    client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)


def test_downloads_the_export_and_saves_it_under_a_fixed_name(client, auth, fake_db, tmp_path, monkeypatch):
    _saved_battle(client, auth)
    script, seen = _script(), []
    monkeypatch.setattr(script, "_http", _route_to(client, seen))
    monkeypatch.setenv("BATTLESTATS_URL", "https://example.test")
    monkeypatch.setenv("BATTLESTATS_USER", "ivan")
    monkeypatch.setenv("BATTLESTATS_PASSWORD", "secret123")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["export_to_table.py", "--date-from", "2025-01-01", "--bi"])
    script.main()

    saved = json.loads((tmp_path / "battlestats.json").read_text(encoding="utf-8"))
    assert saved["total_battles"] == 1 and saved["user"] == "ivan"
    assert (tmp_path / "battlestats_bi_players.csv").exists()
    export_path, export_headers = seen[-1]
    assert export_path == "/api/export?mine=true&date_from=2025-01-01"
    assert export_headers["Authorization"].startswith("Bearer ")


def test_a_wrong_password_stops_with_the_sites_message(client, auth, fake_db, tmp_path, monkeypatch):
    auth("ivan")
    script = _script()
    monkeypatch.setattr(script, "_http", _route_to(client, []))
    monkeypatch.setenv("BATTLESTATS_USER", "ivan")
    monkeypatch.setenv("BATTLESTATS_PASSWORD", "wrong")
    with pytest.raises(SystemExit, match="Неверное имя пользователя или пароль"):
        script.load_from_site(True, None, None)
