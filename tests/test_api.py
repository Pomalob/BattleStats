import pytest

import config
from replay_factory import build_replay


def _upload(client, headers, name="battle.mtreplay", data=None):
    files = [("files", (name, data if data is not None else build_replay(), "application/octet-stream"))]
    return client.post("/api/upload", files=files, headers=headers)


# --- authentication is required everywhere that touches data ---

@pytest.mark.parametrize("method,path", [
    ("post", "/api/upload"),
    ("post", "/api/battles/save"),
    ("get", "/api/battles"),
    ("delete", "/api/battles/1"),
    ("get", "/api/analytics/summary"),
    ("get", "/api/players/hidden"),
    ("post", "/api/players/hidden"),
    ("delete", "/api/players/hidden/Alice"),
    ("get", "/api/auth/me"),
])
def test_endpoints_reject_anonymous_requests(client, method, path):
    assert getattr(client, method)(path).status_code == 401


def test_a_forged_token_is_rejected(client):
    import jwt
    forged = jwt.encode({"sub": "1", "username": "admin"}, "wrong-key", algorithm="HS256")
    res = client.get("/api/auth/me", headers={"Authorization": f"Bearer {forged}"})
    assert res.status_code == 401


# --- registration and login ---

def test_register_then_login(client):
    assert client.post("/api/auth/register", json={"username": "ivan", "password": "secret123"}).status_code == 200
    res = client.post("/api/auth/login", json={"username": "ivan", "password": "secret123"})
    assert res.status_code == 200 and res.json()["username"] == "ivan"


def test_duplicate_username_is_rejected(client):
    client.post("/api/auth/register", json={"username": "ivan", "password": "secret123"})
    res = client.post("/api/auth/register", json={"username": "ivan", "password": "other123"})
    assert res.status_code == 409


@pytest.mark.parametrize("username,password", [("ab", "secret123"), ("x" * 33, "secret123"), ("ivan", "short")])
def test_registration_validates_its_input(client, username, password):
    assert client.post("/api/auth/register", json={"username": username, "password": password}).status_code == 400


def test_login_is_throttled_after_repeated_failures(client):
    client.post("/api/auth/register", json={"username": "ivan", "password": "secret123"})
    bad = {"username": "ivan", "password": "wrong"}
    for _ in range(config.LOGIN_MAX_ATTEMPTS):
        assert client.post("/api/auth/login", json=bad).status_code == 401
    assert client.post("/api/auth/login", json=bad).status_code == 429
    # The lockout must hold even once the right password shows up.
    assert client.post("/api/auth/login", json={"username": "ivan", "password": "secret123"}).status_code == 429


# --- upload ---

def test_upload_parses_a_replay(client, auth):
    res = _upload(client, auth())
    assert res.status_code == 200
    body = res.json()
    assert body["errors"] == []
    assert body["battles"][0]["map_name"] == "Прохоровка"


def test_upload_rejects_other_file_types(client, auth):
    res = _upload(client, auth(), name="screenshot.png", data=b"\x89PNG")
    assert res.json()["battles"] == []
    assert "не файл .mtreplay" in res.json()["errors"][0]["error"].lower()


def test_upload_reports_a_broken_replay_without_failing_the_batch(client, auth):
    headers = auth()
    files = [
        ("files", ("good.mtreplay", build_replay(), "application/octet-stream")),
        ("files", ("bad.mtreplay", b"garbage", "application/octet-stream")),
    ]
    body = client.post("/api/upload", files=files, headers=headers).json()
    assert len(body["battles"]) == 1
    assert body["errors"][0]["file"] == "bad.mtreplay"


def test_upload_refuses_oversized_files(client, auth, monkeypatch):
    monkeypatch.setattr(config, "MAX_FILE_SIZE", 1024)
    body = _upload(client, auth(), data=b"\x12\x32\x34\x11" + b"\x00" * 5000).json()
    assert body["battles"] == []
    assert "больше" in body["errors"][0]["error"]


def test_upload_refuses_too_many_files(client, auth, monkeypatch):
    monkeypatch.setattr(config, "MAX_FILES_PER_UPLOAD", 2)
    files = [("files", (f"b{i}.mtreplay", build_replay(), "application/octet-stream")) for i in range(3)]
    assert client.post("/api/upload", files=files, headers=auth()).status_code == 413


def test_a_traversal_filename_is_reduced_to_its_basename(client, auth):
    """Nothing is written to disk, and the stored name must not carry a path."""
    res = _upload(client, auth(), name="../../../etc/passwd.mtreplay")
    assert res.json()["battles"][0]["filename"] == "passwd.mtreplay"


# --- saving ---

def test_save_then_list_battles(client, auth, fake_db):
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]

    res = client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    assert res.json() == {"saved": 1, "skipped": 0}

    # The same battle uploaded twice is deduplicated by its hash.
    res = client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    assert res.json() == {"saved": 0, "skipped": 1}

    listed = client.get("/api/battles", headers=headers).json()["battles"]
    assert len(listed) == 1 and listed[0]["mine"] is True


@pytest.mark.parametrize("payload", [
    {"battles": "not-a-list"},
    {"battles": [{"players": []}]},                                 # no battle_hash
    {"battles": [{"battle_hash": "h", "players": {}}]},             # players is not a list
    {"battles": [{"battle_hash": "h", "result": "banana", "players": []}]},
])
def test_save_rejects_malformed_payloads(client, auth, payload):
    assert client.post("/api/battles/save", json=payload, headers=auth()).status_code == 400


def test_save_ignores_unknown_fields(client, auth, fake_db):
    """Only known columns may reach SQL, whatever the client sends."""
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]
    battle["dropped_by_the_server"] = "; DROP TABLE battles; --"
    client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    stored = next(iter(fake_db.battles.values()))
    assert "dropped_by_the_server" not in stored


# --- deletion is scoped to the uploader ---

def test_a_user_cannot_delete_someone_elses_battle(client, auth, fake_db):
    owner = auth("owner", "secret123")
    stranger = auth("stranger", "secret123")
    battle = _upload(client, owner).json()["battles"][0]
    client.post("/api/battles/save", json={"battles": [battle]}, headers=owner)
    battle_id = next(iter(fake_db.battles))

    assert client.delete(f"/api/battles/{battle_id}", headers=stranger).status_code == 404
    assert battle_id in fake_db.battles

    res = client.delete(f"/api/battles/{battle_id}", headers=owner)
    assert res.status_code == 200 and res.json()["outcome"] == "deleted"
    assert battle_id not in fake_db.battles


def test_a_shared_battle_survives_until_the_last_owner_deletes_it(client, auth, fake_db):
    first = auth("first", "secret123")
    second = auth("second", "secret123")
    battle = _upload(client, first).json()["battles"][0]
    client.post("/api/battles/save", json={"battles": [battle]}, headers=first)
    client.post("/api/battles/save", json={"battles": [battle]}, headers=second)
    battle_id = next(iter(fake_db.battles))

    assert client.delete(f"/api/battles/{battle_id}", headers=first).json()["outcome"] == "unlinked"
    assert battle_id in fake_db.battles
    assert client.delete(f"/api/battles/{battle_id}", headers=second).json()["outcome"] == "deleted"
    assert battle_id not in fake_db.battles


# --- hiding players is per-user and reversible ---

def test_hidden_players_are_per_user(client, auth, fake_db):
    alice = auth("alice", "secret123")
    bob = auth("bob", "secret123")

    assert client.post("/api/players/hidden", json={"name": "Noob"}, headers=alice).status_code == 200
    assert client.get("/api/players/hidden", headers=alice).json()["hidden"] == ["Noob"]
    assert client.get("/api/players/hidden", headers=bob).json()["hidden"] == []


def test_hiding_a_player_does_not_delete_their_stats(client, auth, fake_db):
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]
    client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    client.post("/api/players/hidden", json={"name": "Alice"}, headers=headers)

    summary = client.get("/api/analytics/summary", headers=headers).json()
    assert "Alice" not in [p["name"] for p in summary["players"]]
    assert summary["hidden"] == ["Alice"]
    # The underlying rows are untouched, so unhiding brings the player straight back.
    assert any(p["name"] == "Alice" for p in next(iter(fake_db.battles.values()))["players"])

    client.delete("/api/players/hidden/Alice", headers=headers)
    summary = client.get("/api/analytics/summary", headers=headers).json()
    assert "Alice" in [p["name"] for p in summary["players"]]


def test_unhiding_a_player_who_is_not_hidden_is_a_404(client, auth):
    assert client.delete("/api/players/hidden/Nobody", headers=auth()).status_code == 404


def test_hide_requires_a_name(client, auth):
    assert client.post("/api/players/hidden", json={"name": "  "}, headers=auth()).status_code == 400


# --- health check ---

def test_healthz_reports_the_database(client):
    res = client.get("/healthz")
    assert res.status_code == 200 and res.json() == {"status": "ok", "database": True}


def test_healthz_is_unhealthy_without_a_database(client, monkeypatch):
    import app as app_module
    monkeypatch.setattr(app_module, "_DB_AVAILABLE", False)
    res = client.get("/healthz")
    assert res.status_code == 503 and res.json()["status"] == "degraded"
