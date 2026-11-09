import re

import pytest

import config
from replay_factory import build_replay


def _upload(client, headers, name="battle.mtreplay", data=None):
    files = [("files", (name, data if data is not None else build_replay(), "application/octet-stream"))]
    return client.post("/api/upload", files=files, headers=headers)


def _save_ticket(client, headers, battle):
    return client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=headers)


def _clean(battle):
    """The server-side cleaning a battle goes through after its ticket is opened."""
    from app import _clean_battle
    return _clean_battle({k: v for k, v in battle.items() if k != "ticket"})


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
    assert client.post("/api/auth/register", json={"consent": True, "username": "ivan", "password": "secret123"}).status_code == 200
    res = client.post("/api/auth/login", json={"username": "ivan", "password": "secret123"})
    assert res.status_code == 200 and res.json()["username"] == "ivan"


def test_duplicate_username_is_rejected(client):
    client.post("/api/auth/register", json={"consent": True, "username": "ivan", "password": "secret123"})
    res = client.post("/api/auth/register", json={"consent": True, "username": "ivan", "password": "other123"})
    assert res.status_code == 409


@pytest.mark.parametrize("username,password", [("ab", "secret123"), ("x" * 33, "secret123"), ("ivan", "short")])
def test_registration_validates_its_input(client, username, password):
    assert client.post("/api/auth/register", json={"consent": True, "username": username, "password": password}).status_code == 400


def test_login_is_throttled_after_repeated_failures(client):
    client.post("/api/auth/register", json={"consent": True, "username": "ivan", "password": "secret123"})
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

    res = _save_ticket(client, headers, battle)
    assert res.json() == {"saved": 1, "created": 1, "linked": 0, "skipped": 0}

    # The same battle uploaded twice is deduplicated by its hash.
    res = _save_ticket(client, headers, battle)
    assert res.json() == {"saved": 0, "created": 0, "linked": 0, "skipped": 1}

    listed = client.get("/api/battles", headers=headers).json()["battles"]
    assert len(listed) == 1 and listed[0]["mine"] is True


@pytest.mark.parametrize("payload", [
    {"tickets": "not-a-list"},
    {"tickets": ["1.abc.def"]},
    {"tickets": [None]},
    {"tickets": ["x" * 2_000_000]},
])
def test_save_rejects_malformed_tickets(client, auth, payload):
    assert client.post("/api/battles/save", json=payload, headers=auth()).status_code == 400


@pytest.mark.parametrize("battle", [
    {"players": []},                                        # no battle_hash
    {"battle_hash": "h", "players": {}},                    # players is not a list
    {"battle_hash": "h", "player_team": 0, "players": []},  # no team
    {"battle_hash": "h", "player_team": 1, "players": []},  # no players
])
def test_cleaning_rejects_malformed_battles(battle):
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        _clean(battle)


def test_battle_json_from_the_client_is_refused(client, auth, fake_db):
    """The forgery this closes: invented numbers posted under a real battle_hash."""
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]
    battle["players"][0]["damage_dealt"] = 99999
    res = client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    assert res.status_code == 400 and "билет" in res.json()["detail"]
    assert fake_db.battles == {}


def test_a_tampered_ticket_is_refused_and_nothing_is_saved(client, auth, fake_db):
    import base64, json, zlib
    headers = auth()
    good = _upload(client, headers).json()["battles"][0]["ticket"]
    version, body, sig = good.split(".")
    data = json.loads(zlib.decompress(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))))
    data["b"]["players"][0]["damage_dealt"] = 99999
    forged_body = base64.urlsafe_b64encode(zlib.compress(json.dumps(data).encode())).rstrip(b"=").decode()
    res = client.post("/api/battles/save", json={"tickets": [good, f"{version}.{forged_body}.{sig}"]},
                      headers=headers)
    assert res.status_code == 400
    assert fake_db.battles == {}  # the genuine one in the same request is not saved either


def test_an_expired_ticket_is_refused(client, auth, fake_db, monkeypatch):
    import tickets
    headers = auth()
    ticket = _upload(client, headers).json()["battles"][0]["ticket"]
    later = __import__("time").time() + (config.BATTLE_TICKET_TTL_HOURS + 1) * 3600
    monkeypatch.setattr(tickets.time, "time", lambda: later)
    assert _save_ticket(client, headers, {"ticket": ticket}).status_code == 400


def test_a_ticket_signed_with_another_key_is_refused(client, auth, fake_db):
    import hashlib, hmac, tickets
    headers = auth()
    own_key = tickets._KEY
    tickets._KEY = hmac.new(b"someone-elses-key", b"x", hashlib.sha256).digest()
    try:
        foreign = tickets.issue({"battle_hash": "h", "players": []})
    finally:
        tickets._KEY = own_key
    assert client.post("/api/battles/save", json={"tickets": [foreign]}, headers=headers).status_code == 400


def test_save_ignores_unknown_fields():
    """Only known columns may reach SQL, even from a genuine parse."""
    from replay_parser import parse_replay_bytes
    from app import battle_to_dict
    battle = battle_to_dict(parse_replay_bytes(build_replay(), "b.mtreplay"))
    battle["dropped_by_the_server"] = "; DROP TABLE battles; --"
    assert "dropped_by_the_server" not in _clean(battle)


# --- deletion is scoped to the uploader ---

def test_a_user_cannot_delete_someone_elses_battle(client, auth, fake_db):
    owner = auth("owner", "secret123")
    stranger = auth("stranger", "secret123")
    battle = _upload(client, owner).json()["battles"][0]
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=owner)
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
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=first)
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=second)
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
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=headers)
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


# --- what actually gets stored ---

def test_the_result_is_derived_from_the_teams_not_taken_from_the_client(client, auth):
    battle = _upload(client, auth()).json()["battles"][0]
    assert battle["result"] == "win"
    battle["result"] = "loss"          # a stray field claiming otherwise
    battle["winner_team"] = 1
    battle["player_team"] = 1
    assert _clean(battle)["result"] == "win"


def test_a_battle_whose_teams_say_loss_is_stored_as_a_loss(client, auth, fake_db):
    headers = auth()
    battle = _upload(client, headers, data=build_replay(winner_team=2)).json()["battles"][0]
    _save_ticket(client, headers, battle)
    assert next(iter(fake_db.battles.values()))["result"] == "loss"


def test_an_unfinished_replay_is_reported_but_not_stored(client, auth, fake_db):
    """It has a roster and no results, so saving it would invent a draw full of zeros."""
    headers = auth()
    body = _upload(client, headers, data=build_replay(with_results=False)).json()
    battle = body["battles"][0]
    assert battle["complete"] is False

    res = _save_ticket(client, headers, battle)
    assert res.status_code == 400
    assert "езавершённый" in res.json()["detail"]
    assert fake_db.battles == {}


def test_a_battle_with_no_recorded_activity_is_refused(client, auth, fake_db):
    headers = auth()
    idle = [{"name": "Alice", "team": 1}, {"name": "Bob", "team": 2}]
    battle = _upload(client, headers, data=build_replay(players=idle)).json()["battles"][0]
    assert _save_ticket(client, headers, battle).status_code == 400


def test_saving_reports_new_shared_and_already_known_battles(client, auth, fake_db):
    first = auth("first", "secret123")
    second = auth("second", "secret123")
    battle = _upload(client, first).json()["battles"][0]

    assert _save_ticket(client, first, battle).json() == \
        {"saved": 1, "created": 1, "linked": 0, "skipped": 0}
    # Someone else's upload of the same battle joins their history rather than vanishing.
    assert _save_ticket(client, second, battle).json() == \
        {"saved": 1, "created": 0, "linked": 1, "skipped": 0}
    assert _save_ticket(client, second, battle).json() == \
        {"saved": 0, "created": 0, "linked": 0, "skipped": 1}


# --- usernames ---

def test_usernames_differing_only_in_case_are_the_same_account(client):
    client.post("/api/auth/register", json={"consent": True, "username": "Pomalob", "password": "secret123"})
    assert client.post("/api/auth/register", json={"consent": True, "username": "pomalob", "password": "other123"}).status_code == 409
    res = client.post("/api/auth/login", json={"username": "POMALOB", "password": "secret123"})
    assert res.status_code == 200 and res.json()["username"] == "Pomalob"


# --- the min_battles filter ---

def test_analytics_reports_how_many_players_the_filter_hides(client, auth, fake_db):
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=headers)

    summary = client.get("/api/analytics/summary?min_battles=1", headers=headers).json()
    assert summary["players_total"] == 2 and len(summary["players"]) == 2
    assert summary["min_battles"] == 1

    # With hundreds of battles uploaded, most names appear once and bury the rest.
    summary = client.get("/api/analytics/summary?min_battles=5", headers=headers).json()
    assert summary["players"] == [] and summary["players_total"] == 2


def test_the_tank_table_shows_the_same_players_as_the_filtered_summary(client, auth, fake_db):
    headers = auth()
    battle = _upload(client, headers).json()["battles"][0]
    client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=headers)

    summary = client.get("/api/analytics/summary?min_battles=5", headers=headers).json()
    assert summary["vehicles"] == []


@pytest.mark.parametrize("requested,expected", [(0, 1), (-5, 1), (1000, 100)])
def test_min_battles_is_clamped(client, auth, requested, expected):
    res = client.get(f"/api/analytics/summary?min_battles={requested}", headers=auth())
    assert res.json()["min_battles"] == expected


# --- saving ---

def _save(client, headers, **replay):
    battle = _upload(client, headers, data=build_replay(**replay)).json()["battles"][0]
    _save_ticket(client, headers, battle)
    return battle


def test_save_keeps_only_known_detail_fields(client, auth):
    battle = _upload(client, auth()).json()["battles"][0]
    battle["details"]["injected"] = "x"
    battle["details"]["duration"] = {"deep": {"deeper": {"deepest": 1}}}
    battle["players"][0]["details"]["credits"] = 10 ** 9
    battle["players"][0]["details"]["shots"] = "x" * 5000

    stored = _clean(battle)
    assert "injected" not in stored["details"]
    assert stored["details"]["duration"] == {"deep": {"deeper": None}}
    assert "credits" not in stored["players"][0]["details"]
    assert len(stored["players"][0]["details"]["shots"]) == 200


def test_login_accepts_json_sent_without_a_content_type(client):
    """Some HTTP clients (curl -d, scripts) do not set Content-Type."""
    client.post("/api/auth/register", json={"consent": True, "username": "ivan", "password": "secret123"})
    res = client.post("/api/auth/login", content='{"username": "ivan", "password": "secret123"}')
    assert res.status_code == 200 and res.json()["token"]


# --- rate limits and the client address ---

class _Req:
    def __init__(self, peer, forwarded=None):
        self.client = type("C", (), {"host": peer})()
        self.headers = {"x-forwarded-for": forwarded} if forwarded else {}


@pytest.mark.parametrize("hops,forwarded,expected", [
    (0, "6.6.6.6", "10.0.0.1"),                    # no proxy trusted: the header is ignored
    (1, "6.6.6.6, 203.0.113.7", "203.0.113.7"),    # our proxy appended the real address
    (1, "203.0.113.7", "203.0.113.7"),
    (2, "6.6.6.6, 203.0.113.7, 10.0.0.9", "203.0.113.7"),
    (1, None, "10.0.0.1"),                         # header missing: fall back to the peer
])
def test_client_ip_trusts_only_the_configured_proxies(monkeypatch, hops, forwarded, expected):
    from app import client_ip
    monkeypatch.setattr(config, "TRUSTED_PROXY_HOPS", hops)
    assert client_ip(_Req("10.0.0.1", forwarded)) == expected


def test_a_victims_login_is_not_locked_by_someone_elses_failures(client, fake_db, monkeypatch):
    """Behind a proxy every request used to share one address, so ten wrong passwords from
    anyone locked the account for everybody."""
    monkeypatch.setattr(config, "TRUSTED_PROXY_HOPS", 1)
    client.post("/api/auth/register", json={"consent": True, "username": "victim", "password": "secret123"})
    attacker = {"X-Forwarded-For": "198.51.100.66"}
    for _ in range(config.LOGIN_MAX_ATTEMPTS):
        client.post("/api/auth/login", json={"username": "victim", "password": "wrong"}, headers=attacker)
    assert client.post("/api/auth/login", json={"username": "victim", "password": "secret123"},
                       headers=attacker).status_code == 429
    res = client.post("/api/auth/login", json={"username": "victim", "password": "secret123"},
                      headers={"X-Forwarded-For": "203.0.113.7"})
    assert res.status_code == 200


def test_one_address_cannot_spray_passwords_across_many_names(client, fake_db, monkeypatch):
    monkeypatch.setattr(config, "LOGIN_FAILURES_PER_IP", 3)
    for name in ("a1", "a2", "a3"):
        client.post("/api/auth/login", json={"username": name, "password": "wrong"})
    assert client.post("/api/auth/login", json={"username": "a4", "password": "x"}).status_code == 429


def test_registrations_are_limited_per_address(client, fake_db, monkeypatch):
    monkeypatch.setattr(config, "REGISTRATIONS_PER_HOUR", 2)
    for name in ("one", "two"):
        assert client.post("/api/auth/register", json={"consent": True, "username": name, "password": "secret123"}).status_code == 200
    assert client.post("/api/auth/register", json={"consent": True, "username": "three", "password": "secret123"}).status_code == 429


def test_uploads_are_limited_per_account(client, auth, monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_FILES_PER_HOUR", 2)
    headers = auth()
    assert _upload(client, headers).status_code == 200
    assert _upload(client, headers).status_code == 200
    assert _upload(client, headers).status_code == 429


# --- consent, sessions, account deletion ---

def test_registration_needs_explicit_consent(client, fake_db):
    for body in ({}, {"consent": False}, {"consent": "yes"}):
        res = client.post("/api/auth/register", json={"username": "ivan", "password": "secret123", **body})
        assert res.status_code == 400
    assert fake_db.users == {}


def test_an_account_without_current_consent_is_asked_again(client, auth, fake_db, monkeypatch):
    headers = auth()
    assert client.get("/api/auth/me", headers=headers).json()["consent_required"] is False
    monkeypatch.setattr(config, "CONSENT_VERSION", "2027-01-01")  # the text changed
    assert client.get("/api/auth/me", headers=headers).json()["consent_required"] is True
    assert client.post("/api/auth/consent", json={"consent": False}, headers=headers).status_code == 400
    assert client.post("/api/auth/consent", json={"consent": True}, headers=headers).status_code == 200
    assert client.get("/api/auth/me", headers=headers).json()["consent_required"] is False


def test_changing_the_password_signs_out_other_sessions(client, auth):
    old = auth("ivan", "secret123")
    login = client.post("/api/auth/login", json={"username": "ivan", "password": "secret123"})
    other_device = {"Authorization": f"Bearer {login.json()['token']}"}
    assert client.get("/api/auth/me", headers=other_device).status_code == 200
    res = client.post("/api/auth/password", json={"old_password": "nope", "new_password": "newpass1"}, headers=old)
    assert res.status_code == 403
    res = client.post("/api/auth/password", json={"old_password": "secret123", "new_password": "newpass1"},
                      headers=old)
    assert res.status_code == 200
    assert client.get("/api/auth/me", headers=other_device).status_code == 401
    fresh = {"Authorization": f"Bearer {res.json()['token']}"}
    assert client.get("/api/auth/me", headers=fresh).status_code == 200
    assert client.post("/api/auth/login", json={"username": "ivan", "password": "newpass1"}).status_code == 200


def test_sign_out_everywhere_voids_every_token(client, auth):
    headers = auth()
    assert client.post("/api/auth/logout-all", headers=headers).status_code == 200
    assert client.get("/api/auth/me", headers=headers).status_code == 401


def test_deleting_the_account_removes_what_only_it_owned(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    shared = _upload(client, alice).json()["battles"][0]
    own = _upload(client, alice, data=build_replay(map_name="01_karelia")).json()["battles"][0]
    for battle in (shared, own):
        _save_ticket(client, alice, battle)
    _save_ticket(client, bob, shared)
    team = client.post("/api/teams", json={"name": "Взвод"}, headers=alice).json()["id"]
    code = client.post(f"/api/teams/{team}/invites", json={}, headers=alice).json()["code"]
    client.post(f"/api/invites/{code}/accept", headers=bob)

    assert client.request("DELETE", "/api/auth/me", json={"password": "wrong"}, headers=alice).status_code == 403
    res = client.request("DELETE", "/api/auth/me", json={"password": "secret123"}, headers=alice)
    assert res.json() == {"battles_deleted": 1, "teams_handed_over": 1, "teams_deleted": 0}

    assert client.get("/api/auth/me", headers=alice).status_code == 401  # the token died with it
    assert len(fake_db.battles) == 1                                     # bob's copy stays
    assert client.get(f"/api/teams/{team}", headers=bob).json()["role"] == "owner"
    assert client.post("/api/auth/login", json={"username": "alice", "password": "secret123"}).status_code == 401


def test_tokens_from_before_versions_existed_still_work(client, auth, fake_db):
    """Sessions issued before this deploy carry no "ver": they count as version 0."""
    import jwt
    auth("ivan")
    user = fake_db.get_user_by_username("ivan")
    legacy = jwt.encode({"sub": str(user["id"]), "username": "ivan"}, config.SECRET_KEY, algorithm="HS256")
    assert client.get("/api/auth/me", headers={"Authorization": f"Bearer {legacy}"}).status_code == 200


def test_the_interface_loads_nothing_from_other_sites():
    """Third-party fonts or scripts would send every visitor's IP abroad, and CSP blocks them."""
    from pathlib import Path
    frontend = Path(__file__).parent.parent / "frontend"
    sources = [frontend / "index.html", *(frontend / "src").rglob("*.ts*"), *(frontend / "src").rglob("*.css")]
    for f in sources:
        text = f.read_text(encoding="utf-8")
        assert not re.search(r"(src|href)=[\"']https?://|url\(['\"]?https?://|googleapis|gstatic", text), f.name
    assert len(list((frontend / "src" / "assets" / "fonts").glob("*.woff2"))) == 8


def test_the_brand_does_not_use_the_games_name():
    from pathlib import Path
    shell = (Path(__file__).parent.parent / "frontend" / "src" / "components" / "Shell.tsx").read_text(encoding="utf-8")
    logo = shell[shell.index("export function Logo"):shell.index("const icons")]
    assert "Мир" not in logo and "танк" not in logo.lower()


# --- the site: the built React app on every path that is not the API ---

@pytest.fixture
def built(tmp_path, monkeypatch):
    import app as app_module
    (tmp_path / "assets").mkdir()
    (tmp_path / "index.html").write_text("<div id=root></div>", encoding="utf-8")
    (tmp_path / "assets" / "main-abc123.js").write_text("console.log(1)", encoding="utf-8")
    (tmp_path / "favicon.svg").write_text("<svg/>", encoding="utf-8")
    (tmp_path.parent / "secret.txt").write_text("nope", encoding="utf-8")
    monkeypatch.setattr(app_module, "FRONTEND_DIR", tmp_path)
    return tmp_path


@pytest.mark.parametrize("path", ["/", "/analytics", "/analytics/players", "/team", "/join/abc", "/login", "/privacy"])
def test_every_page_is_the_app(client, built, path):
    res = client.get(path)
    assert res.status_code == 200 and "id=root" in res.text
    assert res.headers["cache-control"] == "no-cache"
    assert "frame-ancestors 'none'" in res.headers["content-security-policy"]


def test_build_files_are_served_and_cached(client, built):
    res = client.get("/assets/main-abc123.js")
    assert res.text == "console.log(1)" and "immutable" in res.headers["cache-control"]
    assert client.get("/favicon.svg").headers["cache-control"] == "no-cache"


def test_nothing_outside_the_build_is_served(client, built):
    assert "nope" not in client.get("/..%2Fsecret.txt").text
    assert "nope" not in client.get("/assets/..%2F..%2Fsecret.txt").text


def test_an_unknown_api_path_is_a_404_not_the_app(client, built):
    res = client.get("/api/nothing-here")
    assert res.status_code == 404 and "id=root" not in res.text


def test_the_site_says_when_it_is_not_built(client, tmp_path, monkeypatch):
    import app as app_module
    monkeypatch.setattr(app_module, "FRONTEND_DIR", tmp_path / "missing")
    assert client.get("/analytics").status_code == 503
