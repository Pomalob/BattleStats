"""Who sees which battles: own uploads, teams joined through invite links, admins.
Plus battle modes and the standard-battle rule that strangers do not count."""
import pytest

import config
from replay_factory import build_replay


def _save(client, headers, **replay):
    files = {"files": ("battle.mtreplay", build_replay(**replay), "application/octet-stream")}
    battle = client.post("/api/upload", files=files, headers=headers).json()["battles"][0]
    res = client.post("/api/battles/save", json={"tickets": [battle["ticket"]]}, headers=headers)
    assert res.status_code == 200, res.text
    return battle


def _battles(client, headers, query=""):
    res = client.get(f"/api/battles{query}", headers=headers)
    assert res.status_code == 200, res.text
    return res.json()["battles"]


def _team(client, headers, name="Взвод"):
    res = client.post("/api/teams", json={"name": name}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()["id"]


def _invite(client, headers, team_id, **body):
    res = client.post(f"/api/teams/{team_id}/invites", json=body, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()["code"]


def _join(client, headers, code):
    return client.post(f"/api/invites/{code}/accept", headers=headers)


# --- privacy: strangers do not see each other ---

def test_a_stranger_does_not_see_my_battles(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    _save(client, alice)
    assert len(_battles(client, alice)) == 1
    assert _battles(client, bob) == []
    assert client.get("/api/analytics/summary", headers=bob).json()["totals"]["total_battles"] == 0


def test_joining_a_team_shares_battles_both_ways(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    _save(client, alice)
    _save(client, bob, map_name="01_karelia")
    team = _team(client, alice)
    assert _join(client, bob, _invite(client, alice, team)).json() == {"team_id": team, "outcome": "joined"}

    assert len(_battles(client, alice)) == 2 and len(_battles(client, bob)) == 2
    assert len(_battles(client, bob, f"?scope=team:{team}")) == 2
    assert len(_battles(client, bob, "?scope=mine")) == 1
    assert len(_battles(client, bob, "?mine=true")) == 1  # the older spelling


def test_a_team_scope_is_closed_to_outsiders(client, auth, fake_db):
    alice, eve = auth("alice"), auth("eve")
    team = _team(client, alice)
    assert client.get(f"/api/battles?scope=team:{team}", headers=eve).status_code == 404
    assert client.get(f"/api/teams/{team}", headers=eve).status_code == 404
    assert client.get("/api/battles?scope=team:x", headers=eve).status_code == 400
    assert client.get("/api/battles?scope=everything", headers=eve).status_code == 400


def test_leaving_a_team_stops_sharing(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    _save(client, alice)
    team = _team(client, alice)
    _join(client, bob, _invite(client, alice, team))
    bob_id = fake_db.get_user_by_username("bob")["id"]
    assert client.delete(f"/api/teams/{team}/members/{bob_id}", headers=bob).status_code == 200
    assert _battles(client, bob) == []


# --- team management ---

def test_owner_manages_the_team_members_only_read_it(client, auth, fake_db):
    alice, bob, carol = auth("alice"), auth("bob"), auth("carol")
    team = _team(client, alice)
    code = _invite(client, alice, team)
    _join(client, bob, code)
    _join(client, carol, code)

    details = client.get(f"/api/teams/{team}", headers=bob).json()
    assert details["role"] == "member" and details["invites"] == []
    assert [m["username"] for m in details["members"]] == ["alice", "bob", "carol"]
    assert client.get(f"/api/teams/{team}", headers=alice).json()["invites"][0]["code"] == code

    carol_id = fake_db.get_user_by_username("carol")["id"]
    for method, path in (("post", f"/api/teams/{team}/invites"), ("patch", f"/api/teams/{team}"),
                         ("delete", f"/api/teams/{team}"), ("delete", f"/api/teams/{team}/members/{carol_id}"),
                         ("delete", f"/api/teams/{team}/invites/{code}")):
        kwargs = {"json": {"name": "Захват"}} if method in ("post", "patch") else {}
        assert getattr(client, method)(path, headers=bob, **kwargs).status_code == 403, path

    assert client.delete(f"/api/teams/{team}/members/{carol_id}", headers=alice).status_code == 200
    assert client.patch(f"/api/teams/{team}", json={"name": "  Новое   имя "}, headers=alice).json()["name"] == "Новое имя"


def test_the_owner_cannot_leave_but_can_delete_the_team(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    _save(client, alice)
    team = _team(client, alice)
    _join(client, bob, _invite(client, alice, team))
    alice_id = fake_db.get_user_by_username("alice")["id"]
    assert client.delete(f"/api/teams/{team}/members/{alice_id}", headers=alice).status_code == 409
    assert client.delete(f"/api/teams/{team}", headers=alice).status_code == 200
    assert _battles(client, bob) == []
    assert len(_battles(client, alice)) == 1  # the battles themselves stay with the uploader


@pytest.mark.parametrize("name", ["", "x", "y" * 41])
def test_team_names_are_validated(client, auth, name):
    assert client.post("/api/teams", json={"name": name}, headers=auth()).status_code == 400


def test_a_user_owns_a_limited_number_of_teams(client, auth, monkeypatch):
    monkeypatch.setattr(config, "MAX_TEAMS_PER_USER", 2)
    headers = auth()
    _team(client, headers, "Один")
    _team(client, headers, "Два")
    assert client.post("/api/teams", json={"name": "Три"}, headers=headers).status_code == 409
    assert len(client.get("/api/teams", headers=headers).json()["teams"]) == 2


# --- invites ---

def test_an_invite_is_spent_only_by_a_real_join(client, auth, fake_db):
    alice, bob, carol = auth("alice"), auth("bob"), auth("carol")
    team = _team(client, alice)
    code = _invite(client, alice, team, max_uses=1)

    assert client.get(f"/api/invites/{code}", headers=alice).json() == {
        "team_id": team, "team_name": "Взвод", "already_member": True}
    assert _join(client, alice, code).json()["outcome"] == "already"  # the owner clicking it
    assert _join(client, bob, code).json()["outcome"] == "joined"
    assert _join(client, carol, code).status_code == 404  # used up
    assert client.get(f"/api/invites/{code}", headers=carol).status_code == 404


def test_a_revoked_or_unknown_invite_does_not_work(client, auth, fake_db):
    alice, bob = auth("alice"), auth("bob")
    team = _team(client, alice)
    code = _invite(client, alice, team)
    assert client.delete(f"/api/teams/{team}/invites/{code}", headers=alice).status_code == 200
    assert _join(client, bob, code).status_code == 404
    assert _join(client, bob, "made-up").status_code == 404


def test_an_expired_invite_does_not_work(client, auth, fake_db):
    from datetime import datetime, timedelta, timezone
    alice, bob = auth("alice"), auth("bob")
    code = _invite(client, alice, _team(client, alice))
    fake_db.invites[code]["expires_at"] = datetime.now(timezone.utc) - timedelta(minutes=1)
    assert _join(client, bob, code).status_code == 404


def test_invite_lifetime_and_uses_are_clamped(client, auth, fake_db, monkeypatch):
    monkeypatch.setattr(config, "INVITE_MAX_DAYS", 30)
    alice = auth("alice")
    team = _team(client, alice)
    code = _invite(client, alice, team, days=365, max_uses=0)
    invite = fake_db.invites[code]
    assert invite["max_uses"] is None  # 0 / missing means unlimited
    assert client.post(f"/api/teams/{team}/invites", json={"days": "soon"}, headers=alice).status_code == 400
    assert len(code) >= 20


def test_a_full_team_takes_no_one_else(client, auth, fake_db, monkeypatch):
    monkeypatch.setattr(config, "MAX_TEAM_MEMBERS", 2)
    alice, bob, carol = auth("alice"), auth("bob"), auth("carol")
    code = _invite(client, alice, _team(client, alice))
    assert _join(client, bob, code).status_code == 200
    assert _join(client, carol, code).status_code == 409


# --- admin ---

def test_only_an_admin_sees_the_whole_database(client, auth, fake_db, monkeypatch):
    alice, boss = auth("alice"), auth("Boss")
    monkeypatch.setattr(config, "ADMIN_USER_IDS", frozenset({fake_db.get_user_by_username("boss")["id"]}))
    _save(client, alice)

    assert client.get("/api/auth/me", headers=boss).json()["is_admin"] is True
    assert client.get("/api/auth/me", headers=alice).json()["is_admin"] is False
    assert client.get("/api/battles?scope=all", headers=alice).status_code == 403
    assert len(_battles(client, boss, "?scope=all")) == 1
    assert _battles(client, boss) == []  # the default is still the admin's own circle

    assert client.get("/api/admin/overview", headers=alice).status_code == 403
    assert client.get("/api/admin/overview", headers=boss).json()["battles"] == 1


def test_an_admin_may_open_any_team(client, auth, fake_db, monkeypatch):
    alice, boss = auth("alice"), auth("boss")
    monkeypatch.setattr(config, "ADMIN_USER_IDS", frozenset({fake_db.get_user_by_username("boss")["id"]}))
    _save(client, alice)
    team = _team(client, alice)
    assert client.get(f"/api/teams/{team}", headers=boss).json()["role"] == "admin"
    assert len(_battles(client, boss, f"?scope=team:{team}")) == 1


# --- battle modes ---

@pytest.mark.parametrize("bonus_type,mode", [(1, "standard"), (20, "clan"), (43, "onslaught"), (2, "other"),
                                             (None, "other")])
def test_the_mode_is_stored_from_the_replay(client, auth, fake_db, bonus_type, mode):
    headers = auth()
    _save(client, headers, bonus_type=bonus_type)
    assert _battles(client, headers)[0]["mode"] == mode
    assert len(_battles(client, headers, f"?mode={mode}")) == 1


def test_analytics_can_be_narrowed_to_one_mode(client, auth, fake_db):
    headers = auth()
    _save(client, headers, bonus_type=20)
    _save(client, headers, bonus_type=43, map_name="01_karelia")
    totals = client.get("/api/analytics/totals?mode=onslaught", headers=headers).json()
    assert totals["total_battles"] == 1
    assert client.get("/api/analytics/summary?mode=clan", headers=headers).json()["totals"]["total_battles"] == 1
    assert client.get("/api/analytics/totals?mode=arcade", headers=headers).status_code == 400


def test_the_mode_comes_from_the_replay_not_from_a_stray_field():
    from app import _clean_battle, battle_to_dict
    from replay_parser import parse_replay_bytes
    battle = battle_to_dict(parse_replay_bytes(build_replay(bonus_type=1), "b.mtreplay"))
    battle["mode"] = "clan"
    assert _clean_battle(battle)["mode"] == "standard"


RANDOM_SQUAD = [
    {"name": "Alice", "team": 1, "damage": 2000, "shots": 5, "platoon": 77},
    {"name": "Bob", "team": 1, "damage": 1500, "shots": 5, "platoon": 77},
    {"name": "Stranger", "team": 1, "damage": 900, "shots": 5},
    {"name": "Enemy", "team": 2, "damage": 800, "shots": 5},
]
NAMES = {p["name"]: p["name"] for p in RANDOM_SQUAD}


def _summary_names(client, headers):
    res = client.get("/api/analytics/summary?min_battles=1", headers=headers).json()
    return sorted(p["name"] for p in res["players"])


def test_a_random_battle_counts_only_the_uploader_and_their_platoon(client, auth, fake_db):
    headers = auth()
    _save(client, headers, bonus_type=1, players=RANDOM_SQUAD, real_names=NAMES)
    assert _summary_names(client, headers) == ["Alice", "Bob"]


def test_a_solo_random_battle_counts_only_the_uploader(client, auth, fake_db):
    headers = auth()
    solo = [{**p, "platoon": 0} for p in RANDOM_SQUAD]
    _save(client, headers, bonus_type=1, players=solo, real_names=NAMES)
    assert _summary_names(client, headers) == ["Alice"]


def test_onslaught_is_matchmade_too(client, auth, fake_db):
    headers = auth()
    _save(client, headers, bonus_type=43, players=RANDOM_SQUAD, real_names=NAMES)
    assert _summary_names(client, headers) == ["Alice", "Bob"]


def test_clan_battles_count_the_whole_team(client, auth, fake_db):
    headers = auth()
    _save(client, headers, bonus_type=20, players=RANDOM_SQUAD, real_names=NAMES)
    assert _summary_names(client, headers) == ["Alice", "Bob", "Stranger"]


def test_a_username_alone_never_makes_an_admin(client, auth, fake_db, monkeypatch):
    """Admins are listed by id: registering a listed-looking name grants nothing."""
    monkeypatch.setattr(config, "ADMIN_USER_IDS", frozenset({999}))
    headers = auth("admin")
    assert client.get("/api/auth/me", headers=headers).json()["is_admin"] is False
    assert client.get("/api/admin/overview", headers=headers).status_code == 403


def test_pages_and_api_carry_security_headers(client, auth):
    for res in (client.get("/healthz"), client.get("/api/teams", headers=auth())):
        assert "frame-ancestors 'none'" in res.headers["content-security-policy"]
        assert res.headers["x-content-type-options"] == "nosniff"
        assert res.headers["referrer-policy"] == "same-origin"


# --- period ---

def test_analytics_can_be_narrowed_to_a_period(client, auth, fake_db):
    headers = auth()
    for day, name in (("01", "01_karelia"), ("05", "02_malinovka"), ("09", "03_campania")):
        _save(client, headers, date_time=f"{day}.01.2025 10:00:00", map_name=name)
    period = "date_from=2025-01-02&date_to=2025-01-09"
    assert client.get(f"/api/analytics/totals?{period}", headers=headers).json()["total_battles"] == 2
    assert len(_battles(client, headers, f"?{period}")) == 2
    daily = client.get(f"/api/analytics/daily?{period}", headers=headers).json()
    assert [d["day"] for d in daily["days"]] == ["2025-01-05", "2025-01-09"]
    assert daily["totals"]["battles"] == 2
    assert client.get("/api/analytics/daily?date_from=05.01.2025", headers=headers).status_code == 400
