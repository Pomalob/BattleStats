"""Aggregate SQL against a real PostgreSQL.

The rest of the suite stubs the database out, which cannot catch a mistake in the
queries themselves. These tests run only when TEST_DATABASE_URL points at a
throwaway database — `docker compose up db` gives you one:

    TEST_DATABASE_URL=postgresql://battlestats:battlestats@localhost:5432/battlestats pytest

Everything in the target database is truncated between tests.
"""
import os

import pytest

import database as db_module

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is not set"
)


@pytest.fixture(scope="module")
def db():
    original = db_module.DATABASE_URL
    db_module.close_pool()
    db_module.DATABASE_URL = TEST_DATABASE_URL
    db_module.init_db()
    yield db_module
    db_module.close_pool()
    db_module.DATABASE_URL = original


@pytest.fixture(autouse=True)
def clean(db):
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE team_invites, team_members, teams, user_battles, hidden_players, "
                "player_stats, battles, users "
                "RESTART IDENTITY CASCADE"
            )
    yield


def make_battle(battle_hash, map_name, result, winner_team, players, date_time="2025-01-02 20:00:00"):
    """Build the payload save_battle expects, with sensible defaults per player."""
    return {
        "filename": f"{battle_hash}.mtreplay",
        "map_name": map_name,
        "date_time": date_time,
        "battle_hash": battle_hash,
        "result": result,
        "player_team": 1,
        "winner_team": winner_team,
        "players": [
            {
                "name": p["name"], "vehicle": p.get("vehicle", "Object 278"),
                "team": p.get("team", 1),
                "damage_dealt": p.get("damage", 0), "damage_received": p.get("received", 0),
                "blocked": p.get("blocked", 0), "assists": p.get("assists", 0),
                "frags": p.get("frags", 0), "xp": p.get("xp", 0),
                "survived": p.get("survived", True), "shots": p.get("shots", 0),
                "hits": p.get("hits", 0), "piercings": p.get("piercings", 0),
            }
            for p in players
        ],
    }


@pytest.fixture
def seeded(db):
    """Two users and three battles with numbers chosen so every average is exact."""
    alice = db.create_user("alice", "hash")
    bob = db.create_user("bob", "hash")

    win = make_battle("h-win", "Прохоровка", "win", 1, [
        {"name": "P1", "damage": 3000, "assists": 500, "blocked": 1000, "received": 800,
         "frags": 3, "shots": 10, "hits": 8, "piercings": 6},
        {"name": "P2", "damage": 1000, "assists": 100, "blocked": 200, "received": 2000,
         "frags": 0, "shots": 6, "hits": 3, "piercings": 2, "vehicle": "Maus"},
        {"name": "E1", "damage": 9999, "team": 2, "frags": 5},
    ], date_time="2025-01-02 20:00:00")

    loss = make_battle("h-loss", "Прохоровка", "loss", 2, [
        {"name": "P1", "damage": 1000, "assists": 300, "blocked": 400, "received": 1600,
         "frags": 1, "shots": 6, "hits": 4, "piercings": 2},
        {"name": "P2", "damage": 2000, "assists": 200, "blocked": 600, "received": 1000,
         "frags": 2, "shots": 8, "hits": 5, "piercings": 4, "vehicle": "Maus"},
        {"name": "E1", "damage": 8888, "team": 2},
    ], date_time="2025-01-03 20:00:00")

    draw = make_battle("h-draw", "Малиновка", "draw", 0, [
        {"name": "P1", "damage": 2000, "assists": 400, "blocked": 700, "received": 1200,
         "frags": 2, "shots": 8, "hits": 6, "piercings": 4},
    ], date_time="2025-01-04 20:00:00")

    for battle in (win, loss):
        db.save_battle(battle, user_id=alice["id"])
    db.save_battle(draw, user_id=bob["id"])
    return {"alice": alice["id"], "bob": bob["id"]}


# --- schema and users ---

def test_init_db_is_idempotent(db):
    db.init_db()
    db.init_db()
    assert db.ping() is True


def test_usernames_are_unique_case_insensitively(db):
    assert db.create_user("Pomalob", "hash") is not None
    assert db.create_user("pomalob", "hash") is None
    assert db.get_user_by_username("POMALOB")["username"] == "Pomalob"


# --- saving and ownership ---

def test_save_battle_reports_created_linked_and_already(db):
    alice = db.create_user("alice", "h")
    bob = db.create_user("bob", "h")
    battle = make_battle("h1", "Прохоровка", "win", 1, [{"name": "P1", "damage": 100}])

    assert db.save_battle(battle, alice["id"]) == "created"
    assert db.save_battle(battle, alice["id"]) == "already"
    assert db.save_battle(battle, bob["id"]) == "linked"
    assert db.save_battle(battle, bob["id"]) == "already"


def test_a_deduplicated_battle_stores_its_stats_only_once(db):
    alice = db.create_user("alice", "h")
    bob = db.create_user("bob", "h")
    battle = make_battle("h1", "Прохоровка", "win", 1, [
        {"name": "P1", "damage": 100}, {"name": "P2", "damage": 200},
    ])
    db.save_battle(battle, alice["id"])
    db.save_battle(battle, bob["id"])

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM player_stats")
            assert cur.fetchone()["n"] == 2
            cur.execute("SELECT COUNT(*) AS n FROM battles")
            assert cur.fetchone()["n"] == 1


def test_delete_unlinks_first_and_removes_the_row_with_the_last_owner(db):
    alice = db.create_user("alice", "h")
    bob = db.create_user("bob", "h")
    battle = make_battle("h1", "Прохоровка", "win", 1, [{"name": "P1", "damage": 100}])
    db.save_battle(battle, alice["id"])
    db.save_battle(battle, bob["id"])
    battle_id = db.get_battles_list()[0]["id"]

    assert db.delete_battle(battle_id, alice["id"]) == "unlinked"
    assert db.delete_battle(battle_id, alice["id"]) == "not_found"
    assert db.get_battles_list() != []

    assert db.delete_battle(battle_id, bob["id"]) == "deleted"
    assert db.get_battles_list() == []

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM player_stats")
            assert cur.fetchone()["n"] == 0, "player_stats should cascade with the battle"


# --- battle list ---

def test_battles_list_marks_ownership_and_counts_players(db, seeded):
    rows = db.get_battles_list(viewer_id=seeded["alice"])
    assert [r["map_name"] for r in rows] == ["Малиновка", "Прохоровка", "Прохоровка"]
    assert [r["mine"] for r in rows] == [False, True, True]
    assert {r["player_count"] for r in rows} == {1, 3}


def test_battles_list_can_be_narrowed_to_one_user(db, seeded):
    rows = db.get_battles_list(seeded["bob"], viewer_id=seeded["bob"])
    assert len(rows) == 1 and rows[0]["map_name"] == "Малиновка"


# --- aggregates ---

def test_totals_count_each_result(db, seeded):
    totals = db.get_db_totals()
    assert (totals["total_battles"], totals["wins"], totals["losses"], totals["draws"]) == (3, 1, 1, 1)
    assert totals["first_battle"] == "2025-01-02 20:00:00"
    assert totals["last_battle"] == "2025-01-04 20:00:00"


def test_totals_respect_the_user_scope(db, seeded):
    assert db.get_db_totals(seeded["alice"])["total_battles"] == 2
    assert db.get_db_totals(seeded["bob"])["total_battles"] == 1


def test_player_summary_averages_only_the_players_own_team(db, seeded):
    rows = {r["name"]: r for r in db.get_player_summary()}
    assert "E1" not in rows, "the enemy team must not appear in player stats"

    p1 = rows["P1"]
    assert p1["battles"] == 3
    assert int(p1["avg_damage"]) == 2000          # (3000 + 1000 + 2000) / 3
    assert int(p1["avg_assists"]) == 400
    assert int(p1["avg_blocked"]) == 700
    assert int(p1["avg_received"]) == 1200
    assert float(p1["avg_frags"]) == 2.0
    assert (p1["wins"], p1["losses"], p1["draws"]) == (1, 1, 1)
    assert int(p1["avg_dmg_win"]) == 3000
    assert int(p1["avg_dmg_loss"]) == 1000
    assert int(p1["avg_shots"]) == 8 and int(p1["avg_hits"]) == 6 and int(p1["avg_piercings"]) == 4
    # Ratios come from the totals: 18 hits of 24 shots, 12 piercings of 18 hits.
    assert float(p1["accuracy"]) == 75.0 and float(p1["penetration"]) == 66.7
    assert float(p1["survival"]) == 100.0
    assert p1["avg_spotted"] is None, "battles saved without details have no spotting data"


def test_player_summary_is_sorted_by_average_damage(db, seeded):
    names = [r["name"] for r in db.get_player_summary()]
    assert names == ["P1", "P2"]


def test_player_summary_respects_the_user_scope(db, seeded):
    p1 = next(r for r in db.get_player_summary(seeded["bob"]) if r["name"] == "P1")
    assert p1["battles"] == 1 and int(p1["avg_damage"]) == 2000


def test_hidden_players_drop_out_of_the_aggregates_without_losing_data(db, seeded):
    db.hide_player(seeded["alice"], "P2")
    assert db.get_hidden_players(seeded["alice"]) == ["P2"]

    assert "P2" not in [r["name"] for r in db.get_player_summary(viewer_id=seeded["alice"])]
    assert "P2" not in [r["name"] for r in db.get_vehicle_summary(viewer_id=seeded["alice"])]
    # Hiding is per user, and it is a filter rather than a delete.
    assert "P2" in [r["name"] for r in db.get_player_summary(viewer_id=seeded["bob"])]

    db.unhide_player(seeded["alice"], "P2")
    assert "P2" in [r["name"] for r in db.get_player_summary(viewer_id=seeded["alice"])]


def test_map_summary_counts_results_and_winrate(db, seeded):
    rows = {r["map_name"]: r for r in db.get_map_summary()}
    assert int(rows["Прохоровка"]["battles"]) == 2
    assert (rows["Прохоровка"]["wins"], rows["Прохоровка"]["losses"]) == (1, 1)
    assert int(rows["Прохоровка"]["winrate"]) == 50
    assert int(rows["Малиновка"]["winrate"]) == 0, "a draw is not a win"


def test_vehicle_summary_splits_a_player_across_tanks(db, seeded):
    rows = {(r["name"], r["vehicle"]) for r in db.get_vehicle_summary()}
    assert ("P2", "Maus") in rows
    assert ("P1", "Object 278") in rows
    assert not any(name == "E1" for name, _ in rows)

    maus = next(r for r in db.get_vehicle_summary() if r["vehicle"] == "Maus")
    assert int(maus["battles"]) == 2 and int(maus["avg_damage"]) == 1500


def test_aggregates_are_empty_rather_than_broken_without_data(db):
    totals = db.get_db_totals()
    assert totals["total_battles"] == 0 and totals["wins"] is None
    assert db.get_player_summary() == []
    assert db.get_map_summary() == []
    assert db.get_vehicle_summary() == []
    assert db.get_battles_list() == []


def test_min_battles_filters_out_the_one_off_players(db, seeded):
    """A few hundred uploaded battles leave hundreds of names seen exactly once."""
    once = make_battle("h-once", "Утёс", "win", 1, [
        {"name": "P1", "damage": 100}, {"name": "Random", "damage": 100},
    ], date_time="2025-01-05 20:00:00")
    db.save_battle(once, seeded["alice"])

    assert db.count_players() == 3
    assert {r["name"] for r in db.get_player_summary(min_battles=1)} == {"P1", "P2", "Random"}
    assert {r["name"] for r in db.get_player_summary(min_battles=2)} == {"P1", "P2"}
    assert {r["name"] for r in db.get_player_summary(min_battles=4)} == {"P1"}
    assert db.get_player_summary(min_battles=99) == []


def test_count_players_ignores_the_filter_but_honours_hiding(db, seeded):
    assert db.count_players() == 2
    db.hide_player(seeded["alice"], "P2")
    assert db.count_players(viewer_id=seeded["alice"]) == 1
    assert db.count_players(viewer_id=seeded["bob"]) == 2


def test_vehicle_summary_can_be_restricted_to_a_set_of_players(db, seeded):
    assert {r["name"] for r in db.get_vehicle_summary(names=["P1"])} == {"P1"}
    assert db.get_vehicle_summary(names=[]) == []


def test_export_returns_battles_oldest_first_with_players_and_details(db):
    alice = db.create_user("alice", "hash")["id"]
    late = make_battle("h-late", "Карелия", "win", 1, [{"name": "P1", "damage": 100}],
                       date_time="2025-01-03 10:00:00")
    early = make_battle("h-early", "Энск", "loss", 2, [{"name": "P1", "damage": 200}],
                        date_time="2025-01-02 10:00:00")
    early["details"] = {"arenaUniqueID": 1, "teamHealth": {"1": 100}}
    early["recorder"] = {"playerName": "alice"}
    early["players"][0]["details"] = {"accountDBID": 5}
    db.save_battle(late, user_id=alice)
    db.save_battle(early, user_id=alice)

    rows = db.get_battles_export(alice, viewer_id=alice)
    assert [r["battle_hash"] for r in rows] == ["h-early", "h-late"]
    assert rows[0]["details"] == {"arenaUniqueID": 1, "teamHealth": {"1": 100}}
    assert rows[0]["recorder"] == {"playerName": "alice"}
    assert rows[0]["players"][0]["details"] == {"accountDBID": 5}
    assert rows[1]["details"] is None
    assert [r["battle_hash"] for r in db.get_battles_export(
        alice, viewer_id=alice, date_from="2025-01-03", date_to="2025-01-03")] == ["h-late"]


def test_uploading_an_old_battle_again_fills_in_its_details(db):
    alice = db.create_user("alice", "hash")["id"]
    bare = make_battle("h", "Энск", "win", 1, [{"name": "P1"}, {"name": "E1", "team": 2}])
    db.save_battle(bare, user_id=alice)

    full = make_battle("h", "Энск", "win", 1, [{"name": "P1"}, {"name": "E1", "team": 2}])
    full["details"] = {"arenaUniqueID": 9}
    full["recorder"] = {"playerName": "alice"}
    for p in full["players"]:
        p["details"] = {"realName": p["name"]}
    assert db.save_battle(full, user_id=alice) == "already"

    row = db.get_battles_export(alice, viewer_id=alice)[0]
    assert row["details"] == {"arenaUniqueID": 9}
    assert row["recorder"] == {"playerName": "alice"}
    assert {p["details"]["realName"] for p in row["players"]} == {"P1", "E1"}


# --- scopes, teams, modes ---

from database import Scope  # noqa: E402


def _with_mode(battle, mode, recorder=None, platoons=None):
    battle["mode"] = mode
    if recorder:
        battle["recorder"] = {"playerName": recorder}
    for p in battle["players"]:
        p["details"] = {"prebattleID": (platoons or {}).get(p["name"], 0)}
    return battle


def test_scopes_follow_uploads_and_team_membership(db, seeded):
    alice, bob = seeded["alice"], seeded["bob"]
    carol = db.create_user("carol", "hash")["id"]
    assert db.get_db_totals(Scope("visible", user_id=alice))["total_battles"] == 2
    team = db.create_team("Взвод", alice)["id"]
    status, _ = db.accept_invite(db.create_invite(team, alice, "code-1", 7, None)["code"], bob)
    assert status == "joined"

    assert db.get_db_totals(Scope("visible", user_id=alice))["total_battles"] == 3
    assert db.get_db_totals(Scope("visible", user_id=bob))["total_battles"] == 3
    assert db.get_db_totals(Scope("team", team_id=team))["total_battles"] == 3
    assert db.get_db_totals(Scope("mine", user_id=bob))["total_battles"] == 1
    assert db.get_db_totals(Scope("visible", user_id=carol))["total_battles"] == 0
    assert db.get_db_totals(Scope("all"))["total_battles"] == 3
    # every query takes the same scope
    assert db.get_player_summary(Scope("visible", user_id=carol)) == []
    assert len(db.get_battles_list(Scope("team", team_id=team), viewer_id=carol)) == 3
    assert db.count_players(Scope("visible", user_id=bob)) == 2


def test_a_battle_two_teammates_uploaded_is_counted_once(db):
    alice = db.create_user("alice", "hash")["id"]
    bob = db.create_user("bob", "hash")["id"]
    team = db.create_team("Взвод", alice)["id"]
    db.accept_invite(db.create_invite(team, alice, "c", 7, None)["code"], bob)
    battle = make_battle("h", "Энск", "win", 1, [{"name": "P1", "damage": 100}])
    db.save_battle(battle, user_id=alice)
    db.save_battle(battle, user_id=bob)
    assert db.get_db_totals(Scope("team", team_id=team))["total_battles"] == 1
    assert db.get_player_summary(Scope("visible", user_id=alice))[0]["battles"] == 1


def test_mode_narrows_every_scope(db):
    alice = db.create_user("alice", "hash")["id"]
    db.save_battle(_with_mode(make_battle("c", "Энск", "win", 1, [{"name": "P1"}]), "clan"), user_id=alice)
    db.save_battle(_with_mode(make_battle("o", "Энск", "win", 1, [{"name": "P1"}]), "onslaught"), user_id=alice)
    for kind in (Scope("mine", user_id=alice, mode="clan"), Scope("visible", user_id=alice, mode="clan"),
                 Scope("all", mode="clan")):
        assert db.get_db_totals(kind)["total_battles"] == 1, kind
    assert [m["map_name"] for m in db.get_map_summary(Scope("all", mode="onslaught"))] == ["Энск"]
    assert db.get_battles_list(Scope("all", mode="onslaught"))[0]["mode"] == "onslaught"


def test_the_mode_is_derived_from_details_when_not_given(db):
    alice = db.create_user("alice", "hash")["id"]
    battle = make_battle("h", "Энск", "win", 1, [{"name": "P1"}])
    battle["details"] = {"bonusType": 43}
    db.save_battle(battle, user_id=alice)
    assert db.get_battles_list(alice)[0]["mode"] == "onslaught"


def test_old_battles_get_their_mode_backfilled(db):
    alice = db.create_user("alice", "hash")["id"]
    battle = make_battle("h", "Энск", "win", 1, [{"name": "P1"}])
    battle["details"] = {"bonusType": 20}
    db.save_battle(battle, user_id=alice)
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE battles SET mode = NULL")
        db._migrate_battle_modes(conn)
    assert db.get_battles_list(alice)[0]["mode"] == "clan"


def test_a_random_battle_counts_only_the_uploaders_and_their_platoon(db):
    alice = db.create_user("alice", "hash")["id"]
    bob = db.create_user("bob", "hash")["id"]
    players = [{"name": "Me", "damage": 3000}, {"name": "Mate", "damage": 2000},
               {"name": "Random", "damage": 1000}, {"name": "Enemy", "team": 2, "damage": 500}]
    db.save_battle(_with_mode(make_battle("r1", "Энск", "win", 1, players), "standard", recorder="Me",
                              platoons={"Me": 77, "Mate": 77}), user_id=alice)
    # a solo battle: only the uploader
    db.save_battle(_with_mode(make_battle("r2", "Энск", "win", 1, players), "standard", recorder="Me"),
                   user_id=alice)
    summary = {p["name"]: p["battles"] for p in db.get_player_summary(alice)}
    assert summary == {"Me": 2, "Mate": 1}
    assert db.count_players(alice) == 2
    assert {v["name"] for v in db.get_vehicle_summary(alice)} == {"Me", "Mate"}

    # the random ally uploads the same battle too: now they are "ours" as well
    db.save_battle(_with_mode(make_battle("r2", "Энск", "win", 1, players), "standard", recorder="Random"),
                   user_id=bob)
    assert {p["name"] for p in db.get_player_summary(None)} == {"Me", "Mate", "Random"}


def test_clan_battles_keep_the_whole_team(db):
    alice = db.create_user("alice", "hash")["id"]
    players = [{"name": "Me"}, {"name": "Mate"}, {"name": "Enemy", "team": 2}]
    db.save_battle(_with_mode(make_battle("c", "Энск", "win", 1, players), "clan", recorder="Me"), user_id=alice)
    assert {p["name"] for p in db.get_player_summary(alice)} == {"Me", "Mate"}


def test_the_initial_team_gathers_everyone_once(db, monkeypatch):
    alice = db.create_user("alice", "hash")["id"]
    bob = db.create_user("bob", "hash")["id"]
    monkeypatch.setattr(db, "ADMIN_USER_IDS", frozenset({bob}))
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schema_flags WHERE name = 'initial_team'")
        conn.commit()
        db._migrate_initial_team(conn)
    teams = db.get_user_teams(alice)
    assert [(t["name"], t["role"], t["members"]) for t in teams] == [(db.INITIAL_TEAM_NAME, "member", 2)]
    assert db.get_user_teams(bob)[0]["role"] == "owner"  # an admin owns it if there is one

    db.create_user("carol", "hash")
    with db.get_conn() as conn:
        db._migrate_initial_team(conn)
    assert db.get_admin_overview()["teams"] == 1
    assert db.get_team_members(teams[0]["id"])[0]["username"] == "bob"


def test_invites_are_spent_only_on_real_joins(db):
    alice = db.create_user("alice", "hash")["id"]
    bob = db.create_user("bob", "hash")["id"]
    carol = db.create_user("carol", "hash")["id"]
    team = db.create_team("Взвод", alice)["id"]
    db.create_invite(team, alice, "one", 7, 1)
    assert db.accept_invite("one", alice) == ("already", team)
    assert db.accept_invite("one", bob) == ("joined", team)
    assert db.accept_invite("one", carol) == ("invalid", None)
    assert db.get_invite("one") is None and db.list_invites(team) == []

    db.create_invite(team, alice, "two", 7, None)
    assert db.get_invite("two")["team_name"] == "Взвод"
    assert db.revoke_invite(team, "two") and db.accept_invite("two", carol) == ("invalid", None)


def test_the_owner_stays_and_deleting_a_team_keeps_the_battles(db, seeded):
    alice, bob = seeded["alice"], seeded["bob"]
    team = db.create_team("Взвод", alice)["id"]
    db.accept_invite(db.create_invite(team, alice, "c", 7, None)["code"], bob)
    assert not db.remove_member(team, alice)
    assert db.get_member_role(team, alice) == "owner"
    db.delete_team(team)
    assert db.get_team(team) is None and db.get_user_teams(bob) == []
    assert db.get_db_totals(Scope("all"))["total_battles"] == 3


def test_admin_overview_counts_the_whole_database(db, seeded):
    overview = db.get_admin_overview()
    assert (overview["battles"], overview["users"], overview["teams"]) == (3, 2, 0)
    assert sum(m["battles"] for m in overview["by_mode"]) == 3
    assert overview["by_day"][0] == {"day": "2025-01-04", "battles": 1}


# --- accounts ---

def test_token_versions_and_consent_are_stored(db):
    user = db.create_user("ivan", "hash", "2026-11-09")["id"]
    account = db.get_user_auth(user)
    assert (account["token_version"], account["consent_version"]) == (0, "2026-11-09")
    assert db.set_password(user, "hash2") == 1 and db.bump_token_version(user) == 2
    assert db.get_user_auth(user)["password_hash"] == "hash2"
    assert db.get_user_by_username("IVAN")["token_version"] == 2
    db.record_consent(user, "2027-01-01")
    assert db.get_user_auth(user)["consent_version"] == "2027-01-01"
    assert db.get_user_auth(999) is None


def test_deleting_a_user_keeps_shared_battles_and_hands_teams_over(db):
    alice = db.create_user("alice", "hash")["id"]
    bob = db.create_user("bob", "hash")["id"]
    shared = make_battle("shared", "Энск", "win", 1, [{"name": "P1"}])
    own = make_battle("own", "Энск", "win", 1, [{"name": "P1"}])
    db.save_battle(shared, user_id=alice)
    db.save_battle(own, user_id=alice)
    db.save_battle(shared, user_id=bob)
    db.hide_player(alice, "P1")
    kept = db.create_team("Взвод", alice)["id"]
    db.accept_invite(db.create_invite(kept, alice, "c", 7, None)["code"], bob)
    alone = db.create_team("Соло", alice)["id"]

    assert db.delete_user(alice) == {"battles_deleted": 1, "teams_handed_over": 1, "teams_deleted": 1}
    assert db.get_user_auth(alice) is None
    assert [b["battle_hash"] for b in db.get_battles_export(None)] == ["shared"]
    assert db.get_member_role(kept, bob) == "owner" and db.get_team(alone) is None
    assert db.get_db_totals(bob)["total_battles"] == 1
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM player_stats")
        assert cur.fetchone()["n"] == 1  # the deleted battle's stats went with it


# --- period and daily series ---

def test_a_period_narrows_every_scope(db, seeded):
    for scope in (Scope("all", date_from="2025-01-03"), Scope("mine", user_id=seeded["alice"], date_to="2025-01-02"),
                  Scope("visible", user_id=seeded["alice"], date_from="2025-01-03", date_to="2025-01-03")):
        assert db.get_db_totals(scope)["total_battles"] in (1, 2), scope
    assert db.get_db_totals(Scope("all", date_from="2025-01-03"))["total_battles"] == 2
    assert db.get_db_totals(Scope("all", date_from="2025-01-03", mode="clan"))["total_battles"] == 0


def test_daily_summary_averages_our_side_per_day(db, seeded):
    daily = db.get_daily_summary(Scope("mine", user_id=seeded["alice"]))
    first, second = daily["days"]
    assert (first["day"], first["battles"], first["wins"]) == ("2025-01-02", 1, 1)
    assert first["avg_damage"] == 2000          # P1 3000 and P2 1000; the enemy's 9999 is not ours
    assert float(first["accuracy"]) == round(11 / 16 * 100, 1)
    assert (second["losses"], second["avg_damage"]) == (1, 1500)
    assert daily["totals"]["battles"] == 2 and daily["totals"]["avg_damage"] == 1750
    db.hide_player(seeded["alice"], "P2")
    hidden = db.get_daily_summary(Scope("mine", user_id=seeded["alice"]), viewer_id=seeded["alice"])
    assert hidden["totals"]["avg_damage"] == 2000  # P1 only: 3000 and 1000
