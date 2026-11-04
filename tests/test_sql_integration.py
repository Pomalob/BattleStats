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
                "TRUNCATE user_battles, hidden_players, player_stats, battles, users "
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
    rows = db.get_battles_list(user_id=seeded["bob"], viewer_id=seeded["bob"])
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
