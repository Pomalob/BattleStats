import pytest

from replay_factory import MAGIC, build_replay
from replay_parser import _vehicle_name, parse_replay, parse_replay_bytes


def test_parses_a_complete_replay():
    battle = parse_replay_bytes(build_replay(), "20250102_2015_battle.mtreplay")

    assert battle.map_name == "Прохоровка"
    assert battle.date_time == "2025-01-02 20:15:33"
    assert battle.player_team == 1
    assert battle.player_vehicle == "Object 278"
    assert battle.result == "win"
    assert len(battle.players) == 3

    alice = next(p for p in battle.players if p.name == "Alice")
    assert (alice.damage_dealt, alice.frags, alice.shots, alice.hits, alice.piercings) == (2400, 3, 10, 8, 6)
    assert alice.survived is True

    carol = next(p for p in battle.players if p.name == "Carol")
    assert carol.survived is False


@pytest.mark.parametrize("winner_team,expected", [(1, "win"), (2, "loss"), (0, "draw")])
def test_result_follows_winner_team(winner_team, expected):
    assert parse_replay_bytes(build_replay(winner_team=winner_team), "b.mtreplay").result == expected


def test_assists_sum_radio_and_track():
    data = build_replay(players=[
        {"name": "Alice", "team": 1, "assist_radio": 700, "assist_track": 300},
    ])
    assert parse_replay_bytes(data, "b.mtreplay").players[0].assists == 1000


def test_unknown_map_falls_back_to_a_cleaned_id():
    battle = parse_replay_bytes(build_replay(map_name="99_new_arena"), "b.mtreplay")
    assert battle.map_name == "New Arena"


@pytest.mark.parametrize("raw,expected", [
    ("ussr:R231_Object_278", "Object 278"),
    ("germany:G100_Maus", "Maus"),
    ("china:Ch45_WZ_111", "WZ 111"),
    ("", ""),
])
def test_vehicle_names_lose_their_nation_prefix(raw, expected):
    assert _vehicle_name(raw) == expected


def test_battle_hash_is_stable_across_replays_of_one_battle():
    """Every player records the same battle, so the hash must ignore who uploaded it."""
    roster = [
        {"name": "Alice", "team": 1, "damage": 2400},
        {"name": "Bob", "team": 1, "damage": 1200},
    ]
    a = parse_replay_bytes(build_replay(player_name="Alice", players=roster), "alice.mtreplay")
    b = parse_replay_bytes(build_replay(player_name="Bob", players=roster), "bob.mtreplay")
    assert a.battle_hash == b.battle_hash


def test_battle_hash_differs_when_damage_differs():
    a = parse_replay_bytes(build_replay(players=[{"name": "Alice", "team": 1, "damage": 100}]), "a.mtreplay")
    b = parse_replay_bytes(build_replay(players=[{"name": "Alice", "team": 1, "damage": 200}]), "b.mtreplay")
    assert a.battle_hash != b.battle_hash


def test_replay_without_results_returns_the_roster_only():
    battle = parse_replay_bytes(build_replay(with_results=False), "incomplete.mtreplay")
    assert battle.result == "draw"
    assert [p.name for p in battle.players] == ["Alice", "Bob", "Carol"]
    assert all(p.damage_dealt == 0 for p in battle.players)


def test_rejects_a_file_that_is_not_a_replay():
    with pytest.raises(ValueError, match="magic bytes"):
        parse_replay_bytes(b"PK\x03\x04 this is a zip", "notes.mtreplay")


def test_rejects_a_truncated_file():
    with pytest.raises(ValueError, match="too small|out of range"):
        parse_replay_bytes(MAGIC + b"\x02\x00\x00", "short.mtreplay")


def test_rejects_a_block_size_that_runs_past_the_end_of_the_file():
    import struct
    data = MAGIC + struct.pack("<I", 2) + struct.pack("<I", 10_000) + b"{}"
    with pytest.raises(ValueError, match="out of range"):
        parse_replay_bytes(data, "lying.mtreplay")


def test_rejects_a_block_that_is_not_json():
    import struct
    payload = b"not json at all"
    data = MAGIC + struct.pack("<I", 1) + struct.pack("<I", len(payload)) + payload
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_replay_bytes(data, "garbage.mtreplay")


def test_parse_replay_reads_from_disk(tmp_path):
    path = tmp_path / "battle.mtreplay"
    path.write_bytes(build_replay())
    assert parse_replay(path).filename == "battle.mtreplay"
