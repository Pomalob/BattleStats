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
    with pytest.raises(ValueError, match="сигнатура"):
        parse_replay_bytes(b"PK\x03\x04 this is a zip", "notes.mtreplay")


def test_rejects_a_truncated_file():
    with pytest.raises(ValueError, match="слишком маленький|вне диапазона"):
        parse_replay_bytes(MAGIC + b"\x02\x00\x00", "short.mtreplay")


def test_rejects_a_block_size_that_runs_past_the_end_of_the_file():
    import struct
    data = MAGIC + struct.pack("<I", 2) + struct.pack("<I", 10_000) + b"{}"
    with pytest.raises(ValueError, match="вне диапазона"):
        parse_replay_bytes(data, "lying.mtreplay")


def test_rejects_a_block_that_is_not_json():
    import struct
    payload = b"not json at all"
    data = MAGIC + struct.pack("<I", 1) + struct.pack("<I", len(payload)) + payload
    with pytest.raises(ValueError, match="некорректный JSON"):
        parse_replay_bytes(data, "garbage.mtreplay")


def test_parse_replay_reads_from_disk(tmp_path):
    path = tmp_path / "battle.mtreplay"
    path.write_bytes(build_replay())
    assert parse_replay(path).filename == "battle.mtreplay"


def test_assists_include_stun_damage():
    """Artillery stun assist counts towards assist damage just like radio and track."""
    data = build_replay(players=[
        {"name": "Alice", "team": 1, "assist_radio": 300, "assist_track": 200, "assist_stun": 500},
    ])
    assert parse_replay_bytes(data, "b.mtreplay").players[0].assists == 1000


def test_a_finished_replay_is_marked_complete():
    assert parse_replay_bytes(build_replay(), "b.mtreplay").complete is True


def test_a_replay_without_results_is_marked_incomplete():
    assert parse_replay_bytes(build_replay(with_results=False), "b.mtreplay").complete is False


def test_team_is_recovered_when_meta_hides_the_player_name():
    """The anonymiser rewrites names in meta, so the team has to come from the results."""
    data = build_replay(player_name="Alice", anonymise_meta=True, winner_team=2,
                        players=[
                            {"name": "Alice", "team": 2, "damage": 1000},
                            {"name": "Enemy", "team": 1, "damage": 500},
                        ])
    battle = parse_replay_bytes(data, "b.mtreplay")
    assert battle.player_team == 2
    assert battle.result == "win"


def test_a_replay_with_no_identifiable_player_is_rejected():
    """Better a named error than a battle silently filed as a loss for team 0."""
    data = build_replay(player_name="Ghost", anonymise_meta=True, drop_personal=True,
                        players=[{"name": "Alice", "team": 1, "damage": 100}])
    with pytest.raises(ValueError, match="команду игрока"):
        parse_replay_bytes(data, "b.mtreplay")


def test_the_games_own_map_name_wins_over_the_table():
    """Replays carry mapDisplayName; the built-in table is only for replays without one."""
    data = build_replay(map_name="114_czech", map_display_name="Промзона")
    assert parse_replay_bytes(data, "b.mtreplay").map_name == "Промзона"


def test_a_map_missing_from_the_table_still_gets_its_real_name():
    data = build_replay(map_name="777_brand_new", map_display_name="Новая карта")
    assert parse_replay_bytes(data, "b.mtreplay").map_name == "Новая карта"


def test_replay_dates_are_rewritten_so_they_sort_chronologically():
    """Replays write DD.MM.YYYY, which as text puts 31 August after 8 September."""
    august = parse_replay_bytes(build_replay(date_time="31.08.2026 07:17:59"), "a.mtreplay")
    september = parse_replay_bytes(build_replay(date_time="08.09.2026 02:03:59"), "b.mtreplay")
    assert august.date_time == "2026-08-31 07:17:59"
    assert september.date_time == "2026-09-08 02:03:59"
    assert august.date_time < september.date_time


def test_an_unrecognised_date_is_left_alone():
    assert parse_replay_bytes(build_replay(date_time="whenever"), "b.mtreplay").date_time == "whenever"


def test_the_real_nickname_beats_the_anonymised_one():
    """Enemy names in the results block are fake; realName is the one that is stable."""
    data = build_replay(
        player_name="FakeName",
        players=[{"name": "FakeName", "team": 1, "damage": 100}],
        real_names={"FakeName": "TrueName"},
    )
    assert parse_replay_bytes(data, "b.mtreplay").players[0].name == "TrueName"


@pytest.mark.parametrize("raw,expected", [
    ("germany-G89_Leopard1", "Leopard1"),
    ("ussr:R43_IS-7", "IS-7"),
    ("IS-7", "IS-7"),
])
def test_vehicle_names_survive_both_separators_and_their_own_hyphens(raw, expected):
    assert _vehicle_name(raw) == expected


def test_extended_details_keep_the_chosen_fields_and_drop_the_rest():
    battle = parse_replay_bytes(
        build_replay(real_names={"Alice": "Alice"}, players=[
            {"name": "Alice", "team": 1, "damage": 2400, "sniper_damage": 700, "platoon": 55},
            {"name": "Carol", "team": 2, "damage": 900, "killer_id": 1, "death_reason": 0},
        ]),
        "b.mtreplay",
    )
    assert battle.details["arenaUniqueID"] == 7226559141569606
    assert battle.details["duration"] == 293
    assert battle.details["teamHealth"] == {"1": 30000, "2": 31000}
    assert "serverSettings" not in battle.details
    assert battle.recorder == {"playerName": "Alice", "playerID": 7, "hasMods": True}

    alice = next(p for p in battle.players if p.name == "Alice")
    assert alice.details["sniperDamageDealt"] == 700
    assert alice.details["prebattleID"] == 55
    assert alice.details["clanDBID"] == 589477
    assert alice.details["vehicleID"] == 1
    # Economy and game-mode counters are not part of the export.
    assert "credits" not in alice.details and "winPoints" not in alice.details

    carol = next(p for p in battle.players if p.name == "Carol")
    assert carol.details["killerID"] == alice.details["vehicleID"]


def test_an_unfinished_replay_still_carries_battle_and_recorder_details():
    battle = parse_replay_bytes(build_replay(with_results=False), "b.mtreplay")
    assert battle.details["serverName"] == "RU4"
    assert battle.recorder["playerID"] == 7
