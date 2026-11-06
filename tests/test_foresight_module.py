"""The flat tables are described three times: src/app.py (site), scripts/export_to_table.py --bi
(CSV from a JSON file) and integrations/foresight/BattleStats.fore (tables in Foresight).
These tests keep the three in step, since nothing else would notice a drift until a load fails."""
import csv
import importlib.util
import re
from pathlib import Path

import pytest

from app import FLAT_BATTLE_COLUMNS, FLAT_PLAYER_COLUMNS, flatten_export
from replay_factory import build_replay

ROOT = Path(__file__).parent.parent
FORE = (ROOT / "integrations" / "foresight" / "BattleStats.fore").read_text(encoding="utf-8")


def _script():
    spec = importlib.util.spec_from_file_location("export_to_table", ROOT / "scripts" / "export_to_table.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fore_function(name: str) -> str:
    """The string a `Function <name>: String` returns: its literals glued together."""
    body = re.search(rf"Function {name}: String;\s*Begin\s*Return(.*?);\s*End Function", FORE, re.S)
    assert body, name
    return "".join(re.findall(r'"((?:[^"]|"")*)"', body.group(1)))


def _fore_columns(name: str) -> list[tuple[str, str]]:
    return [tuple(c.split(":")) for c in _fore_function(name).split(";")]


@pytest.mark.parametrize("function, columns", [
    ("BattleColumns", FLAT_BATTLE_COLUMNS),
    ("PlayerColumns", FLAT_PLAYER_COLUMNS),
])
def test_fore_tables_have_exactly_the_site_columns(function, columns):
    fore = _fore_columns(function)
    assert [name for name, _ in fore] == list(columns)
    assert all(re.fullmatch(r"I|F|S\d+", kind) for _, kind in fore)
    # Foresight and the databases under it cap identifiers at 30 characters
    assert all(len(name) <= 30 for name, _ in fore)


def test_cube_facts_and_dimensions_point_at_real_player_fields():
    player_fields = {name.upper() for name, _ in _fore_columns("PlayerColumns")}
    facts = [f.split("|") for f in _fore_function("FactColumns").split(";")]
    for name, aggregation, target in facts:
        if aggregation == "CALC":
            assert all(1 <= int(k) <= len(facts) and facts[int(k) - 1][1] != "CALC"
                       for k in re.findall(r"@\[(\d+)\]", target)), name
        else:
            assert aggregation in ("SUM", "COUNT") and target in player_fields, name
    for table, dictionary, name, field, key_type in (d.split("|") for d in _fore_function("DimColumns").split(";")):
        assert field in player_fields and key_type in ("S", "I"), name


def test_script_bi_columns_match_the_site():
    script = _script()
    assert script.BI_BATTLE_COLUMNS == FLAT_BATTLE_COLUMNS
    assert script.BI_PLAYER_COLUMNS == FLAT_PLAYER_COLUMNS


def _export(client, auth):
    headers = auth()
    for day, winner in (("01", 1), ("02", 2)):
        replay = build_replay(date_time=f"{day}.01.2025 10:00:00", winner_team=winner,
                              map_name=f"0{day}_map")
        files = {"files": ("a.mtreplay", replay, "application/octet-stream")}
        battle = client.post("/api/upload", files=files, headers=headers).json()["battles"][0]
        client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    export = client.get("/api/export", headers=headers).json()
    assert export["total_battles"] == 2
    return export


def test_script_bi_rows_match_the_site(client, auth, fake_db):
    export = _export(client, auth)
    script = _script()
    for kind in ("battles", "players"):
        assert script.flatten_bi(export["battles"], kind) == flatten_export(export["battles"], kind)


def test_script_bi_writes_csv_foresight_can_read(client, auth, fake_db, tmp_path, monkeypatch):
    export = _export(client, auth)
    source = tmp_path / "battlestats.json"
    import json
    source.write_text(json.dumps(export, ensure_ascii=False), encoding="utf-8")
    script = _script()
    monkeypatch.setattr("sys.argv", ["export_to_table.py", str(source), "--bi"])
    script.main()

    players = (tmp_path / "battlestats_bi_players.csv").read_bytes()
    assert not players.startswith(b"\xef\xbb\xbf")  # a BOM would stick to the first column name
    rows = list(csv.reader(players.decode("utf-8").splitlines(), delimiter=";"))
    assert rows[0] == list(FLAT_PLAYER_COLUMNS)
    assert len(rows) - 1 == len(flatten_export(export["battles"], "players"))
    battles = list(csv.reader((tmp_path / "battlestats_bi_battles.csv").read_text(encoding="utf-8").splitlines(),
                              delimiter=";"))
    assert battles[0] == list(FLAT_BATTLE_COLUMNS) and len(battles) - 1 == export["total_battles"]
