"""scripts/export_to_table.py --refs: the player and vehicle reference book."""
import csv
import importlib.util
import json
from pathlib import Path

import pytest

pytest.importorskip("openpyxl")
from openpyxl import load_workbook  # noqa: E402

from replay_factory import build_replay  # noqa: E402

ROOT = Path(__file__).parent.parent


def _script():
    spec = importlib.util.spec_from_file_location("export_to_table", ROOT / "scripts" / "export_to_table.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _export(client, auth):
    headers = auth()
    # real_names adds the results "players" block, which carries every player's clanDBID
    files = {"files": ("a.mtreplay", build_replay(real_names={"Alice": "Alice"}), "application/octet-stream")}
    battle = client.post("/api/upload", files=files, headers=headers).json()["battles"][0]
    client.post("/api/battles/save", json={"battles": [battle]}, headers=headers)
    return client.get("/api/export", headers=headers).json()


def _fake_lesta(calls):
    """Stands in for the Lesta Games API: one vehicle, every asked account."""
    def get(method, params):
        calls.append(method)
        if method == "encyclopedia/vehicles":
            return {"status": "ok", "meta": {"page_total": 1}, "data": {
                "7937": {"tank_id": 7937, "name": "Объект 278", "tag": "R231_Object_278", "type": "heavyTank",
                         "tier": 10, "is_premium": False, "is_wheeled": False, "default_profile": {"hp": 2400}},
            }}
        if method == "clans/info":
            return {"status": "ok", "data": {i: {"name": "Тестовый клан", "members_count": 42,
                                                 "created_at": 1_400_000_000}
                                             for i in params["clan_id"].split(",")}}
        if method == "stronghold/claninfo":
            return {"status": "ok", "data": {i: {"stronghold_level": 10,
                                                 "skirmish_statistics": {"win_10": 60, "total_10": 90},
                                                 "battles_for_strongholds_statistics": {"win_10": 10, "total_10": 10}}
                                             for i in params["clan_id"].split(",")}}
        ids = params["account_id"].split(",")
        return {"status": "ok", "data": {i: {"created_at": 1_400_000_000, "global_rating": 5000,
                                             "statistics": {"all": {"battles": 2000, "wins": 1100}}}
                                         for i in ids}}
    return get


def _run(script, monkeypatch, *argv):
    monkeypatch.setattr("sys.argv", ["export_to_table.py", *map(str, argv)])
    script.main()


def test_refs_fill_api_columns_and_keep_hand_filled_ones(client, auth, fake_db, tmp_path, monkeypatch):
    source = tmp_path / "battlestats.json"
    source.write_text(json.dumps(_export(client, auth), ensure_ascii=False), encoding="utf-8")
    script = _script()
    calls = []
    monkeypatch.setattr(script, "_lesta_get", _fake_lesta(calls))
    monkeypatch.setenv("LESTA_APP_ID", "test")
    _run(script, monkeypatch, source, "--refs")

    book = tmp_path / "battlestats_refs.xlsx"
    wb = load_workbook(book)
    tanks, players = wb["Танки"], wb["Игроки"]
    header = [c.value for c in tanks[1]]
    tank = dict(zip(header, [c.value for c in tanks[2]]))
    assert tank["Класс"] == "ТТ" and tank["Уровень"] == 10 and tank["Нация"] in script.NATIONS.values()
    p_header = [c.value for c in players[1]]
    player = dict(zip(p_header, [c.value for c in players[2]]))
    assert player["Процент побед, %"] == 55.0 and player["Аккаунт создан"] == "2014-05-13"
    assert player["Боёв в наших реплеях"] == 1
    clans = wb["Кланы"]
    clan = dict(zip([c.value for c in clans[1]], [c.value for c in clans[2]]))
    # 60 + 10 wins of 90 + 10 Stronghold battles on tier X
    assert clan["Боёв в УР (X)"] == 100 and clan["% побед в УР (X)"] == 70.0
    assert clan["Сила клана"] == "4. Топ (65%+)" and clan["Игроков в наших реплеях"] == 3

    # fill the hand columns, then update again: they must survive
    tanks.cell(row=2, column=header.index("Роль") + 1, value="Штурмовой ТТ")
    clans.cell(row=2, column=[c.value for c in clans[1]].index("Комментарий") + 1, value="соседи по УР")
    wb.save(book)
    _run(script, monkeypatch, source, "--refs")
    wb = load_workbook(book)
    tank = dict(zip(header, [c.value for c in wb["Танки"][2]]))
    player = dict(zip(p_header, [c.value for c in wb["Игроки"][2]]))
    clan = dict(zip([c.value for c in wb["Кланы"][1]], [c.value for c in wb["Кланы"][2]]))
    assert tank["Роль"] == "Штурмовой ТТ" and clan["Комментарий"] == "соседи по УР"
    assert tank["Класс"] == "ТТ"
    # name, age and city are entered in Foresight now, not in this book
    assert "Имя" not in p_header and "Возраст" not in p_header

    # --bi carries the attributes into the player rows and writes the two reference tables
    monkeypatch.delenv("LESTA_APP_ID")
    _run(script, monkeypatch, source, "--bi")
    rows = list(csv.DictReader((tmp_path / "battlestats_bi_players.csv").read_text(encoding="utf-8").splitlines(),
                               delimiter=";"))
    first = next(r for r in rows if r["accountDBID"] == str(player["ID аккаунта"]))
    assert first["vehicle_class"] == "ТТ" and first["vehicle_tier"] == "10" and first["vehicle_role"] == "Штурмовой ТТ"
    # name and age are attributes of the MDM dictionary of players in Foresight, not CSV columns
    assert "player_age" not in first and "player_real_name" not in first
    assert first["clan_strength"] == "4. Топ (65%+)" and first["clan_ur_winrate"] == "70.0"
    assert first["player_strength"] == "4. Сильный (53–56%)" and first["player_winrate"] == "55.0"
    assert (tmp_path / "battlestats_bi_player_refs.csv").exists()
    assert (tmp_path / "battlestats_bi_vehicle_refs.csv").exists()
    assert (tmp_path / "battlestats_bi_clan_refs.csv").exists()


def test_refs_without_an_api_key_still_list_players_and_vehicles(client, auth, fake_db, tmp_path, monkeypatch):
    source = tmp_path / "battlestats.json"
    source.write_text(json.dumps(_export(client, auth), ensure_ascii=False), encoding="utf-8")
    script = _script()
    monkeypatch.setattr(script, "_lesta_get", lambda *a: pytest.fail("API called without a key"))
    monkeypatch.delenv("LESTA_APP_ID", raising=False)
    _run(script, monkeypatch, source, "--refs")
    wb = load_workbook(tmp_path / "battlestats_refs.xlsx")
    assert wb["Игроки"].max_row > 1 and wb["Танки"].max_row == 2
    assert wb["Танки"]["D2"].value in script.NATIONS.values()  # nation comes from the replay's vehicleType


def test_bi_without_a_reference_book_marks_attributes_as_not_set(client, auth, fake_db, tmp_path, monkeypatch):
    source = tmp_path / "battlestats.json"
    source.write_text(json.dumps(_export(client, auth), ensure_ascii=False), encoding="utf-8")
    script = _script()
    _run(script, monkeypatch, source, "--bi")
    rows = list(csv.reader((tmp_path / "battlestats_bi_players.csv").read_text(encoding="utf-8").splitlines(),
                           delimiter=";"))
    enrich = dict(zip(rows[0][-len(script.BI_ENRICH_COLUMNS):], rows[1][-len(script.BI_ENRICH_COLUMNS):]))
    assert list(enrich) == list(script.BI_ENRICH_COLUMNS)
    # cube dimensions get an explicit "not set" element instead of an empty key that drops the row
    assert enrich["vehicle_class"] == enrich["vehicle_role"] == enrich["vehicle_nation"] == script.NOT_SET
    assert enrich["vehicle_tier"] == "0" and enrich["vehicle_premium"] == ""


def test_strength_groups():
    script = _script()
    assert script.clan_strength(70, 20).startswith("0. Мало боёв")       # too few Stronghold battles
    assert script.clan_strength(None, None) == "0. Нет данных УР"
    assert script.clan_strength(44.9, 500) == "1. Слабый (до 45%)"
    assert script.clan_strength(65, 500) == "4. Топ (65%+)"
    assert script.player_strength(60, 300).startswith("0. Мало боёв")    # twinks and newcomers
    assert script.player_strength(52, 5000) == "3. Хороший (50–53%)"
    assert script.player_strength(None, None) is None
    assert script.enemy_strength(49.0) == "2. Средние (48–50%)"


def test_enemy_strength_is_the_other_teams_mean_of_meaningful_win_rates():
    script = _script()
    players = {i: {"battles_total": 5000, "winrate_total": 46 + i} for i in range(1, 11)}
    players[11] = {"battles_total": 50, "winrate_total": 90}           # a twink: ignored
    rows = [{"battle_id": 1, "team": 1 if i <= 5 else 2, "accountDBID": i} for i in range(1, 12)]
    rows[-1]["team"] = 2
    script.enrich_players(rows, {"Игроки": players, "Танки": {}, "Кланы": {}})
    team1 = next(r for r in rows if r["team"] == 1)
    team2 = next(r for r in rows if r["team"] == 2)
    assert team1["enemy_avg_winrate"] == 54.0      # players 6..10: 52..56, the twink left out
    assert team1["enemy_strength"] == "4. Очень сильные (52%+)"
    assert team2["enemy_avg_winrate"] == 49.0      # players 1..5: 47..51
    assert team2["clan_tag"] == team2["clan_strength"] == script.NO_CLAN
