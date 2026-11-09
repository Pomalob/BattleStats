"""Turn a /api/export JSON file into tables: one row per battle, one row per player in a battle.

Usage:
    python scripts/export_to_table.py                                  # download from the site -> battlestats.json + .xlsx
    python scripts/export_to_table.py --all --date-from 2026-09-01     # every battle, from that day on
    python scripts/export_to_table.py battlestats.json                 # local file -> battlestats.xlsx
    python scripts/export_to_table.py battlestats.json -o stats.csv    # -> stats_battles.csv, stats_players.csv
    python scripts/export_to_table.py battlestats.json --bi            # -> battlestats_bi_battles.csv, ..._bi_players.csv
    python scripts/export_to_table.py battlestats.json --refs          # -> battlestats_refs.xlsx: players and vehicles

--refs creates or updates an Excel reference book next to the JSON: a sheet of players
(account age, rating, battles, win rate from the Lesta Games API; name, age and city are entered
in Foresight instead), a sheet of vehicles (class, tier, premium, role from game_vehicles.json, which
scripts/game_vehicles.py writes on the machine with the game; no key needed) and a sheet of
clans (Stronghold results from the API). Hand-
filled columns (yellow headers) survive every update. The API needs LESTA_APP_ID, an
application_id from https://developers.lesta.ru/applications/ ; without it only what the
replays and the game client know is filled in. --bi then adds these attributes to the CSV for Foresight.

Without a file the export is downloaded from the BattleStats site and saved as battlestats.json:
    BATTLESTATS_URL       site address, https://battlestats.relaxdev.ru by default
    BATTLESTATS_USER      login on the site; asked for if not set
    BATTLESTATS_PASSWORD  password; asked for if not set

Needs nothing beyond the standard library for CSV; .xlsx needs `python -m pip install openpyxl`.
CSV is written with ";" and a UTF-8 BOM, so Excel with a Russian locale opens it as is.
"""
import argparse
import csv
import getpass
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

# Battle columns that come first and get readable headers; the rest of `details` follows.
BATTLE_COLUMNS = [
    ("id", "ID боя"),
    ("date_time", "Дата и время"),
    ("map_name", "Карта"),
    ("result", "Результат"),
    ("mine", "Мой бой"),
    ("player_team", "Моя команда"),
    ("winner_team", "Победила команда"),
    ("duration", "Длительность, с"),
    ("gameplayID", "Режим"),
    ("bonusType", "Тип боя"),
    ("finishReason", "Причина окончания"),
    ("serverName", "Сервер"),
    ("arenaUniqueID", "arenaUniqueID"),
    ("playerName", "Записал"),
    ("playerVehicle", "Танк записавшего"),
    ("filename", "Файл"),
]

PLAYER_COLUMNS = [
    ("name", "Игрок"),
    ("vehicle", "Танк"),
    ("team", "Команда"),
    ("ally", "Союзник"),
    ("damage_dealt", "Урон"),
    ("assists", "Ассист"),
    ("blocked", "Заблокировано"),
    ("damage_received", "Получено урона"),
    ("frags", "Фраги"),
    ("survived", "Выжил"),
    ("shots", "Выстрелы"),
    ("hits", "Попадания"),
    ("piercings", "Пробития"),
    ("accuracy", "Точность, %"),
    ("pen_rate", "Пробития от попаданий, %"),
    ("xp", "Опыт"),
]

# Shared battle fields repeated on every player row, so the sheet filters on its own.
PLAYER_BATTLE_COLUMNS = [
    ("battle_id", "ID боя"),
    ("date_time", "Дата и время"),
    ("map_name", "Карта"),
    ("result", "Результат"),
    ("mine", "Мой бой"),
]

# Order of the extended player fields after the readable columns: who, then damage,
# assist, shooting, shots taken, spotting, death. Anything not listed goes last.
PLAYER_DETAIL_ORDER = [
    "realName", "fakeName", "accountDBID", "clanAbbrev", "clanDBID", "prebattleID",
    "vehicleType", "typeCompDescr", "vehicleID", "maxHealth",
    "sniperDamageDealt", "damageReceivedFromInvisibles", "potentialDamageReceived",
    "tdamageDealt", "equipmentDamageDealt", "artilleryFortEquipDamageDealt",
    "damageAssistedRadio", "damageAssistedTrack", "damageAssistedStun",
    "damageAssistedSmoke", "damageAssistedInspire",
    "directEnemyHits", "directTeamHits", "piercingEnemyHits", "explosionHits",
    "directHitsReceived", "piercingsReceived", "noDamageDirectHitsReceived", "explosionHitsReceived",
    "spotted", "damaged", "damagedHp", "tkills", "tdestroyedModules", "isFirstBlood",
    "stunNum", "stunDuration", "stunned",
    "deathReason", "killerID", "lifeTime", "health", "mileage", "deathCount", "isAlive",
    "capturePoints", "droppedCapturePoints", "capturingBase",
]

# Replay fields that only repeat a readable column ("kills" is "Фраги", and so on).
# The roster `name` can be the anonymiser's fake one; the real nickname is in "Игрок".
PLAYER_DUPLICATES = {
    "name", "team", "damageDealt", "damageReceived", "damageBlockedByArmor",
    "kills", "shots", "directHits", "piercings",
}
BATTLE_DUPLICATES = {"winnerTeam", "mapDisplayName"}

RESULTS = {"win": "победа", "loss": "поражение", "draw": "ничья"}

# --bi: the same flat tables as GET /api/export/table/{battles,players} on the site, for loading
# into Foresight (integrations/foresight/BattleStats.fore) or another BI tool. Raw column names,
# 0/1 for booleans, one type per column. Kept identical to src/app.py by tests/test_export_script.py.
BI_BATTLE_COLUMNS = (
    'id', 'day', 'date_time', 'map_name', 'result', 'mine', 'player_team', 'winner_team',
    'battle_hash', 'filename', 'playerName', 'playerID', 'playerVehicle', 'hasMods',
    'clientVersionFromExe', 'arenaUniqueID', 'mapName', 'gameplayID', 'battleType',
    'serverName', 'regionCode', 'winnerTeam', 'finishReason', 'duration', 'arenaCreateTime',
    'arenaTypeID', 'bonusType', 'guiType', 'teamHealth', 'vehLockMode', 'finishAllPlayersLeft',
    'division', 'accountCompDescr', 'gasAttackWinnerTeam', 'commonNumStarted',
    'commonNumCaptured', 'commonNumDefended', 'commonNumDestroyed',
)
BI_PLAYER_COLUMNS = (
    'battle_id', 'day', 'date_time', 'map_name', 'result', 'outcome', 'win', 'mine', 'ally',
    'name', 'vehicle', 'team', 'damage_dealt', 'damage_received', 'blocked', 'assists', 'frags',
    'xp', 'survived', 'shots', 'hits', 'piercings', 'vehicleID', 'realName', 'clanDBID',
    'prebattleID', 'fakeName', 'vehicleType', 'maxHealth', 'isAlive', 'isTeamKiller', 'wtr',
    'igrType', 'avatarSessionID', 'badges', 'overriddenBadge', 'ranked', 'selectedComp7Skill',
    'customRoleSlotTypeId', 'vehPostProgression', 'personalMissionIDs', 'statTrackFrags',
    'botDisplayStatus', 'forbidInBattleInvitations', 'clanAbbrev', 'accountDBID',
    'typeCompDescr', 'damageDealt', 'sniperDamageDealt', 'damageReceived',
    'damageReceivedFromInvisibles', 'potentialDamageReceived', 'damageBlockedByArmor',
    'tdamageDealt', 'equipmentDamageDealt', 'artilleryFortEquipDamageDealt',
    'damageAssistedRadio', 'damageAssistedTrack', 'damageAssistedStun', 'damageAssistedSmoke',
    'damageAssistedInspire', 'directHits', 'directEnemyHits', 'directTeamHits',
    'piercingEnemyHits', 'explosionHits', 'directHitsReceived', 'piercingsReceived',
    'noDamageDirectHitsReceived', 'explosionHitsReceived', 'spotted', 'damaged', 'damagedHp',
    'kills', 'tkills', 'tdestroyedModules', 'isFirstBlood', 'stunNum', 'stunDuration',
    'stunned', 'deathReason', 'killerID', 'lifeTime', 'health', 'mileage', 'deathCount',
    'capturePoints', 'droppedCapturePoints', 'capturingBase',
)
_BI_TEXT_COLUMNS = {"arenaUniqueID"}


def _bi_value(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and abs(value) >= 2 ** 53:
        return str(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _bi_row(columns, *sources):
    merged = {}
    for source in reversed(sources):  # the first source wins on a clash
        merged.update(source or {})
    row = {c: _bi_value(merged.get(c)) for c in columns}
    for c in _BI_TEXT_COLUMNS.intersection(row):
        if row[c] is not None:
            row[c] = str(row[c])
    return row


def _bi_outcome(team, winner_team):
    if not winner_team:
        return "draw" if winner_team == 0 else None
    return "win" if team == winner_team else "loss"


def flatten_bi(battles: list[dict], kind: str) -> list[dict]:
    """Same rows as src/app.py flatten_export."""
    if kind == "battles":
        return [_bi_row(BI_BATTLE_COLUMNS, {"day": (b.get("date_time") or "")[:10] or None},
                        b, b.get("recorder"), b.get("details"))
                for b in battles]
    rows = []
    for b in battles:
        shared = {"battle_id": b["id"], "day": (b.get("date_time") or "")[:10] or None,
                  "date_time": b.get("date_time"), "map_name": b.get("map_name"),
                  "result": b.get("result"), "mine": b.get("mine")}
        for p in b.get("players", []):
            outcome = _bi_outcome(p.get("team"), b.get("winner_team"))
            own = {"ally": p.get("team") == b.get("player_team"), "outcome": outcome,
                   "win": None if outcome is None else outcome == "win"}
            rows.append(_bi_row(BI_PLAYER_COLUMNS, shared, own, p, p.get("details")))
    return rows


def write_bi_csv(path: Path, columns, rows: list[dict]) -> None:
    # No BOM: Foresight's text import would read it as part of the first column name.
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(columns)
        writer.writerows([["" if r[c] is None else r[c] for c in columns] for r in rows])


def _cell(value):
    """A value as a spreadsheet can hold it without losing anything."""
    if isinstance(value, bool):
        return "да" if value else "нет"
    # Excel keeps 15 significant digits, so a 17-digit arenaUniqueID would come back
    # rounded and no longer match the battle; such IDs go in as text.
    if isinstance(value, int) and abs(value) >= 10 ** 15:
        return str(value)
    # Lists and maps (badges, teamHealth) go into one cell as compact JSON.
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _percent(part, whole):
    return round(part / whole * 100, 1) if whole else None


def _extra_keys(rows: list[dict], known: set, order: list[str] = ()) -> list[str]:
    """Every key seen in any row that has no column yet: listed ones first, the rest A-Z."""
    seen = {key for row in rows for key in row if key not in known}
    listed = [k for k in order if k in seen]
    return listed + sorted(seen.difference(listed), key=str.lower)


def build_tables(export: dict) -> tuple[list[str], list[list], list[str], list[list]]:
    battle_rows, player_rows = [], []
    for battle in export.get("battles", []):
        details = battle.get("details") or {}
        recorder = battle.get("recorder") or {}
        result = RESULTS.get(battle.get("result"), battle.get("result"))
        battle_rows.append({**details, **recorder, **{k: v for k, v in battle.items()
                                                      if k not in ("details", "recorder", "players")},
                            "result": result})
        for p in battle.get("players", []):
            player_rows.append({
                **(p.get("details") or {}),
                **{k: v for k, v in p.items() if k != "details"},
                "battle_id": battle.get("id"),
                "date_time": battle.get("date_time"),
                "map_name": battle.get("map_name"),
                "result": result,
                "mine": battle.get("mine"),
                "ally": p.get("team") == battle.get("player_team"),
                "accuracy": _percent(p.get("hits") or 0, p.get("shots") or 0),
                "pen_rate": _percent(p.get("piercings") or 0, p.get("hits") or 0),
            })

    battle_known = {k for k, _ in BATTLE_COLUMNS} | BATTLE_DUPLICATES
    battle_cols = BATTLE_COLUMNS + [(k, k) for k in _extra_keys(battle_rows, battle_known)]
    player_known = PLAYER_BATTLE_COLUMNS + PLAYER_COLUMNS
    player_cols = player_known + [
        (k, k) for k in _extra_keys(player_rows, {k for k, _ in player_known} | PLAYER_DUPLICATES,
                                    PLAYER_DETAIL_ORDER)
    ]

    def table(rows, cols):
        return [title for _, title in cols], [[_cell(r.get(k)) for k, _ in cols] for r in rows]

    return (*table(battle_rows, battle_cols), *table(player_rows, player_cols))


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)


def write_xlsx(path: Path, sheets: list[tuple[str, list[str], list[list]]]) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    wb = Workbook(write_only=False)
    wb.remove(wb.active)
    for title, header, rows in sheets:
        ws = wb.create_sheet(title)
        ws.append(header)
        for row in rows:
            ws.append(row)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        ws.freeze_panes = "A2"
        if rows:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(header))}{len(rows) + 1}"
        for i, name in enumerate(header, start=1):
            ws.column_dimensions[get_column_letter(i)].width = min(max(len(str(name)) + 2, 10), 40)
    wb.save(path)


SITE_URL = "https://battlestats.relaxdev.ru"


def _http(url: str, data: bytes | None = None, headers: dict | None = None) -> tuple[int, dict, bytes]:
    """(status, headers, body) of one request; HTTP errors come back as a status, not an exception."""
    req = urllib.request.Request(url, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()
    except urllib.error.URLError as e:
        sys.exit(f"Не удалось подключиться к {url}: {e.reason}")


def _site_error(body: bytes) -> str:
    try:
        return json.loads(body).get("detail") or ""
    except (ValueError, AttributeError):
        return ""


def load_from_site(mine: bool, date_from: str | None, date_to: str | None) -> tuple[bytes, str]:
    """Log in to the BattleStats site and download /api/export. Returns the JSON and its file name."""
    url = (os.environ.get("BATTLESTATS_URL") or SITE_URL).rstrip("/")
    user = os.environ.get("BATTLESTATS_USER") or input(f"Логин на {url}: ").strip()
    password = os.environ.get("BATTLESTATS_PASSWORD") or getpass.getpass(f"Пароль для {user}: ")

    status, _, body = _http(f"{url}/api/auth/login",
                            json.dumps({"username": user, "password": password}).encode(),
                            {"Content-Type": "application/json"})
    if status != 200:
        sys.exit(f"Вход на {url} не удался ({status}): {_site_error(body) or 'неизвестная ошибка'}")
    token = json.loads(body)["token"]

    query = {"mine": "true" if mine else "false"}
    query.update({k: v for k, v in (("date_from", date_from), ("date_to", date_to)) if v})
    print(f"Скачиваю выгрузку с {url} ...")
    status, headers, body = _http(f"{url}/api/export?{urlencode(query)}",
                                  headers={"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"})
    if status != 200:
        sys.exit(f"Сайт не отдал выгрузку ({status}): {_site_error(body) or 'неизвестная ошибка'}")
    headers = {k.lower(): v for k, v in headers.items()}
    if headers.get("content-encoding") == "gzip":  # about 2 MB over the wire instead of 24
        body = gzip.decompress(body)
    name = re.search(r'filename="([^"]+)"', headers.get("content-disposition", ""))
    return body, name.group(1) if name else "battlestats.json"


# --- Reference book: player and vehicle attributes the replays do not have -----------------

LESTA_API = "https://api.tanki.su/wot"
VEHICLE_CLASSES = {"heavyTank": "ТТ", "mediumTank": "СТ", "lightTank": "ЛТ", "AT-SPG": "ПТ-САУ", "SPG": "САУ"}
NATIONS = {
    "ussr": "СССР", "germany": "Германия", "usa": "США", "china": "Китай", "france": "Франция",
    "uk": "Великобритания", "japan": "Япония", "czech": "Чехословакия", "sweden": "Швеция",
    "poland": "Польша", "italy": "Италия", "intunion": "Сборная нация",
}

# (key, header, source): "replay" - from the export, "api" - Lesta Games API, "manual" - by hand.
PLAYER_REF_COLUMNS = [
    ("account_id", "ID аккаунта", "replay"),
    ("nickname", "Ник", "replay"),
    ("clan", "Клан", "replay"),
    ("battles_seen", "Боёв в наших реплеях", "replay"),
    ("created_at", "Аккаунт создан", "api"),
    ("account_age_days", "Возраст аккаунта, дней", "api"),
    ("global_rating", "Личный рейтинг", "api"),
    ("battles_total", "Боёв всего", "api"),
    ("winrate_total", "Процент побед, %", "api"),
    ("last_battle", "Последний бой", "api"),
    # Name, age, city and comment are entered in Foresight (a registry form over an MDM dictionary,
    # see integrations/foresight), not here: there they are live attributes of the player dimension.
]
VEHICLE_REF_COLUMNS = [
    ("tank_id", "ID танка", "replay"),
    ("vehicle", "Танк", "replay"),
    ("tag", "Тег", "replay"),
    ("nation", "Нация", "replay"),
    ("api_name", "Название в танкопедии", "api"),
    ("vehicle_class", "Класс", "api"),
    ("tier", "Уровень", "api"),
    ("is_premium", "Премиум", "api"),
    ("is_wheeled", "Колёсный", "api"),
    ("hp", "Прочность", "api"),
    ("role", "Роль", "manual_auto"),  # from the game client; a hand-filled value wins
    ("comment", "Комментарий", "manual"),
]
# Clan strength is judged by Stronghold (Укрепрайон) results on tier X - skirmishes plus
# battles for the stronghold - not by its members' averages, which twinks and newcomers skew.
CLAN_REF_COLUMNS = [
    ("clan_id", "ID клана", "replay"),
    ("tag", "Тег", "replay"),
    ("players_seen", "Игроков в наших реплеях", "replay"),
    ("name", "Название", "api"),
    ("members", "Участников", "api"),
    ("created_at", "Клан создан", "api"),
    ("stronghold_level", "Уровень Укрепрайона", "api"),
    ("ur_battles", "Боёв в УР (X)", "api"),
    ("ur_winrate", "% побед в УР (X)", "api"),
    ("ur_battles_28d", "Боёв в УР за 28 дней", "api"),
    ("ur_winrate_28d", "% побед в УР за 28 дней", "api"),
    ("strength", "Сила клана", "api"),
    ("comment", "Комментарий", "manual"),
]
REF_SHEETS = (
    ("Игроки", "account_id", PLAYER_REF_COLUMNS),
    ("Танки", "tank_id", VEHICLE_REF_COLUMNS),
    ("Кланы", "clan_id", CLAN_REF_COLUMNS),
)
REF_CSV_NAMES = {"Игроки": "player_refs", "Танки": "vehicle_refs", "Кланы": "clan_refs"}

# Columns --bi adds to every player row from the reference book (empty without one).
BI_ENRICH_COLUMNS = (
    "vehicle_class", "vehicle_tier", "vehicle_nation", "vehicle_role", "vehicle_premium",
    "clan_tag", "clan_strength", "clan_ur_winrate",
    "player_strength", "player_winrate", "enemy_avg_winrate", "enemy_strength",
    # the day as a date (ГГГГ-ММ-ДД) for a Foresight calendar dictionary; "day" itself is text
    "battle_date",
)

# Strength groups. The numbers in front keep them in order in a dictionary sorted by name.
NO_CLAN = "Без клана"
CLAN_MIN_UR_BATTLES = 50
PLAYER_MIN_BATTLES = 1000   # below this the win rate says little: twinks, newcomers
ENEMY_MIN_KNOWN = 5         # enemies with a meaningful win rate needed to judge a team


def clan_strength(winrate, battles):
    if battles is None:
        return "0. Нет данных УР"
    if battles < CLAN_MIN_UR_BATTLES or winrate is None:
        return f"0. Мало боёв в УР (до {CLAN_MIN_UR_BATTLES})"
    for limit, label in ((45, "1. Слабый (до 45%)"), (55, "2. Средний (45–55%)"), (65, "3. Сильный (55–65%)")):
        if winrate < limit:
            return label
    return "4. Топ (65%+)"


def player_strength(winrate, battles):
    if battles is None:
        return None
    if battles < PLAYER_MIN_BATTLES or winrate is None:
        return f"0. Мало боёв (до {PLAYER_MIN_BATTLES})"
    for limit, label in ((47, "1. Слабый (до 47%)"), (50, "2. Средний (47–50%)"),
                         (53, "3. Хороший (50–53%)"), (56, "4. Сильный (53–56%)")):
        if winrate < limit:
            return label
    return "5. Топ (56%+)"


def enemy_strength(avg_winrate):
    if avg_winrate is None:
        return None
    for limit, label in ((48, "1. Слабые (до 48%)"), (50, "2. Средние (48–50%)"), (52, "3. Сильные (50–52%)")):
        if avg_winrate < limit:
            return label
    return "4. Очень сильные (52%+)"


def _int_or_none(value):
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def refs_from_export(battles: list[dict]) -> tuple[dict, dict, dict]:
    """What the replays themselves say about players, vehicles and clans, the latest battle winning."""
    players, vehicles, clans, clan_members = {}, {}, {}, {}
    for battle in battles:  # oldest first, so later battles overwrite: the current nickname
        for p in battle.get("players", []):
            d = p.get("details") or {}
            account_id = _int_or_none(d.get("accountDBID"))
            if account_id:
                seen = (players.get(account_id) or {}).get("battles_seen", 0)
                players[account_id] = {"account_id": account_id, "nickname": p.get("name"),
                                       "clan": d.get("clanAbbrev") or None, "battles_seen": seen + 1}
            tank_id = _int_or_none(d.get("typeCompDescr"))
            if tank_id:
                nation, _, tag = (d.get("vehicleType") or "").partition(":")
                vehicles[tank_id] = {"tank_id": tank_id, "vehicle": p.get("vehicle"), "tag": tag or None,
                                     "nation": NATIONS.get(nation, nation or None)}
            clan_id = _int_or_none(d.get("clanDBID"))
            if clan_id:
                clan_members.setdefault(clan_id, set()).add(account_id)
                clans[clan_id] = {"clan_id": clan_id, "tag": d.get("clanAbbrev") or None,
                                  "players_seen": len(clan_members[clan_id])}
    return players, vehicles, clans


def _lesta_get(method: str, params: dict) -> dict:
    url = f"{LESTA_API}/{method}/?" + urlencode(params)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                body = json.loads(resp.read())
        except (urllib.error.URLError, OSError, ValueError) as e:
            if attempt == 2:
                sys.exit(f"API Лесты недоступен ({method}): {e}")
            time.sleep(2)
            continue
        if body.get("status") == "ok":
            return body
        error = body.get("error") or {}
        if error.get("message") == "REQUEST_LIMIT_EXCEEDED" and attempt < 2:
            time.sleep(2)
            continue
        sys.exit(f"API Лесты вернул ошибку на {method}: {error.get('message')} {error.get('field') or ''}".strip())
    return {}


def fetch_vehicles(app_id: str) -> dict:
    """The whole tankopedia (about a thousand vehicles in ten pages), keyed by tank_id."""
    fields = "tank_id,name,tag,type,tier,nation,is_premium,is_wheeled,default_profile.hp"
    found, page = {}, 1
    while True:
        body = _lesta_get("encyclopedia/vehicles", {"application_id": app_id, "fields": fields,
                                                    "limit": 100, "page_no": page})
        found.update({int(k): v for k, v in (body.get("data") or {}).items() if v})
        if page >= (body.get("meta") or {}).get("page_total", 1):
            return found
        page += 1
        time.sleep(0.15)


def fetch_accounts(app_id: str, account_ids: list[int]) -> dict:
    fields = "account_id,created_at,last_battle_time,global_rating,statistics.all.battles,statistics.all.wins"
    found = {}
    for start in range(0, len(account_ids), 100):
        chunk = account_ids[start:start + 100]
        body = _lesta_get("account/info", {"application_id": app_id, "fields": fields,
                                           "account_id": ",".join(map(str, chunk))})
        found.update({int(k): v for k, v in (body.get("data") or {}).items() if v})
        time.sleep(0.15)
        if start and start % 2000 == 0:
            print(f"  аккаунты: {start} из {len(account_ids)}")
    return found


def fetch_clans(app_id: str, clan_ids: list[int]) -> dict:
    """Clan card (clans/info, 100 per request) and Stronghold results (stronghold/claninfo, 10 per request)."""
    found = {}
    for start in range(0, len(clan_ids), 100):
        chunk = clan_ids[start:start + 100]
        body = _lesta_get("clans/info", {"application_id": app_id, "fields": "clan_id,tag,name,members_count,created_at",
                                         "clan_id": ",".join(map(str, chunk))})
        for k, v in (body.get("data") or {}).items():
            if v:
                found.setdefault(int(k), {})["info"] = v
        time.sleep(0.15)
    fields = ",".join([
        "clan_id", "stronghold_level",
        "skirmish_statistics.win_10", "skirmish_statistics.total_10",
        "skirmish_statistics.win_10_in_28d", "skirmish_statistics.total_10_in_28d",
        "battles_for_strongholds_statistics.win_10", "battles_for_strongholds_statistics.total_10",
        "battles_for_strongholds_statistics.win_10_in_28d", "battles_for_strongholds_statistics.total_10_in_28d",
    ])
    for start in range(0, len(clan_ids), 10):
        chunk = clan_ids[start:start + 10]
        body = _lesta_get("stronghold/claninfo", {"application_id": app_id, "fields": fields,
                                                  "clan_id": ",".join(map(str, chunk))})
        for k, v in (body.get("data") or {}).items():
            if v:
                found.setdefault(int(k), {})["stronghold"] = v
        time.sleep(0.15)
        if start and start % 500 == 0:
            print(f"  Укрепрайоны: {start} из {len(clan_ids)}")
    return found


def _percent_or_none(wins, total):
    return round(wins / total * 100, 2) if total and wins is not None else None


def api_clan_fields(info: dict) -> dict:
    card, sh = info.get("info") or {}, info.get("stronghold")
    row = {"name": card.get("name"), "members": card.get("members_count"), "created_at": _day(card.get("created_at")),
           "tag": card.get("tag")}
    if sh is None:  # no Stronghold at all
        row.update({"stronghold_level": None, "ur_battles": None, "ur_winrate": None,
                    "ur_battles_28d": None, "ur_winrate_28d": None})
    else:
        sk = sh.get("skirmish_statistics") or {}
        bf = sh.get("battles_for_strongholds_statistics") or {}
        total = (sk.get("total_10") or 0) + (bf.get("total_10") or 0)
        wins = (sk.get("win_10") or 0) + (bf.get("win_10") or 0)
        total_28 = (sk.get("total_10_in_28d") or 0) + (bf.get("total_10_in_28d") or 0)
        wins_28 = (sk.get("win_10_in_28d") or 0) + (bf.get("win_10_in_28d") or 0)
        row.update({"stronghold_level": sh.get("stronghold_level"), "ur_battles": total,
                    "ur_winrate": _percent_or_none(wins, total),
                    "ur_battles_28d": total_28, "ur_winrate_28d": _percent_or_none(wins_28, total_28)})
    row["strength"] = clan_strength(row["ur_winrate"], row["ur_battles"])
    return row


def _day(timestamp):
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")


def api_player_fields(info: dict, now: datetime) -> dict:
    stats = (info.get("statistics") or {}).get("all") or {}
    battles, wins = stats.get("battles"), stats.get("wins")
    created = info.get("created_at")
    return {
        "created_at": _day(created),
        "account_age_days": (now - datetime.fromtimestamp(created, timezone.utc)).days if created else None,
        "global_rating": info.get("global_rating"),
        "battles_total": battles,
        "winrate_total": round(wins / battles * 100, 2) if battles and wins is not None else None,
        "last_battle": _day(info.get("last_battle_time")),
    }


def api_vehicle_fields(info: dict) -> dict:
    return {
        "api_name": info.get("name"),
        "vehicle_class": VEHICLE_CLASSES.get(info.get("type"), info.get("type")),
        "tier": info.get("tier"),
        "is_premium": int(bool(info.get("is_premium"))),
        "is_wheeled": int(bool(info.get("is_wheeled"))),
        "hp": (info.get("default_profile") or {}).get("hp"),
    }


def read_refs(path: Path) -> dict:
    """{sheet: {key: row}} from an existing reference book; columns found by header or key."""
    from openpyxl import load_workbook
    book, wb = {}, load_workbook(path, read_only=True, data_only=True)
    for sheet, key, columns in REF_SHEETS:
        rows = {}
        if sheet in wb.sheetnames:
            values = list(wb[sheet].iter_rows(values_only=True))
            by_header = {h: k for k, h, _ in columns} | {k: k for k, _, _ in columns}
            header = [by_header.get(str(h).strip()) if h is not None else None for h in (values[0] if values else ())]
            for line in values[1:]:
                row = {k: v for k, v in zip(header, line) if k and v not in (None, "")}
                if _int_or_none(row.get(key)):
                    row[key] = int(row[key])
                    rows[row[key]] = row
        book[sheet] = rows
    wb.close()
    return book


def merge_refs(existing: dict, replay: dict, api: dict, columns) -> list[dict]:
    """Replay columns from the latest export, API ones fresh when fetched, manual ones always kept.
    Rows only in the old book stay: hand-filled data is never dropped."""
    merged = []
    for key in sorted(set(existing) | set(replay)):
        old, row = existing.get(key, {}), {}
        for column, _, source in columns:
            if source == "replay":  # the API may fill a gap the replay left (a clan's tag)
                value = (replay.get(key) or {}).get(column)
                if value is None:
                    value = (api.get(key) or {}).get(column)
                row[column] = value if value is not None else old.get(column)
            elif source == "api":
                row[column] = api[key].get(column) if key in api else old.get(column)
            elif source == "manual_auto":  # filled automatically, but a hand-written value is kept
                value = old.get(column)
                row[column] = value if value not in (None, "") else (api.get(key) or {}).get(column)
            else:
                row[column] = old.get(column)
        merged.append(row)
    return merged


def write_refs(path: Path, sheets: dict) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
    manual_fill = PatternFill("solid", fgColor="FFF2CC")
    wb = Workbook()
    wb.remove(wb.active)
    for sheet, key, columns in REF_SHEETS:
        ws = wb.create_sheet(sheet)
        ws.append([header for _, header, _ in columns])
        for row in sheets[sheet]:
            ws.append([_cell(row.get(k)) for k, _, _ in columns])
        for i, (_, header, source) in enumerate(columns, start=1):
            cell = ws.cell(row=1, column=i)
            cell.font = Font(bold=True)
            if source in ("manual", "manual_auto"):
                cell.fill = manual_fill
            ws.column_dimensions[get_column_letter(i)].width = min(max(len(header) + 2, 12), 30)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{max(len(sheets[sheet]), 1) + 1}"
    wb.save(path)


def load_game_vehicles(path: Path) -> dict:
    """{tank_id: class, tier, premium, wheeled, role} from game_vehicles.json (scripts/game_vehicles.py)."""
    try:
        body = json.loads(Path(path).read_text(encoding="utf-8"))
        vehicles = {int(v["tank_id"]): v for v in body["vehicles"]}
    except (OSError, ValueError, KeyError, TypeError) as e:
        sys.exit(f"Не удалось прочитать список техники {path}: {e}")
    print(f"Техника из клиента {body.get('client_version') or '?'}: {len(vehicles)} машин ({path})")
    return vehicles


def update_refs(export: dict, path: Path, app_id: str | None, game_vehicles: dict | None = None) -> dict:
    players, vehicles, clans = refs_from_export(export["battles"])
    existing = read_refs(path) if path.exists() else {sheet: {} for sheet, _, _ in REF_SHEETS}
    api_players, api_vehicles, api_clans = {}, {}, {}
    if game_vehicles:
        by_tag = {v.get("tag"): v for v in game_vehicles.values()}
        for tank_id, v in vehicles.items():
            info = game_vehicles.get(tank_id) or by_tag.get(v.get("tag"))
            if info:
                api_vehicles[tank_id] = {k: info.get(k) for k in ("vehicle_class", "tier", "is_premium", "is_wheeled", "role")}
        print(f"  из них в наших реплеях {len(api_vehicles)} из {len(vehicles)}")
    if app_id:
        print(f"Танкопедия Лесты...")
        catalog = fetch_vehicles(app_id)
        by_tag = {v.get("tag"): v for v in catalog.values() if v.get("tag")}
        for tank_id, v in vehicles.items():
            info = catalog.get(tank_id) or by_tag.get(v.get("tag"))
            if info:
                api_vehicles[tank_id] = {**api_vehicles.get(tank_id, {}), **api_vehicle_fields(info)}
        print(f"Аккаунты игроков ({len(players)})...")
        now = datetime.now(timezone.utc)
        api_players = {k: api_player_fields(v, now) for k, v in fetch_accounts(app_id, sorted(players)).items()}
        print(f"Кланы и их Укрепрайоны ({len(clans)})...")
        api_clans = {k: api_clan_fields(v) for k, v in fetch_clans(app_id, sorted(clans)).items()}
    else:
        print("LESTA_APP_ID не задан: рейтинг, % побед, Укрепрайон и прочее из API не обновляются"
              + ("" if game_vehicles else "; класс, уровень и роль танков - из game_vehicles.json (--vehicles)"))
    sheets = {
        "Игроки": merge_refs(existing.get("Игроки", {}), players, api_players, PLAYER_REF_COLUMNS),
        "Танки": merge_refs(existing.get("Танки", {}), vehicles, api_vehicles, VEHICLE_REF_COLUMNS),
        "Кланы": merge_refs(existing.get("Кланы", {}), clans, api_clans, CLAN_REF_COLUMNS),
    }
    write_refs(path, sheets)
    print(f"{path}: игроков {len(sheets['Игроки'])} (из API {len(api_players)}), "
          f"танков {len(sheets['Танки'])} (из API {len(api_vehicles)}), "
          f"кланов {len(sheets['Кланы'])} (из API {len(api_clans)})")
    return sheets


# A cube drops a row whose dimension key matches no element, so an empty class or role would
# take the row out of every total. Unknown values get an explicit "not set" element instead.
NOT_SET = "Не указано"


def _float_or_none(value):
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _meaningful_winrate(p: dict):
    """A player's win rate, only when there are enough battles behind it to mean something."""
    battles, winrate = _int_or_none(p.get("battles_total")), _float_or_none(p.get("winrate_total"))
    return winrate if battles is not None and battles >= PLAYER_MIN_BATTLES else None


def enrich_players(rows: list[dict], refs: dict | None) -> list[dict]:
    """Adds BI_ENRICH_COLUMNS to flat player rows by typeCompDescr, accountDBID and clanDBID.

    enemy_avg_winrate is the mean win rate of the other team in the same battle, counting only
    players with PLAYER_MIN_BATTLES or more; with fewer than ENEMY_MIN_KNOWN such players it stays empty."""
    vehicles = (refs or {}).get("Танки", {})
    players = (refs or {}).get("Игроки", {})
    clans = (refs or {}).get("Кланы", {})

    team_rates = {}  # battle_id -> {team: win rates of its players that count}
    for row in rows:
        rate = _meaningful_winrate(players.get(_int_or_none(row.get("accountDBID"))) or {})
        if rate is not None:
            team_rates.setdefault(row.get("battle_id"), {}).setdefault(row.get("team"), []).append(rate)

    for row in rows:
        v = vehicles.get(_int_or_none(row.get("typeCompDescr"))) or {}
        p = players.get(_int_or_none(row.get("accountDBID"))) or {}
        clan_id = _int_or_none(row.get("clanDBID"))
        c = clans.get(clan_id) or {}
        enemies = [r for team, rates in team_rates.get(row.get("battle_id"), {}).items()
                   if team != row.get("team") for r in rates]
        enemy_avg = round(sum(enemies) / len(enemies), 2) if len(enemies) >= ENEMY_MIN_KNOWN else None
        row.update({
            "vehicle_class": v.get("vehicle_class") or NOT_SET,
            "vehicle_tier": _int_or_none(v.get("tier")) or 0,
            "vehicle_nation": v.get("nation") or NOT_SET,
            "vehicle_role": v.get("role") or NOT_SET,
            "vehicle_premium": _int_or_none(v.get("is_premium")),
            # some replays carry a clan id but no tag; the id keeps the row in the cube until the API names it
            "clan_tag": (row.get("clanAbbrev") or c.get("tag") or f"#{clan_id}") if clan_id else NO_CLAN,
            "clan_strength": (c.get("strength") or NOT_SET) if clan_id else NO_CLAN,
            "clan_ur_winrate": _float_or_none(c.get("ur_winrate")),
            "player_strength": player_strength(_float_or_none(p.get("winrate_total")),
                                               _int_or_none(p.get("battles_total"))) or NOT_SET,
            "player_winrate": _float_or_none(p.get("winrate_total")),
            "enemy_avg_winrate": enemy_avg,
            "enemy_strength": enemy_strength(enemy_avg) or NOT_SET,
            "battle_date": row.get("day"),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="BattleStats JSON -> таблица (.xlsx или .csv)")
    parser.add_argument("json_file", type=Path, nargs="?",
                        help="локальный файл; без него выгрузка скачивается с сайта")
    parser.add_argument("--save", type=Path, default=Path("battlestats.json"),
                        help="куда сохранить скачанную выгрузку (по умолчанию battlestats.json). Имя постоянное, "
                             "чтобы справочник <имя>_refs.xlsx с ручными колонками был один и тот же")
    parser.add_argument("--all", action="store_true",
                        help="при скачивании: все бои в базе, а не только свои")
    parser.add_argument("--date-from", metavar="ГГГГ-ММ-ДД", help="при скачивании: бои с этого дня")
    parser.add_argument("--date-to", metavar="ГГГГ-ММ-ДД", help="при скачивании: бои по этот день")
    parser.add_argument("-o", "--output", type=Path,
                        help="куда сохранить: .xlsx (по умолчанию) или .csv")
    parser.add_argument("--bi", action="store_true",
                        help="плоские CSV для Форсайта/BI: <имя>_bi_battles.csv и <имя>_bi_players.csv")
    parser.add_argument("--refs", action="store_true",
                        help="создать/обновить справочник игроков и танков <имя>_refs.xlsx (API Лесты: LESTA_APP_ID)")
    parser.add_argument("--vehicles", type=Path,
                        help="список техники из scripts/game_vehicles.py (класс, уровень, роль без API); "
                             "по умолчанию game_vehicles.json рядом с выгрузкой, если он есть")
    parser.add_argument("--refs-file", type=Path,
                        help="путь к справочнику вместо <имя>_refs.xlsx рядом с JSON")
    args = parser.parse_args()

    if args.json_file:
        source, default_out = str(args.json_file), args.json_file.with_suffix(".xlsx")
        try:
            raw = args.json_file.read_bytes()
        except OSError as e:
            sys.exit(f"Не удалось прочитать {source}: {e}")
    else:
        raw, name = load_from_site(not args.all, args.date_from, args.date_to)
        args.save.write_bytes(raw)
        print(f"{args.save}: выгрузка с сайта ({name}, {len(raw) / 1e6:.1f} МБ)")
        source, default_out = str(args.save), args.save.with_suffix(".xlsx")

    try:
        export = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        sys.exit(f"{source} не читается как JSON: {e}")
    if not isinstance(export, dict) or "battles" not in export:
        sys.exit(f"{source} не похож на выгрузку BattleStats: нет поля battles")

    out = args.output or default_out
    refs_path = args.refs_file or default_out.with_name(f"{default_out.stem}_refs.xlsx")
    if args.refs or args.bi:
        try:
            import openpyxl  # noqa: F401  the reference book is an Excel file
        except ImportError as e:
            if args.refs or refs_path.exists():
                sys.exit(f"Для справочника нужен openpyxl ({e}): \"{sys.executable}\" -m pip install openpyxl")

    refs = None
    if args.refs:
        vehicles_path = args.vehicles or default_out.with_name("game_vehicles.json")
        game_vehicles = load_game_vehicles(vehicles_path) if args.vehicles or vehicles_path.exists() else None
        refs_sheets = update_refs(export, refs_path, os.environ.get("LESTA_APP_ID"), game_vehicles)
        refs = {sheet: {row[key]: row for row in refs_sheets[sheet]} for sheet, key, _ in REF_SHEETS}
    elif args.bi and refs_path.exists():
        refs = read_refs(refs_path)
        print(f"Атрибуты игроков и танков из {refs_path}")

    if args.bi:
        battles = export["battles"]
        path = out.with_name(f"{out.stem}_bi_battles.csv")
        rows = flatten_bi(battles, "battles")
        write_bi_csv(path, BI_BATTLE_COLUMNS, rows)
        print(f"{path}: {len(rows)} строк")
        path = out.with_name(f"{out.stem}_bi_players.csv")
        rows = enrich_players(flatten_bi(battles, "players"), refs)
        write_bi_csv(path, BI_PLAYER_COLUMNS + BI_ENRICH_COLUMNS, rows)
        print(f"{path}: {len(rows)} строк")
        if refs is not None:
            for sheet, key, columns in REF_SHEETS:
                path = out.with_name(f"{out.stem}_bi_{REF_CSV_NAMES[sheet]}.csv")
                rows = [{k: _bi_value(r.get(k)) for k, _, _ in columns} for r in refs[sheet].values()]
                write_bi_csv(path, [k for k, _, _ in columns], rows)
                print(f"{path}: {len(rows)} строк")
        else:
            print(f"Справочника {refs_path.name} нет - атрибуты игроков и танков пустые (создать: --refs)")
    if args.bi or args.refs:
        return

    battle_header, battle_rows, player_header, player_rows = build_tables(export)

    if out.suffix.lower() == ".xlsx":
        try:
            import openpyxl  # noqa: F401  checked up front so a real import error is not hidden
        except ImportError as e:
            out = out.with_suffix(".csv")
            print(f"Не удалось загрузить openpyxl: {e}")
            print(f"Этот Python: {sys.executable}")
            print(f"Поставь openpyxl именно в него: \"{sys.executable}\" -m pip install openpyxl")
            print("Пока сохраняю в CSV.")
        else:
            write_xlsx(out, [("Бои", battle_header, battle_rows), ("Игроки", player_header, player_rows)])
            print(f"{out}: {len(battle_rows)} боёв, {len(player_rows)} строк игроков")
            return

    battles_path = out.with_name(f"{out.stem}_battles.csv")
    players_path = out.with_name(f"{out.stem}_players.csv")
    write_csv(battles_path, battle_header, battle_rows)
    write_csv(players_path, player_header, player_rows)
    print(f"{battles_path}: {len(battle_rows)} боёв")
    print(f"{players_path}: {len(player_rows)} строк игроков")


if __name__ == "__main__":
    main()
