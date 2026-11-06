"""Turn a /api/export JSON file into tables: one row per battle, one row per player in a battle.

Usage:
    python scripts/export_to_table.py                                  # newest .json in Nextcloud "battlestats"
    python scripts/export_to_table.py --remote battlestats_mine.json   # that file from the same folder
    python scripts/export_to_table.py battlestats.json                 # local file -> battlestats.xlsx
    python scripts/export_to_table.py battlestats.json -o stats.csv    # -> stats_battles.csv, stats_players.csv
    python scripts/export_to_table.py battlestats.json --bi            # -> battlestats_bi_battles.csv, ..._bi_players.csv

Nextcloud is reached over WebDAV with these environment variables:
    NEXTCLOUD_URL       https://cloud.example.ru
    NEXTCLOUD_USER      login
    NEXTCLOUD_PASSWORD  an app password (Settings -> Security); asked for if not set
    NEXTCLOUD_FOLDER    folder with the exports, "battlestats" by default

Needs nothing beyond the standard library for CSV; .xlsx needs `python -m pip install openpyxl`.
CSV is written with ";" and a UTF-8 BOM, so Excel with a Russian locale opens it as is.
"""
import argparse
import base64
import csv
import getpass
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, unquote

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


class Nextcloud:
    """Just enough WebDAV to list one folder and download a file from it."""

    def __init__(self, url: str, user: str, password: str, verify_tls: bool = True):
        url = url.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {token}", "OCS-APIRequest": "true"}
        self.context = None if verify_tls else ssl._create_unverified_context()
        self.base = f"{url}/remote.php/dav/files/{quote(self._user_id(url, user))}/"

    def _user_id(self, url: str, login: str) -> str:
        """The account ID the files live under. With LDAP/AD sign-in it can differ from the login."""
        req = urllib.request.Request(f"{url}/ocs/v2.php/cloud/user?format=json", headers=self.headers)
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=30) as resp:
                return json.loads(resp.read())["ocs"]["data"]["id"] or login
        except (urllib.error.URLError, ValueError, KeyError, TypeError, OSError):
            return login  # the folder request that follows reports what is actually wrong

    def _request(self, url: str, method: str = "GET", body: bytes = None, headers: dict = None) -> bytes:
        req = urllib.request.Request(url, data=body, method=method,
                                     headers={**self.headers, **(headers or {})})
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=120) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            hint = {401: "неверный логин или пароль приложения",
                    404: "нет такой папки или файла"}.get(e.code, e.reason)
            sys.exit(f"Nextcloud ответил {e.code} на {unquote(url)}: {hint}")
        except urllib.error.URLError as e:
            sys.exit(f"Не удалось подключиться к Nextcloud ({unquote(url)}): {e.reason}")

    def list_json(self, folder: str) -> list[tuple[str, object]]:
        """(name, modified) of every .json directly in `folder`, newest first."""
        body = (b'<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop>'
                b"<d:getlastmodified/><d:resourcetype/></d:prop></d:propfind>")
        url = self.base + quote(folder.strip("/")) + "/"
        tree = ET.fromstring(self._request(url, "PROPFIND", body,
                                           {"Depth": "1", "Content-Type": "application/xml"}))
        ns = {"d": "DAV:"}
        files = []
        for resp in tree.findall("d:response", ns):
            name = unquote(resp.findtext("d:href", "", ns).rstrip("/").rsplit("/", 1)[-1])
            if resp.find(".//d:resourcetype/d:collection", ns) is not None:
                continue
            if not name.lower().endswith(".json"):
                continue
            modified = resp.findtext(".//d:getlastmodified", "", ns)
            files.append((name, parsedate_to_datetime(modified) if modified else None))
        files.sort(key=lambda f: (f[1] is not None, f[1] or 0), reverse=True)
        return files

    def download(self, folder: str, name: str) -> bytes:
        return self._request(self.base + quote(folder.strip("/")) + "/" + quote(name))


def load_from_nextcloud(remote_name: str | None, verify_tls: bool) -> tuple[bytes, str]:
    url = os.environ.get("NEXTCLOUD_URL")
    user = os.environ.get("NEXTCLOUD_USER")
    if not url or not user:
        sys.exit("Укажи NEXTCLOUD_URL и NEXTCLOUD_USER (или передай путь к локальному JSON)")
    password = os.environ.get("NEXTCLOUD_PASSWORD") or getpass.getpass(f"Пароль Nextcloud для {user}: ")
    folder = os.environ.get("NEXTCLOUD_FOLDER", "battlestats")

    cloud = Nextcloud(url, user, password, verify_tls)
    if remote_name is None:
        files = cloud.list_json(folder)
        if not files:
            sys.exit(f"В папке {folder} на Nextcloud нет .json файлов")
        remote_name = files[0][0]
        print(f"Беру самый свежий файл: {folder}/{remote_name}")
    return cloud.download(folder, remote_name), remote_name


def main():
    parser = argparse.ArgumentParser(description="BattleStats JSON -> таблица (.xlsx или .csv)")
    parser.add_argument("json_file", type=Path, nargs="?",
                        help="локальный файл; без него JSON берётся из Nextcloud")
    parser.add_argument("-r", "--remote", metavar="NAME",
                        help="имя файла в папке Nextcloud (по умолчанию самый свежий .json)")
    parser.add_argument("-o", "--output", type=Path,
                        help="куда сохранить: .xlsx (по умолчанию) или .csv")
    parser.add_argument("--bi", action="store_true",
                        help="плоские CSV для Форсайта/BI: <имя>_bi_battles.csv и <имя>_bi_players.csv")
    parser.add_argument("--no-verify-tls", action="store_true",
                        help="не проверять сертификат Nextcloud (корпоративный самоподписанный)")
    args = parser.parse_args()

    if args.json_file:
        source, default_out = str(args.json_file), args.json_file.with_suffix(".xlsx")
        try:
            raw = args.json_file.read_bytes()
        except OSError as e:
            sys.exit(f"Не удалось прочитать {source}: {e}")
    else:
        raw, name = load_from_nextcloud(args.remote, not args.no_verify_tls)
        source, default_out = name, Path(name).with_suffix(".xlsx")

    try:
        export = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        sys.exit(f"{source} не читается как JSON: {e}")
    if not isinstance(export, dict) or "battles" not in export:
        sys.exit(f"{source} не похож на выгрузку BattleStats: нет поля battles")

    out = args.output or default_out
    if args.bi:
        battles = export["battles"]
        for kind, columns in (("battles", BI_BATTLE_COLUMNS), ("players", BI_PLAYER_COLUMNS)):
            path = out.with_name(f"{out.stem}_bi_{kind}.csv")
            rows = flatten_bi(battles, kind)
            write_bi_csv(path, columns, rows)
            print(f"{path}: {len(rows)} строк")
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
