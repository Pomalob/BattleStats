import re
import struct
import json
import hashlib
from pathlib import Path
from dataclasses import dataclass

# .mtreplay magic bytes (first 4 bytes of file)
_MT_MAGIC = bytes([0x12, 0x32, 0x34, 0x11])

# A replay block is JSON metadata; anything above a few MB is malformed or hostile.
_MAX_BLOCK_SIZE = 32 * 1024 * 1024

MAP_NAMES: dict[str, str] = {
    "01_karelia":           "Карелия",
    "02_malinovka":         "Малиновка",
    "04_himmelsdorf":       "Химмельсдорф",
    "05_prohorovka":        "Прохоровка",
    "07_lakeville":         "Ласвилль",
    "08_ruinberg":          "Руинберг",
    "10_hills":             "Рудники",
    "11_murovanka":         "Мурованка",
    "13_erlenberg":         "Эрленберг",
    "14_siegfried_line":    "Линия Зигфрида",
    "18_cliff":             "Утёс",
    "28_desert":            "Песчаная река",
    "34_redshire":          "Редшир",
    "35_steppes":           "Степи",
    "36_fishing_bay":       "Рыбацкая бухта",
    "38_mannerheim_line":   "Линия Маннергейма",
    "44_north_america":     "Лайв Окс",
    "45_north_america":     "Хайвей",
    "47_canada_a":          "Тихий берег",
    "60_asia_miao":         "Жемчужная река",
    "90_minsk":             "Минск",
    "127_japort":           "Старая гавань",
    "503_kaliningrad":      "Калининград",
}

# Display names that earlier versions wrote into the database with typos.
MAP_NAME_FIXES: dict[str, str] = {
    "Песчанаая река": "Песчаная река",
    "Каллининград":   "Калининград",
}


def _map_display(internal: str) -> str:
    """Translate internal map ID to display name, fall back to cleaned-up ID."""
    if internal in MAP_NAMES:
        return MAP_NAMES[internal]
    # Strip leading number: "05_prohorovka" -> "Prohorovka"
    cleaned = re.sub(r'^\d+_', '', internal).replace("_", " ").title()
    return cleaned or internal


@dataclass
class PlayerResult:
    name: str
    vehicle: str
    team: int
    damage_dealt: int
    damage_received: int
    blocked: int
    assists: int
    frags: int
    xp: int
    survived: bool
    shots: int
    hits: int
    piercings: int


@dataclass
class BattleResult:
    filename: str
    map_name: str
    date_time: str
    battle_hash: str
    winner_team: int
    player_team: int
    player_vehicle: str
    players: list[PlayerResult]

    @property
    def result(self) -> str:
        if self.winner_team == 0:
            return "draw"
        return "win" if self.winner_team == self.player_team else "loss"


def _read_blocks_mt(data: bytes) -> tuple[bytes, bytes | None]:
    """Parse .mtreplay: magic(4) | num_blocks(4) | size1(4) | block1 | size2(4) | block2"""
    if len(data) < 12:
        raise ValueError("Файл слишком маленький")
    num_blocks = struct.unpack_from("<I", data, 4)[0]
    size1 = struct.unpack_from("<I", data, 8)[0]
    if size1 > _MAX_BLOCK_SIZE or 12 + size1 > len(data):
        raise ValueError("Битый реплей: размер блока метаданных вне диапазона")
    block1 = data[12:12 + size1]

    block2 = None
    if num_blocks >= 2:
        off = 12 + size1
        if off + 4 > len(data):
            raise ValueError("Битый реплей: файл обрывается перед блоком результатов")
        size2 = struct.unpack_from("<I", data, off)[0]
        if size2 > _MAX_BLOCK_SIZE or off + 4 + size2 > len(data):
            raise ValueError("Битый реплей: размер блока результатов вне диапазона")
        block2 = data[off + 4:off + 4 + size2]
    return block1, block2


def _load_json(raw: bytes, what: str):
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Битый реплей: {what} — некорректный JSON ({e.msg})") from e


def _vehicle_name(raw: str) -> str:
    """'ussr:R231_Object_278' -> 'Object 278'"""
    if not raw:
        return ""
    name = raw.split(":")[-1]
    name = re.sub(r'^[A-Za-z]+\d+_', '', name)  # strip nation prefix: R231_, G100_, Ch45_, etc.
    return name.replace("_", " ")


def _make_battle_hash(players: list["PlayerResult"], map_name: str) -> str:
    """Stable hash of map + sorted (name, damage) — identical for all replays of the same battle."""
    parts = sorted(f"{p.name}:{p.damage_dealt}" for p in players)
    key = map_name + "|" + ",".join(parts)
    return hashlib.md5(key.encode()).hexdigest()


def _as_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def parse_replay(path: str | Path) -> BattleResult:
    """Parse a .mtreplay file from disk."""
    path = Path(path)
    return parse_replay_bytes(path.read_bytes(), path.name)


def parse_replay_bytes(data: bytes, filename: str) -> BattleResult:
    """Parse .mtreplay contents already held in memory."""
    if data[:4] != _MT_MAGIC:
        raise ValueError("Не похоже на .mtreplay: неверная сигнатура файла")

    block1_raw, block2_raw = _read_blocks_mt(data)

    meta = _load_json(block1_raw, "блок метаданных")
    if not isinstance(meta, dict):
        raise ValueError("Битый реплей: блок метаданных не является объектом")

    map_internal: str = meta.get("mapName") or ""
    map_name: str = _map_display(map_internal) if map_internal else (meta.get("mapDisplayName") or "Unknown")
    date_time: str = meta.get("dateTime", "")
    player_name: str = meta.get("playerName", "")

    # meta vehicles: {arena_vehicle_id: {name, vehicleType, team, ...}}
    meta_vehicles: dict = meta.get("vehicles") or {}
    if not isinstance(meta_vehicles, dict):
        meta_vehicles = {}

    player_team = 0
    player_vehicle = ""
    for v in meta_vehicles.values():
        if isinstance(v, dict) and v.get("name") == player_name:
            player_team = _as_int(v.get("team"))
            player_vehicle = _vehicle_name(v.get("vehicleType", ""))
            break

    if not block2_raw:
        # Incomplete replay — no battle results, show roster only
        players = [
            PlayerResult(
                name=v.get("name", ""),
                vehicle=_vehicle_name(v.get("vehicleType", "")),
                team=_as_int(v.get("team")),
                damage_dealt=0,
                damage_received=0,
                blocked=0,
                assists=0,
                frags=0,
                xp=0,
                survived=False,
                shots=0,
                hits=0,
                piercings=0,
            )
            for v in meta_vehicles.values()
            if isinstance(v, dict)
        ]
        players.sort(key=lambda p: (p.team, -p.damage_dealt))
        return BattleResult(
            filename=filename,
            map_name=map_name,
            date_time=date_time,
            battle_hash=_make_battle_hash(players, map_internal),
            winner_team=0,
            player_team=player_team,
            player_vehicle=player_vehicle,
            players=players,
        )

    # block2 is a JSON list: [results_dict, players_dict, frags_dict]
    block2 = _load_json(block2_raw, "блок результатов")
    if not isinstance(block2, list) or not block2:
        raise ValueError("Битый реплей: неожиданная структура блока результатов")

    results: dict = block2[0] if isinstance(block2[0], dict) else {}
    # players_info has name, vehicleType, team, isAlive per arena_vehicle_id
    players_info: dict = block2[1] if len(block2) > 1 and isinstance(block2[1], dict) else {}

    common: dict = results.get("common") or {}
    winner_team: int = _as_int(common.get("winnerTeam"))

    # vehicles has full stats per arena_vehicle_id -> list with one dict
    res_vehicles: dict = results.get("vehicles") or {}

    players: list[PlayerResult] = []

    for vid, v_list in res_vehicles.items():
        if not isinstance(v_list, list) or not v_list or not isinstance(v_list[0], dict):
            continue
        v: dict = v_list[0]

        # Name and vehicle type from players_info (more reliable than meta for final state)
        pinfo = players_info.get(vid) or meta_vehicles.get(vid) or {}
        if not isinstance(pinfo, dict):
            pinfo = {}
        name: str = pinfo.get("name") or pinfo.get("fakeName") or f"id:{vid}"
        vehicle: str = _vehicle_name(pinfo.get("vehicleType", ""))

        players.append(PlayerResult(
            name=name,
            vehicle=vehicle,
            team=_as_int(v.get("team")),
            damage_dealt=_as_int(v.get("damageDealt")),
            damage_received=_as_int(v.get("damageReceived")),
            blocked=_as_int(v.get("damageBlockedByArmor")),
            assists=_as_int(v.get("damageAssistedRadio")) + _as_int(v.get("damageAssistedTrack")),
            frags=_as_int(v.get("kills")),
            xp=_as_int(v.get("xp")),
            survived=v.get("deathReason", -1) == -1,
            shots=_as_int(v.get("shots")),
            hits=_as_int(v.get("directHits")),
            piercings=_as_int(v.get("piercings")),
        ))

    players.sort(key=lambda p: (p.team, -p.damage_dealt))

    return BattleResult(
        filename=filename,
        map_name=map_name,
        date_time=date_time,
        battle_hash=_make_battle_hash(players, map_internal),
        winner_team=winner_team,
        player_team=player_team,
        player_vehicle=player_vehicle,
        players=players,
    )
