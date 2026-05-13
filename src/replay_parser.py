import re
import struct
import json
import hashlib
from pathlib import Path
from dataclasses import dataclass

# .mtreplay magic bytes (first 4 bytes of file)
_MT_MAGIC = bytes([0x12, 0x32, 0x34, 0x11])

_MAP_NAMES: dict[str, str] = {
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
    "28_desert":            "Песчанаая река",
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
    "503_kaliningrad":      "Каллининград",
}


def _map_display(internal: str) -> str:
    """Translate internal map ID to display name, fall back to cleaned-up ID."""
    if internal in _MAP_NAMES:
        return _MAP_NAMES[internal]
    # Strip leading number: "05_prohorovka" → "Prohorovka"
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
        raise ValueError("File too small")
    num_blocks = struct.unpack_from("<I", data, 4)[0]
    size1 = struct.unpack_from("<I", data, 8)[0]
    block1 = data[12 : 12 + size1]
    block2 = None
    if num_blocks >= 2:
        off = 12 + size1
        size2 = struct.unpack_from("<I", data, off)[0]
        block2 = data[off + 4 : off + 4 + size2]
    return block1, block2


def _vehicle_name(raw: str) -> str:
    """'ussr:R231_Object_278' → 'Object 278'"""
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


def parse_replay(path: str | Path) -> BattleResult:
    data = Path(path).read_bytes()
    filename = Path(path).name

    if data[:4] != _MT_MAGIC:
        raise ValueError("Not a valid .mtreplay file (wrong magic bytes)")

    block1_raw, block2_raw = _read_blocks_mt(data)

    meta: dict = json.loads(block1_raw.decode("utf-8", errors="replace"))

    map_internal: str = meta.get("mapName") or ""
    map_name: str = _map_display(map_internal) if map_internal else (meta.get("mapDisplayName") or "Unknown")
    date_time: str = meta.get("dateTime", "")
    player_name: str = meta.get("playerName", "")

    # meta vehicles: {arena_vehicle_id: {name, vehicleType, team, ...}}
    meta_vehicles: dict = meta.get("vehicles", {})

    player_team = 0
    player_vehicle = ""
    for v in meta_vehicles.values():
        if isinstance(v, dict) and v.get("name") == player_name:
            player_team = v.get("team", 0)
            player_vehicle = _vehicle_name(v.get("vehicleType", ""))
            break

    if not block2_raw:
        # Incomplete replay — no battle results, show roster only
        players = [
            PlayerResult(
                name=v.get("name", ""),
                vehicle=_vehicle_name(v.get("vehicleType", "")),
                team=v.get("team", 0),
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
        battle_hash = _make_battle_hash(players, map_internal)
        return BattleResult(
            filename=filename,
            map_name=map_name,
            date_time=date_time,
            battle_hash=battle_hash,
            winner_team=0,
            player_team=player_team,
            player_vehicle=player_vehicle,
            players=players,
        )

    # block2 is a JSON list: [results_dict, players_dict, frags_dict]
    block2: list = json.loads(block2_raw.decode("utf-8", errors="replace"))

    results: dict = block2[0] if isinstance(block2, list) and block2 else {}
    # players_info has name, vehicleType, team, isAlive per arena_vehicle_id
    players_info: dict = block2[1] if len(block2) > 1 else {}

    common: dict = results.get("common", {})
    winner_team: int = common.get("winnerTeam", 0)

    # vehicles has full stats per arena_vehicle_id → list with one dict
    res_vehicles: dict = results.get("vehicles", {})

    players: list[PlayerResult] = []

    for vid, v_list in res_vehicles.items():
        if not isinstance(v_list, list) or not v_list:
            continue
        v: dict = v_list[0]

        # Name and vehicle type from players_info (more reliable than meta for final state)
        pinfo: dict = players_info.get(vid, {}) or meta_vehicles.get(vid, {})
        name: str = pinfo.get("name") or pinfo.get("fakeName") or f"id:{vid}"
        vehicle: str = _vehicle_name(pinfo.get("vehicleType", ""))

        team: int = v.get("team", 0)
        damage_dealt: int = v.get("damageDealt", 0)
        damage_received: int = v.get("damageReceived", 0)
        blocked: int = v.get("damageBlockedByArmor", 0)
        assists: int = v.get("damageAssistedRadio", 0) + v.get("damageAssistedTrack", 0)
        frags: int = v.get("kills", 0)
        xp: int = v.get("xp", 0)
        survived: bool = v.get("deathReason", -1) == -1
        shots: int = v.get("shots", 0)
        hits: int = v.get("directHits", 0)
        piercings: int = v.get("piercings", 0)

        players.append(PlayerResult(
            name=name,
            vehicle=vehicle,
            team=team,
            damage_dealt=damage_dealt,
            damage_received=damage_received,
            blocked=blocked,
            assists=assists,
            frags=frags,
            xp=xp,
            survived=survived,
            shots=shots,
            hits=hits,
            piercings=piercings,
        ))

    players.sort(key=lambda p: (p.team, -p.damage_dealt))
    battle_hash = _make_battle_hash(players, map_internal)

    return BattleResult(
        filename=filename,
        map_name=map_name,
        date_time=date_time,
        battle_hash=battle_hash,
        winner_team=winner_team,
        player_team=player_team,
        player_vehicle=player_vehicle,
        players=players,
    )
