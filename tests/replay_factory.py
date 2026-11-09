"""Builds synthetic .mtreplay files so the parser can be tested without real game data."""
import json
import struct

MAGIC = bytes([0x12, 0x32, 0x34, 0x11])


def _block(payload) -> bytes:
    raw = json.dumps(payload).encode("utf-8")
    return struct.pack("<I", len(raw)) + raw


def build_replay(
    *,
    map_name: str = "05_prohorovka",
    date_time: str = "2025-01-02 20:15:33",
    player_name: str = "Alice",
    winner_team: int = 1,
    players: list[dict] | None = None,
    with_results: bool = True,
    anonymise_meta: bool = False,
    drop_personal: bool = False,
    map_display_name: str | None = None,
    real_names: dict[str, str] | None = None,
    bonus_type: int | None = None,
    avatar: dict | None = None,
) -> bytes:
    """Assemble a replay from a compact player spec.

    Each player dict accepts: name, team, vehicle_type, damage, received, blocked,
    assist_radio, assist_track, assist_stun, kills, xp, shots, hits, piercings, death_reason,
    platoon (prebattleID, needs real_names) and extra (more raw result fields).

    bonus_type is the battle mode number (1 random, 20 clan 7x7, 43 Onslaught); avatar is
    the recorder's personal block (comp7Rating and the like).

    anonymise_meta replaces the names in the metadata block the way the in-game
    nickname anonymiser does; drop_personal removes the recording player's own
    result entry.
    """
    players = players if players is not None else [
        {"name": "Alice", "team": 1, "vehicle_type": "ussr:R231_Object_278", "damage": 2400,
         "kills": 3, "shots": 10, "hits": 8, "piercings": 6},
        {"name": "Bob", "team": 1, "vehicle_type": "germany:G100_Maus", "damage": 1200,
         "blocked": 3000, "kills": 1},
        {"name": "Carol", "team": 2, "vehicle_type": "usa:A80_T110E5", "damage": 900,
         "kills": 0, "death_reason": 0},
    ]

    meta_vehicles = {
        str(i + 1): {
            "name": f"Hidden{i + 1}" if anonymise_meta else p["name"],
            "vehicleType": p.get("vehicle_type", "ussr:R231_Object_278"),
            "team": p.get("team", 1),
        }
        for i, p in enumerate(players)
    }
    meta = {
        "mapName": map_name,
        "dateTime": date_time,
        "playerName": player_name,
        "playerID": 7,
        "hasMods": True,
        "arenaUniqueID": 7226559141569606,
        "serverName": "RU4",
        "serverSettings": {"comp7_config": {"huge": "block"}},
        "vehicles": meta_vehicles,
    }
    if bonus_type is not None:
        meta["battleType"] = bonus_type
    if map_display_name is not None:
        meta["mapDisplayName"] = map_display_name

    data = MAGIC + struct.pack("<I", 2 if with_results else 1) + _block(meta)
    if not with_results:
        return MAGIC + struct.pack("<I", 1) + _block(meta)

    res_vehicles = {
        str(i + 1): [{
            "team": p.get("team", 1),
            "damageDealt": p.get("damage", 0),
            "damageReceived": p.get("received", 0),
            "damageBlockedByArmor": p.get("blocked", 0),
            "damageAssistedRadio": p.get("assist_radio", 0),
            "damageAssistedTrack": p.get("assist_track", 0),
            "damageAssistedStun": p.get("assist_stun", 0),
            "kills": p.get("kills", 0),
            "xp": p.get("xp", 0),
            "shots": p.get("shots", 0),
            "directHits": p.get("hits", 0),
            "piercings": p.get("piercings", 0),
            "deathReason": p.get("death_reason", -1),
            "accountDBID": 1000 + i,
            "typeCompDescr": p.get("tank_id", 7937),
            "sniperDamageDealt": p.get("sniper_damage", 0),
            "killerID": p.get("killer_id", 0),
            "credits": 15000,      # economy: must not reach the export
            "winPoints": 0,        # game-mode counter: must not reach the export
            **p.get("extra", {}),
        }]
        for i, p in enumerate(players)
    }
    players_info = {
        str(i + 1): {
            "name": p["name"],
            "vehicleType": p.get("vehicle_type", "ussr:R231_Object_278"),
            "team": p.get("team", 1),
        }
        for i, p in enumerate(players)
    }

    common = {"winnerTeam": winner_team, "duration": 293, "finishReason": 1, "teamHealth": {"1": 30000, "2": 31000}}
    if bonus_type is not None:
        common["bonusType"] = bonus_type
    results_block = {"common": common, "vehicles": res_vehicles}
    if real_names:
        results_block["players"] = {
            str(1000 + i): {"name": p["name"], "realName": real_names.get(p["name"], p["name"]),
                            "team": p.get("team", 1), "prebattleID": p.get("platoon", 0),
                            "clanDBID": 589477}
            for i, p in enumerate(players)
        }
    if not drop_personal:
        own = next((p for p in players if p["name"] == player_name), None)
        if own is not None:
            results_block["personal"] = {"avatar": dict(avatar or {}), "12345": {"team": own.get("team", 1)}}

    return data + _block([results_block, players_info, {}])
