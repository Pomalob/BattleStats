"""Vehicle list from the installed "Мир танков" client, for export_to_table.py --refs.

Usage:
    python game_vehicles.py D:\\Games\\Tanki                   # -> game_vehicles.json
    python game_vehicles.py D:\\Games\\Tanki -o vehicles.json

Runs where the game is installed; the JSON it writes is carried to wherever export_to_table.py
runs. Standard library only.

res/packages/scripts.pkg is a zip; scripts/item_defs/vehicles/<nation>/list.xml inside is BigWorld
"packed XML". Tier comes from <level>, class and role from <tags>, premium from a gold price.
The vehicle <id> gives the replay's typeCompDescr: (id << 8) + (nation index << 4) + 1.
"""
import argparse
import json
import struct
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# Order matters: the index is part of tank_id.
NATIONS = ("ussr", "germany", "usa", "china", "france", "uk", "japan", "czech", "sweden",
           "poland", "italy", "intunion")
NATION_NAMES = {
    "ussr": "СССР", "germany": "Германия", "usa": "США", "china": "Китай", "france": "Франция",
    "uk": "Великобритания", "japan": "Япония", "czech": "Чехословакия", "sweden": "Швеция",
    "poland": "Польша", "italy": "Италия", "intunion": "Сборная нация",
}
CLASSES = {"heavyTank": "ТТ", "mediumTank": "СТ", "lightTank": "ЛТ", "AT-SPG": "ПТ-САУ", "SPG": "САУ"}
ROLES = {
    "role_HT_assault": "Штурмовой ТТ", "role_HT_break": "ТТ прорыва", "role_HT_support": "ТТ поддержки",
    "role_HT_universal": "Универсальный ТТ", "role_MT_assault": "Штурмовой СТ", "role_MT_sniper": "Снайперский СТ",
    "role_MT_support": "СТ поддержки", "role_MT_universal": "Универсальный СТ", "role_LT_universal": "Универсальный ЛТ",
    "role_LT_wheeled": "Колёсный ЛТ", "role_ATSPG_assault": "Штурмовая ПТ-САУ", "role_ATSPG_sniper": "Снайперская ПТ-САУ",
    "role_ATSPG_support": "ПТ-САУ поддержки", "role_ATSPG_universal": "Универсальная ПТ-САУ", "role_SPG": "САУ",
    "role_SPG_assault": "Штурмовая САУ", "role_SPG_flame": "Огнемётная САУ",
}
PACKED_XML_MAGIC = b"\x45\x4e\xa1\x62"


def unpack_xml(data: bytes) -> ET.Element:
    """BigWorld packed XML -> ElementTree element. Layout: magic, a version byte, a zero-terminated
    name table, then the root element: child count (u16), own value descriptor, child descriptors
    (name index u16 + descriptor), then the values. A descriptor is u32: end offset in the low 28 bits,
    value type in the high 4 (0 element, 1 string, 2 integer, 3 floats, 4 boolean, 5 binary)."""
    if data[:4] != PACKED_XML_MAGIC:
        raise ValueError("не packed XML")
    pos, names = 5, []
    while True:
        end = data.index(b"\0", pos)
        name, pos = data[pos:end].decode("latin-1"), end + 1
        if not name:
            break
        names.append(name)

    def element(node, at):
        count = struct.unpack_from("<H", data, at)[0]
        own = struct.unpack_from("<I", data, at + 2)[0]
        kids = [struct.unpack_from("<HI", data, at + 6 + i * 6) for i in range(count)]
        start = at + 6 + count * 6
        offset = value(node, start, 0, own)
        for name_index, desc in kids:
            offset = value(ET.SubElement(node, names[name_index]), start, offset, desc)

    def value(node, start, offset, desc):
        end, kind = desc & 0x0FFFFFFF, desc >> 28
        raw = data[start + offset:start + end]
        if kind == 0:
            element(node, start + offset)
        elif kind == 1:
            node.text = raw.decode("utf-8", "replace")
        elif kind == 2:
            node.text = str(int.from_bytes(raw, "little", signed=True) if raw else 0)
        elif kind == 3:
            node.text = " ".join(f"{x:g}" for x in struct.unpack(f"<{len(raw) // 4}f", raw))
        elif kind == 4:
            node.text = "true" if raw == b"\x01" else "false"
        else:
            node.text = raw.hex()
        return end

    root = ET.Element("root")
    element(root, pos)
    return root


def read_vehicles(game_dir: Path) -> list[dict]:
    pkg = Path(game_dir) / "res" / "packages" / "scripts.pkg"
    if not pkg.exists():
        sys.exit(f"Нет {pkg}: укажи папку игры (где лежит Tanki.exe)")
    vehicles = []
    with zipfile.ZipFile(pkg) as z:
        names = set(z.namelist())
        for index, nation in enumerate(NATIONS):
            name = f"scripts/item_defs/vehicles/{nation}/list.xml"
            if name not in names:
                continue
            for v in unpack_xml(z.read(name)):
                if v.find("id") is None or v.find("level") is None:
                    continue  # xmlns and other service entries
                tags = (v.findtext("tags") or "").split()
                price = v.find("price")
                vehicles.append({
                    "tank_id": (int(v.findtext("id")) << 8) + (index << 4) + 1,
                    "tag": v.tag,
                    "nation": NATION_NAMES[nation],
                    "vehicle_class": next((CLASSES[t] for t in tags if t in CLASSES), None),
                    "tier": int(v.findtext("level")),
                    "is_premium": int(price is not None and price.find("gold") is not None),
                    "is_wheeled": int("wheeledVehicle" in tags),
                    "role": next((ROLES.get(t, t) for t in tags if t.startswith("role_")), None),
                })
    return vehicles


def client_version(game_dir: Path) -> str | None:
    try:
        root = ET.parse(Path(game_dir) / "version.xml").getroot()
        return " ".join((root.findtext("version") or "").split()) or None
    except (OSError, ET.ParseError):
        return None


def main():
    parser = argparse.ArgumentParser(description="Список техники из установленного клиента -> JSON")
    parser.add_argument("game_dir", type=Path, help="папка игры, где лежит Tanki.exe")
    parser.add_argument("-o", "--output", type=Path, default=Path("game_vehicles.json"),
                        help="куда сохранить (по умолчанию game_vehicles.json)")
    args = parser.parse_args()
    vehicles = read_vehicles(args.game_dir)
    body = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "client_version": client_version(args.game_dir),
        "vehicles": vehicles,
    }
    args.output.write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{args.output}: {len(vehicles)} машин, клиент {body['client_version'] or '?'}")


if __name__ == "__main__":
    main()
