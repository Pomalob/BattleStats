"""scripts/game_vehicles.py reads the installed game; export_to_table.py --refs uses its JSON."""
import importlib.util
import json
import struct
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent


def _load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pack_xml(tree) -> bytes:
    """The writing side of BigWorld packed XML, for tests. tree = (own_value, [(name, subtree), ...]);
    a value is str (string), int (integer), None (empty) or a subtree tuple (element)."""
    names = []

    def name_index(name):
        if name not in names:
            names.append(name)
        return names.index(name)

    def encode_value(v):
        if isinstance(v, tuple):
            return 0, encode_element(v)
        if v is None:
            return 1, b""
        if isinstance(v, int):
            return 2, struct.pack("<i", v)
        return 1, v.encode("utf-8")

    def encode_element(node):
        own, children = node
        kind, own_raw = encode_value(own)
        blobs, descs, offset = [own_raw], [], len(own_raw)
        own_desc = (kind << 28) | offset
        for name, child in children:
            kind, raw = encode_value(child)
            offset += len(raw)
            blobs.append(raw)
            descs.append(struct.pack("<HI", name_index(name), (kind << 28) | offset))
        return struct.pack("<HI", len(children), own_desc) + b"".join(descs) + b"".join(blobs)

    body = encode_element(tree)
    table = b"".join(n.encode() + b"\0" for n in names) + b"\0"
    return b"\x45\x4e\xa1\x62\x00" + table + body


def _vehicle(vid, level, tags, gold=False):
    price = (25000, [("gold", None)]) if gold else 356700
    return (None, [("id", vid), ("userString", "#x"), ("price", price), ("tags", tags), ("level", level)])


@pytest.fixture
def game_dir(tmp_path):
    ussr = pack_xml((None, [
        ("xmlns:xmlref", (None, [])),  # service entry without id/level, as in the real file
        ("R04_T-34", _vehicle(0, 5, "mediumTank HD")),
        ("R231_Object_278", _vehicle(31, 10, "heavyTank role_HT_break special", gold=True)),
    ]))
    germany = pack_xml((None, [("G02_Hummel", _vehicle(1, 6, "SPG role_SPG"))]))
    pkg = tmp_path / "res" / "packages" / "scripts.pkg"
    pkg.parent.mkdir(parents=True)
    with zipfile.ZipFile(pkg, "w") as z:
        z.writestr("scripts/item_defs/vehicles/ussr/list.xml", ussr)
        z.writestr("scripts/item_defs/vehicles/germany/list.xml", germany)
    (tmp_path / "version.xml").write_text("<version.xml><version> v.1.46.0.0 #2318 </version></version.xml>")
    return tmp_path


def test_unpack_xml_reads_nested_elements_and_types():
    gv = _load("game_vehicles")
    root = gv.unpack_xml(pack_xml((None, [("a", 7), ("b", "текст"), ("c", (None, [("d", -3)]))])))
    assert root.findtext("a") == "7" and root.findtext("b") == "текст" and root.findtext("c/d") == "-3"


def test_vehicles_get_the_replays_tank_id_and_attributes(game_dir):
    vehicles = {v["tank_id"]: v for v in _load("game_vehicles").read_vehicles(game_dir)}
    # tank_id = (id << 8) + (nation index << 4) + 1, as typeCompDescr in the replays: T-34 is 1, Object 278 is 7937
    assert vehicles[1] == {"tank_id": 1, "tag": "R04_T-34", "nation": "СССР", "vehicle_class": "СТ", "tier": 5,
                           "is_premium": 0, "is_wheeled": 0, "role": None}
    assert vehicles[7937]["role"] == "ТТ прорыва" and vehicles[7937]["is_premium"] == 1
    hummel = (1 << 8) + (1 << 4) + 1
    assert vehicles[hummel]["vehicle_class"] == "САУ" and vehicles[hummel]["nation"] == "Германия"
    assert len(vehicles) == 3  # the service entry is skipped


def test_a_wrong_game_folder_is_an_explicit_error(tmp_path):
    with pytest.raises(SystemExit, match="Tanki.exe"):
        _load("game_vehicles").read_vehicles(tmp_path)


def test_the_json_carries_over_to_export_to_table(game_dir, tmp_path, monkeypatch):
    gv = _load("game_vehicles")
    out = tmp_path / "game_vehicles.json"
    monkeypatch.setattr("sys.argv", ["game_vehicles.py", str(game_dir), "-o", str(out)])
    gv.main()
    body = json.loads(out.read_text(encoding="utf-8"))
    assert body["client_version"] == "v.1.46.0.0 #2318" and len(body["vehicles"]) == 3

    script = _load("export_to_table")
    loaded = script.load_game_vehicles(out)
    existing = {7937: {"tank_id": 7937, "role": "Моя роль"}}
    replay = {7937: {"tank_id": 7937, "vehicle": "Объект 278"}, 1: {"tank_id": 1, "vehicle": "Т-34"}}
    rows = {r["tank_id"]: r for r in script.merge_refs(existing, replay, loaded, script.VEHICLE_REF_COLUMNS)}
    assert rows[7937]["role"] == "Моя роль" and rows[7937]["vehicle_class"] == "ТТ"  # a hand-written role wins
    assert rows[1]["role"] is None and rows[1]["tier"] == 5


def test_export_to_table_has_no_game_client_code():
    """The client is read only on the machine with the game; export_to_table runs elsewhere."""
    source = (ROOT / "scripts" / "export_to_table.py").read_text(encoding="utf-8")
    assert "unpack_xml" not in source and "scripts.pkg" not in source
