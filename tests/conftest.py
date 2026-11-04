import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

# config reads the environment at import time, so set it before anything imports it.
os.environ.setdefault("SECRET_KEY", "test-secret-key-of-sufficient-length-000")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENV", "test")


class FakeDB:
    """In-memory stand-in for src/database.py.

    It reproduces the ownership rules the API depends on, so endpoint behaviour can be
    tested without Postgres. The SQL itself is only exercised against a real database.
    """

    def __init__(self):
        self.users: dict[str, dict] = {}
        self.battles: dict[int, dict] = {}
        self.owners: set[tuple[int, int]] = set()
        self.hidden: set[tuple[int, str]] = set()
        self.recorders: dict[tuple[int, int], dict] = {}
        self._next_user_id = 1
        self._next_battle_id = 1

    # --- auth ---
    def create_user(self, username, password_hash):
        if any(u.lower() == username.lower() for u in self.users):
            return None
        user = {"id": self._next_user_id, "username": username, "password_hash": password_hash}
        self._next_user_id += 1
        self.users[username] = user
        return {"id": user["id"], "username": user["username"]}

    def get_user_by_username(self, username):
        return next((u for name, u in self.users.items() if name.lower() == username.lower()), None)

    # --- battles ---
    def save_battle(self, battle, user_id):
        existing = next(
            (b for b in self.battles.values() if b["battle_hash"] == battle["battle_hash"]), None
        )
        if existing:
            self.recorders.setdefault((user_id, existing["id"]), battle.get("recorder"))
            if (user_id, existing["id"]) in self.owners:
                return "already"
            self.owners.add((user_id, existing["id"]))
            return "linked"
        battle_id = self._next_battle_id
        self._next_battle_id += 1
        self.battles[battle_id] = {**{k: v for k, v in battle.items() if k != "recorder"}, "id": battle_id}
        self.owners.add((user_id, battle_id))
        self.recorders[(user_id, battle_id)] = battle.get("recorder")
        return "created"

    def get_battles_export(self, user_id=None, viewer_id=None, date_from=None, date_to=None):
        rows = []
        for b in sorted(self.battles.values(), key=lambda b: (b["date_time"], b["id"])):
            if user_id and (user_id, b["id"]) not in self.owners:
                continue
            day = b["date_time"][:10]
            if (date_from and day < date_from) or (date_to and day > date_to):
                continue
            mine = (viewer_id, b["id"]) in self.owners
            rows.append({**b, "details": b.get("details") or None,
                         "recorder": self.recorders.get((viewer_id, b["id"])) if mine else None,
                         "mine": mine})
        return rows

    def get_battles_list(self, user_id=None, viewer_id=None):
        rows = []
        for b in self.battles.values():
            if user_id and (user_id, b["id"]) not in self.owners:
                continue
            rows.append({
                "id": b["id"], "date_time": b["date_time"], "map_name": b["map_name"],
                "result": b["result"], "filename": b["filename"],
                "player_count": len(b.get("players", [])),
                "mine": (viewer_id, b["id"]) in self.owners,
            })
        return sorted(rows, key=lambda r: r["date_time"], reverse=True)

    def delete_battle(self, battle_id, user_id):
        if (user_id, battle_id) not in self.owners:
            return "not_found"
        self.owners.discard((user_id, battle_id))
        if any(bid == battle_id for _, bid in self.owners):
            return "unlinked"
        self.battles.pop(battle_id, None)
        return "deleted"

    # --- hidden players ---
    def get_hidden_players(self, user_id):
        return sorted(name for uid, name in self.hidden if uid == user_id)

    def hide_player(self, user_id, name):
        self.hidden.add((user_id, name))

    def unhide_player(self, user_id, name):
        if (user_id, name) not in self.hidden:
            return False
        self.hidden.discard((user_id, name))
        return True

    # --- analytics (shape only; aggregation is SQL and needs a real database) ---
    def get_db_totals(self, user_id=None):
        rows = [b for b in self.battles.values()
                if not user_id or (user_id, b["id"]) in self.owners]
        return {
            "total_battles": len(rows),
            "wins": sum(1 for b in rows if b["result"] == "win"),
            "losses": sum(1 for b in rows if b["result"] == "loss"),
            "draws": sum(1 for b in rows if b["result"] == "draw"),
            "first_battle": min((b["date_time"] for b in rows), default=None),
            "last_battle": max((b["date_time"] for b in rows), default=None),
        }

    def _team_names(self, user_id=None):
        counts = {}
        for b in self.battles.values():
            if user_id and (user_id, b["id"]) not in self.owners:
                continue
            for p in b.get("players", []):
                if p["team"] == b["player_team"]:
                    counts[p["name"]] = counts.get(p["name"], 0) + 1
        return counts

    def count_players(self, user_id=None, viewer_id=None):
        hidden = set(self.get_hidden_players(viewer_id)) if viewer_id else set()
        return len(set(self._team_names(user_id)) - hidden)

    def get_player_summary(self, user_id=None, viewer_id=None, min_battles=1):
        hidden = set(self.get_hidden_players(viewer_id)) if viewer_id else set()
        counts = self._team_names(user_id)
        return [
            {"name": n, "battles": c, "avg_damage": 0}
            for n, c in sorted(counts.items())
            if n not in hidden and c >= min_battles
        ]

    def get_map_summary(self, user_id=None):
        return []

    def get_vehicle_summary(self, user_id=None, viewer_id=None, names=None):
        return [{"name": n, "vehicle": "T-34"} for n in (names or [])]

    def ping(self):
        return True


@pytest.fixture
def fake_db(monkeypatch):
    """Swap the database layer out for FakeDB and mark the database as available."""
    import app as app_module
    import database as database_module

    db = FakeDB()
    monkeypatch.setattr(app_module, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app_module, "init_db", lambda: None)
    monkeypatch.setattr(database_module, "ping", db.ping)
    monkeypatch.setattr(database_module, "close_pool", lambda: None)
    for name in (
        "create_user", "get_user_by_username", "save_battle", "get_battles_list",
        "delete_battle", "get_hidden_players", "hide_player", "unhide_player",
        "get_db_totals", "get_player_summary", "get_map_summary", "get_vehicle_summary",
        "count_players", "get_battles_export",
    ):
        monkeypatch.setattr(app_module, name, getattr(db, name))
    app_module._login_failures.clear()
    return db


@pytest.fixture
def client(fake_db):
    from fastapi.testclient import TestClient
    import app as app_module

    with TestClient(app_module.app) as c:
        yield c


@pytest.fixture
def auth(client):
    """Register a user and return a callable that builds its Authorization header."""
    def _register(username="tester", password="secret123"):
        res = client.post("/api/auth/register", json={"username": username, "password": password})
        assert res.status_code == 200, res.text
        return {"Authorization": f"Bearer {res.json()['token']}"}
    return _register
