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
        self.teams: dict[int, dict] = {}
        self.members: dict[tuple[int, int], str] = {}  # (team_id, user_id) -> role
        self.invites: dict[str, dict] = {}
        self._next_team_id = 1
        self.explanations = []

    # --- scope: mirrors database._scope_join ---
    def _teammates(self, user_id):
        teams = {t for t, u in self.members if u == user_id}
        return {u for t, u in self.members if t in teams} | {user_id}

    def _in_scope(self, scope, battle):
        from database import _as_scope
        scope = _as_scope(scope)
        if scope.mode and battle.get("mode") != scope.mode:
            return False
        day = battle["date_time"][:10]
        if (scope.date_from and day < scope.date_from) or (scope.date_to and day > scope.date_to):
            return False
        uploaders = {u for u, b in self.owners if b == battle["id"]}
        if scope.kind == "all":
            return True
        if scope.kind == "mine":
            return scope.user_id in uploaders
        if scope.kind == "team":
            return bool(uploaders & {u for t, u in self.members if t == scope.team_id})
        return bool(uploaders & self._teammates(scope.user_id))

    def _counts(self, battle, player):
        """database._SQUAD_FOCUS: in a matchmade battle only uploaders and their platoon."""
        if battle.get("mode") not in ("standard", "onslaught"):
            return True
        uploaders = {(self.recorders.get((u, battle["id"])) or {}).get("playerName")
                     for u, b in self.owners if b == battle["id"]}
        if player["name"] in uploaders:
            return True
        platoon = (player.get("details") or {}).get("prebattleID")
        return bool(platoon) and any(
            p["name"] in uploaders and (p.get("details") or {}).get("prebattleID") == platoon
            for p in battle.get("players", []))

    # --- auth ---
    def create_user(self, username, password_hash, consent_version=None):
        if any(u.lower() == username.lower() for u in self.users):
            return None
        user = {"id": self._next_user_id, "username": username, "password_hash": password_hash,
                "token_version": 0, "consent_version": consent_version}
        self._next_user_id += 1
        self.users[username] = user
        return {"id": user["id"], "username": user["username"]}

    def get_user_by_username(self, username):
        return next((u for name, u in self.users.items() if name.lower() == username.lower()), None)

    def _by_id(self, user_id):
        return next((u for u in self.users.values() if u["id"] == user_id), None)

    def get_user_auth(self, user_id):
        user = self._by_id(user_id)
        return dict(user) if user else None

    def set_password(self, user_id, password_hash):
        user = self._by_id(user_id)
        user["password_hash"] = password_hash
        user["token_version"] += 1
        return user["token_version"]

    def bump_token_version(self, user_id):
        user = self._by_id(user_id)
        user["token_version"] += 1
        return user["token_version"]

    def record_consent(self, user_id, version):
        self._by_id(user_id)["consent_version"] = version

    def delete_user(self, user_id):
        mine = {b for u, b in self.owners if u == user_id}
        others = {b for u, b in self.owners if u != user_id}
        for b in mine - others:
            self.battles.pop(b, None)
        moved = deleted = 0
        for (t, u), role in list(self.members.items()):
            if u == user_id and role == "owner":
                heirs = sorted(x for (tt, x) in self.members if tt == t and x != user_id)
                if heirs:
                    self.members[(t, heirs[0])] = "owner"
                    moved += 1
                else:
                    self.delete_team(t)
                    deleted += 1
        self.members = {k: v for k, v in self.members.items() if k[1] != user_id}
        self.owners = {(u, b) for u, b in self.owners if u != user_id}
        self.hidden = {(u, n) for u, n in self.hidden if u != user_id}
        self.users = {n: u for n, u in self.users.items() if u["id"] != user_id}
        return {"battles_deleted": len(mine - others), "teams_handed_over": moved, "teams_deleted": deleted}

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

    def get_battles_export(self, scope=None, viewer_id=None, date_from=None, date_to=None):
        rows = []
        for b in sorted(self.battles.values(), key=lambda b: (b["date_time"], b["id"])):
            if not self._in_scope(scope, b):
                continue
            day = b["date_time"][:10]
            if (date_from and day < date_from) or (date_to and day > date_to):
                continue
            mine = (viewer_id, b["id"]) in self.owners
            rows.append({**b, "details": b.get("details") or None,
                         "recorder": self.recorders.get((viewer_id, b["id"])) if mine else None,
                         "mine": mine})
        return rows

    def get_battles_list(self, scope=None, viewer_id=None):
        rows = []
        for b in self.battles.values():
            if not self._in_scope(scope, b):
                continue
            rows.append({
                "id": b["id"], "date_time": b["date_time"], "map_name": b["map_name"], "mode": b.get("mode"),
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
    def get_db_totals(self, scope=None):
        rows = [b for b in self.battles.values() if self._in_scope(scope, b)]
        return {
            "total_battles": len(rows),
            "wins": sum(1 for b in rows if b["result"] == "win"),
            "losses": sum(1 for b in rows if b["result"] == "loss"),
            "draws": sum(1 for b in rows if b["result"] == "draw"),
            "first_battle": min((b["date_time"] for b in rows), default=None),
            "last_battle": max((b["date_time"] for b in rows), default=None),
        }

    def _team_names(self, scope=None):
        counts = {}
        for b in self.battles.values():
            if not self._in_scope(scope, b):
                continue
            for p in b.get("players", []):
                if p["team"] == b["player_team"] and self._counts(b, p):
                    counts[p["name"]] = counts.get(p["name"], 0) + 1
        return counts

    def count_players(self, scope=None, viewer_id=None):
        hidden = set(self.get_hidden_players(viewer_id)) if viewer_id else set()
        return len(set(self._team_names(scope)) - hidden)

    def get_player_summary(self, scope=None, viewer_id=None, min_battles=1):
        hidden = set(self.get_hidden_players(viewer_id)) if viewer_id else set()
        counts = self._team_names(scope)
        return [
            {"name": n, "battles": c, "avg_damage": 0}
            for n, c in sorted(counts.items())
            if n not in hidden and c >= min_battles
        ]

    def get_daily_summary(self, scope=None, viewer_id=None):
        days = {}
        for b in self.battles.values():
            if self._in_scope(scope, b):
                d = days.setdefault(b["date_time"][:10], {"battles": 0, "wins": 0})
                d["battles"] += 1
                d["wins"] += b["result"] == "win"
        rows = [{"day": k, **v} for k, v in sorted(days.items())]
        return {"days": rows, "totals": {"battles": sum(r["battles"] for r in rows),
                                         "wins": sum(r["wins"] for r in rows)}}

    def get_map_summary(self, scope=None):
        return []

    def get_vehicle_summary(self, scope=None, viewer_id=None, names=None):
        return [{"name": n, "vehicle": "T-34"} for n in (names or [])]

    def ping(self):
        return True

    # --- AI analysis ---
    def get_period_rows(self, scope=None, viewer_id=None):
        hidden = set(self.get_hidden_players(viewer_id)) if viewer_id else set()
        rows = []
        for b in sorted(self.battles.values(), key=lambda b: (b["date_time"], b["id"])):
            if not self._in_scope(scope, b):
                continue
            for p in b.get("players", []):
                if p["team"] != b["player_team"]:
                    continue
                d = p.get("details") or {}
                rows.append({
                    "battle_id": b["id"], "date_time": b["date_time"], "result": b["result"],
                    "map_name": b["map_name"], "mode": b.get("mode"), "duration": 420,
                    "name": p["name"], "vehicle": p["vehicle"], "tank_id": d.get("typeCompDescr"),
                    "damage_dealt": p.get("damage_dealt", 0), "assists": p.get("assists", 0),
                    "frags": p.get("frags", 0), "blocked": p.get("blocked", 0),
                    "damage_received": p.get("damage_received", 0), "survived": p.get("survived", True),
                    "shots": p.get("shots", 0), "hits": p.get("hits", 0), "piercings": p.get("piercings", 0),
                    "spotted": d.get("spotted"), "lifetime": d.get("lifeTime"), "prestige": None,
                    "platoon": str(d.get("prebattleID") or 0),
                    "focus": self._counts(b, p) and p["name"] not in hidden,
                })
        return rows

    def get_rating_rows(self, scope, user_id):
        return []

    def get_recorder_names(self, user_id):
        names = [(r or {}).get("playerName") for (u, _), r in self.recorders.items() if u == user_id]
        return sorted({n for n in names if n}, key=lambda n: -names.count(n))

    def get_explanation(self, cache_key):
        return next((e for e in reversed(self.explanations)
                     if e["cache_key"] == cache_key and e["status"] == "ok"), None)

    def start_explanation(self, user_id, cache_key, mode, model):
        self.explanations.append({"user_id": user_id, "cache_key": cache_key, "mode": mode,
                                  "status": "pending", "answer": None, "model": model,
                                  "created_at": "2026-11-10T12:00:00"})
        return len(self.explanations) - 1

    def finish_explanation(self, explanation_id, status, answer=None, problems=None, model=None,
                           usage=None, error_kind=None, error=None, duration_ms=None):
        self.explanations[explanation_id].update(status=status, answer=answer, problems=problems,
                                                 usage=usage, error_kind=error_kind, error=error,
                                                 duration_ms=duration_ms)

    def get_explanation_log(self, limit=100):
        recent = [{"id": i, **e} for i, e in enumerate(self.explanations)][::-1][:limit]
        return {"recent": recent, "by_day": [], "errors_7d": []}

    def count_explanations(self, user_id, hours=24):
        return sum(1 for e in self.explanations if user_id is None or e["user_id"] == user_id)

    # --- teams ---
    def create_team(self, name, owner_id):
        team_id = self._next_team_id
        self._next_team_id += 1
        self.teams[team_id] = {"id": team_id, "name": name, "created_at": None}
        self.members[(team_id, owner_id)] = "owner"
        return {"id": team_id, "name": name, "role": "owner", "members": 1}

    def get_user_teams(self, user_id):
        return [{**self.teams[t], "role": role, "members": sum(1 for x, _ in self.members if x == t)}
                for (t, u), role in sorted(self.members.items()) if u == user_id]

    def get_team(self, team_id):
        return self.teams.get(team_id)

    def get_member_role(self, team_id, user_id):
        return self.members.get((team_id, user_id))

    def get_team_members(self, team_id):
        names = {u["id"]: u["username"] for u in self.users.values()}
        return [{"user_id": u, "username": names.get(u), "role": role, "joined_at": None,
                 "battles": sum(1 for o, _ in self.owners if o == u)}
                for (t, u), role in sorted(self.members.items()) if t == team_id]

    def rename_team(self, team_id, name):
        self.teams[team_id]["name"] = name

    def delete_team(self, team_id):
        self.teams.pop(team_id, None)
        self.members = {k: v for k, v in self.members.items() if k[0] != team_id}
        self.invites = {c: i for c, i in self.invites.items() if i["team_id"] != team_id}

    def remove_member(self, team_id, user_id):
        if self.members.get((team_id, user_id)) in (None, "owner"):
            return False
        del self.members[(team_id, user_id)]
        return True

    def _invite_ok(self, invite):
        from datetime import datetime, timezone
        return (invite["expires_at"] > datetime.now(timezone.utc)
                and (invite["max_uses"] is None or invite["uses"] < invite["max_uses"]))

    def create_invite(self, team_id, created_by, code, days, max_uses):
        from datetime import datetime, timedelta, timezone
        self.invites[code] = {"code": code, "team_id": team_id, "max_uses": max_uses, "uses": 0,
                              "expires_at": datetime.now(timezone.utc) + timedelta(days=days)}
        return {k: v for k, v in self.invites[code].items() if k != "team_id"}

    def list_invites(self, team_id):
        return [{k: v for k, v in i.items() if k != "team_id"}
                for i in self.invites.values() if i["team_id"] == team_id and self._invite_ok(i)]

    def revoke_invite(self, team_id, code):
        if self.invites.get(code, {}).get("team_id") != team_id:
            return False
        del self.invites[code]
        return True

    def get_invite(self, code):
        invite = self.invites.get(code)
        if not invite or not self._invite_ok(invite):
            return None
        return {"code": code, "team_id": invite["team_id"],
                "team_name": self.teams[invite["team_id"]]["name"], "expires_at": invite["expires_at"]}

    def accept_invite(self, code, user_id):
        invite = self.invites.get(code)
        if not invite or not self._invite_ok(invite):
            return "invalid", None
        key = (invite["team_id"], user_id)
        if key in self.members:
            return "already", invite["team_id"]
        self.members[key] = "member"
        invite["uses"] += 1
        return "joined", invite["team_id"]

    def get_admin_overview(self):
        return {"battles": len(self.battles), "users": len(self.users), "teams": len(self.teams),
                "player_rows": 0, "accounts": 0, "by_mode": [], "by_day": []}


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
        "count_players", "get_daily_summary", "get_user_auth", "set_password", "bump_token_version", "record_consent",
        "delete_user", "create_team", "get_user_teams", "get_team", "get_member_role",
        "get_team_members", "rename_team", "delete_team", "remove_member", "create_invite",
        "list_invites", "revoke_invite", "get_invite", "accept_invite", "get_admin_overview",
        "get_period_rows", "get_rating_rows", "get_recorder_names", "get_explanation",
        "start_explanation", "finish_explanation", "count_explanations", "get_explanation_log",
    ):
        monkeypatch.setattr(app_module, name, getattr(db, name))
    for limiter in (app_module._login_failures, app_module._registrations, app_module._uploaded_files):
        limiter.clear()
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
        res = client.post("/api/auth/register",
                          json={"username": username, "password": password, "consent": True})
        assert res.status_code == 200, res.text
        return {"Authorization": f"Bearer {res.json()['token']}"}
    return _register
