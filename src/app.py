import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import json
import re
import secrets
import threading
import time
import traceback
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse

import config
import database
import tickets
from auth import create_token, decode_token, hash_password, verify_password
from database import (
    Scope, accept_invite, count_players, create_invite, create_team, create_user, delete_battle,
    delete_team, get_admin_overview, get_battles_list, get_db_totals, get_hidden_players, get_invite,
    get_map_summary, get_member_role, get_player_summary, get_team, get_team_members, get_user_by_username,
    get_user_teams, get_vehicle_summary, hide_player, init_db, list_invites, remove_member, rename_team,
    revoke_invite, save_battle, unhide_player, get_daily_summary, get_user_auth, set_password, bump_token_version,
    record_consent, delete_user,
)
from replay_parser import (
    BATTLE_COMMON_FIELDS, BATTLE_META_FIELDS, PLAYER_ACCOUNT_FIELDS, PLAYER_RESULT_FIELDS,
    MODES, PLAYER_ROSTER_FIELDS, RECORDER_AVATAR_FIELDS, RECORDER_FIELDS, battle_mode, parse_replay_bytes,
)

# The site is a React app (frontend/, built by Vite into frontend/dist); see serve_frontend.
FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"

_DB_AVAILABLE = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _DB_AVAILABLE
    print(f"[DB] DATABASE_URL set: {bool(config.DATABASE_URL)}")
    try:
        init_db()
        _DB_AVAILABLE = True
        print("[DB] init OK")
    except Exception as e:
        print(f"[DB] init failed: {e}")
        traceback.print_exc()
    yield
    database.close_pool()


# The interactive API docs are for development; in production they only map the API for strangers.
_docs = {} if not config.IS_PRODUCTION else {"docs_url": None, "redoc_url": None, "openapi_url": None}
app = FastAPI(title="StatsBattle", lifespan=lifespan, **_docs)


# Pages carry their scripts inline, so script-src keeps 'unsafe-inline'; the rest is locked down.
# Fonts are served from /static/fonts: nothing on a page loads from another site.
_SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "font-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    # Invite links carry their code in the path: never send it to another site.
    "Referrer-Policy": "same-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
}


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    for name, value in _SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)
    if config.IS_PRODUCTION:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000")
    return response
# Analytics over a few hundred battles is a lot of repetitive JSON.
app.add_middleware(GZipMiddleware, minimum_size=4096)


# --- Auth helpers ---

def _current_user(authorization: str | None) -> dict | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    payload = decode_token(authorization.split(" ", 1)[1])
    if not payload:
        return None
    try:
        user_id, version = int(payload["sub"]), int(payload.get("ver", 0))
    except (KeyError, TypeError, ValueError):
        return None
    # The signature alone is not enough: the account may have been deleted, or signed out
    # everywhere / changed its password since this token was issued.
    account = get_user_auth(user_id)
    if not account or account["token_version"] != version:
        return None
    user = {"id": user_id, "username": account["username"],
            "consent_required": account.get("consent_version") != config.CONSENT_VERSION}
    # Read on every request, so adding or removing an admin needs no new token.
    user["is_admin"] = user["id"] in config.ADMIN_USER_IDS
    return user


def require_user(authorization: str | None = Header(default=None)) -> dict:
    """FastAPI dependency: the signed-in user, or 401."""
    if not _DB_AVAILABLE:
        raise HTTPException(503, "База данных недоступна")
    user = _current_user(authorization)
    if not user:
        raise HTTPException(401, "Нужно войти в аккаунт")
    return user


def require_db() -> None:
    if not _DB_AVAILABLE:
        raise HTTPException(503, "База данных недоступна")


# --- Rate limits ---
# In-memory and therefore per-instance: enough for a single-container deployment,
# not a substitute for a shared limiter once there are several instances.

def client_ip(request: Request) -> str:
    """The visitor's address. Behind TRUSTED_PROXY_HOPS proxies it is the entry that many hops
    from the right of X-Forwarded-For: the rightmost entries are written by our own proxies,
    anything to their left is client-supplied and could be anything."""
    peer = request.client.host if request.client else "unknown"
    hops = config.TRUSTED_PROXY_HOPS
    if hops <= 0:
        return peer
    chain = [part.strip() for part in request.headers.get("x-forwarded-for", "").split(",") if part.strip()]
    return chain[-hops] if len(chain) >= hops else peer


class SlidingWindow:
    """At most `limit` events per key in the last `window` seconds."""

    def __init__(self):
        self._events: dict[object, list[float]] = {}
        self._lock = threading.Lock()

    def count(self, key, window: float) -> int:
        cutoff = time.monotonic() - window
        with self._lock:
            recent = [t for t in self._events.get(key, []) if t > cutoff]
            if recent:
                self._events[key] = recent
            else:
                self._events.pop(key, None)
            return len(recent)

    def add(self, key, n: int = 1) -> None:
        now = time.monotonic()
        with self._lock:
            self._events.setdefault(key, []).extend([now] * n)
            if len(self._events) > 50_000:  # keep a flood of one-off keys from growing forever
                self._events.clear()

    def clear(self, key=None) -> None:
        with self._lock:
            if key is None:
                self._events.clear()
            else:
                self._events.pop(key, None)


_login_failures = SlidingWindow()   # keys: (ip, username) and ("ip", ip)
_registrations = SlidingWindow()    # key: ip
_uploaded_files = SlidingWindow()   # key: user id


def _throttle_key(request: Request, username: str) -> tuple[str, str]:
    return (client_ip(request), username.lower())


def _check_login_allowed(key: tuple[str, str]) -> None:
    window = config.LOGIN_LOCKOUT_SECONDS
    # Per name from this address, and per address across every name (password spraying).
    if (_login_failures.count(key, window) >= config.LOGIN_MAX_ATTEMPTS
            or _login_failures.count(("ip", key[0]), window) >= config.LOGIN_FAILURES_PER_IP):
        raise HTTPException(429, "Слишком много неудачных попыток, попробуй позже")


def _record_login_failure(key: tuple[str, str]) -> None:
    _login_failures.add(key)
    _login_failures.add(("ip", key[0]))


def _clear_login_failures(key: tuple[str, str]) -> None:
    _login_failures.clear(key)


def battle_to_dict(b) -> dict:
    return {
        "filename": b.filename,
        "map_name": b.map_name,
        "date_time": b.date_time,
        "battle_hash": b.battle_hash,
        "result": b.result,
        "winner_team": b.winner_team,
        "player_team": b.player_team,
        "player_vehicle": b.player_vehicle,
        "complete": b.complete,
        "details": b.details,
        "recorder": b.recorder,
        "players": [
            {
                "name": p.name,
                "vehicle": p.vehicle,
                "team": p.team,
                "damage_dealt": p.damage_dealt,
                "damage_received": p.damage_received,
                "blocked": p.blocked,
                "assists": p.assists,
                "frags": p.frags,
                "xp": p.xp,
                "survived": p.survived,
                "shots": p.shots,
                "hits": p.hits,
                "piercings": p.piercings,
                "details": p.details,
            }
            for p in b.players
        ],
    }


# --- Pages ---

@app.get("/healthz")
def healthz():
    """Liveness plus a database round-trip, for the platform health check."""
    db_ok = False
    if _DB_AVAILABLE:
        try:
            db_ok = database.ping()
        except Exception:
            db_ok = False
    status = 200 if db_ok else 503
    return JSONResponse({"status": "ok" if db_ok else "degraded", "database": db_ok}, status_code=status)


# --- Auth endpoints ---

async def json_body(request: Request) -> dict:
    """The body as JSON whatever Content-Type says: curl -d and some scripts send other ones."""
    try:
        data = json.loads(await request.body() or b"{}")
    except ValueError:
        raise HTTPException(400, "Тело запроса должно быть JSON")
    if not isinstance(data, dict):
        raise HTTPException(400, "Тело запроса должно быть JSON-объектом")
    return data


@app.post("/api/auth/register")
def register(request: Request, body: dict = Depends(json_body)):
    require_db()
    ip = client_ip(request)
    if _registrations.count(ip, 3600) >= config.REGISTRATIONS_PER_HOUR:
        raise HTTPException(429, "Слишком много регистраций с этого адреса, попробуй через час")
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not 3 <= len(username) <= 32:
        raise HTTPException(400, "Имя пользователя — от 3 до 32 символов")
    if len(password) < 6:
        raise HTTPException(400, "Пароль — минимум 6 символов")
    # 152-FZ: consent to processing personal data, given explicitly, before the account exists.
    if body.get("consent") is not True:
        raise HTTPException(400, "Нужно согласие на обработку персональных данных")
    user = create_user(username, hash_password(password), config.CONSENT_VERSION)
    if not user:
        raise HTTPException(409, "Такое имя уже занято")
    _registrations.add(ip)
    _clear_login_failures(_throttle_key(request, username))
    return {"token": create_token(user["id"], user["username"]), "username": user["username"]}


@app.post("/api/auth/login")
def login(request: Request, body: dict = Depends(json_body)):
    require_db()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    key = _throttle_key(request, username)
    _check_login_allowed(key)

    user = get_user_by_username(username)
    if not user or not verify_password(password, user["password_hash"]):
        _record_login_failure(key)
        raise HTTPException(401, "Неверное имя пользователя или пароль")

    _clear_login_failures(key)
    token = create_token(user["id"], user["username"], user.get("token_version") or 0)
    return {"token": token, "username": user["username"]}


@app.get("/api/auth/me")
def me(user: dict = Depends(require_user)):
    return user


@app.post("/api/auth/consent")
def accept_consent(body: dict = Depends(json_body), user: dict = Depends(require_user)):
    """Accept the current consent text (accounts made before it, or after it changed)."""
    if body.get("consent") is not True:
        raise HTTPException(400, "Нужно согласие на обработку персональных данных")
    record_consent(user["id"], config.CONSENT_VERSION)
    return {"consent_version": config.CONSENT_VERSION}


def _check_password(user: dict, password) -> dict:
    account = get_user_auth(user["id"])
    if not account or not isinstance(password, str) or not verify_password(password, account["password_hash"]):
        raise HTTPException(403, "Неверный пароль")
    return account


@app.post("/api/auth/password")
def change_password(body: dict = Depends(json_body), user: dict = Depends(require_user)):
    """New password; every other session is signed out. Returns a fresh token for this one."""
    _check_password(user, body.get("old_password"))
    new = body.get("new_password") or ""
    if not isinstance(new, str) or len(new) < 6:
        raise HTTPException(400, "Пароль — минимум 6 символов")
    version = set_password(user["id"], hash_password(new))
    return {"token": create_token(user["id"], user["username"], version), "username": user["username"]}


@app.post("/api/auth/logout-all")
def logout_everywhere(user: dict = Depends(require_user)):
    """Void every token of this account, the current one included."""
    bump_token_version(user["id"])
    return {"signed_out": True}


@app.delete("/api/auth/me")
def delete_account(body: dict = Depends(json_body), user: dict = Depends(require_user)):
    """Delete the account and the battles only it uploaded. Asks for the password again:
    a stolen token alone must not be enough to wipe someone's history."""
    _check_password(user, body.get("password"))
    return delete_user(user["id"])


# --- Upload ---

async def _read_limited(upload: UploadFile, limit: int) -> bytes:
    """Read an upload into memory, refusing anything past `limit` bytes."""
    chunks, size = [], 0
    while chunk := await upload.read(1 << 20):
        size += len(chunk)
        if size > limit:
            raise ValueError(f"Файл больше {limit // (1024 * 1024)} МБ")
        chunks.append(chunk)
    return b"".join(chunks)


@app.post("/api/upload")
async def upload_replays(
    files: list[UploadFile] = File(...),
    user: dict = Depends(require_user),
):
    if len(files) > config.MAX_FILES_PER_UPLOAD:
        raise HTTPException(413, f"Слишком много файлов: максимум {config.MAX_FILES_PER_UPLOAD} за раз")
    # Parsing is the expensive part of the site; one account should not be able to hog it.
    if _uploaded_files.count(user["id"], 3600) + len(files) > config.UPLOAD_FILES_PER_HOUR:
        raise HTTPException(429, f"Лимит — {config.UPLOAD_FILES_PER_HOUR} реплеев в час, попробуй позже")
    _uploaded_files.add(user["id"], len(files))

    results, errors = [], []
    budget = config.MAX_UPLOAD_SIZE

    for i, upload in enumerate(files):
        name = upload.filename or "unnamed"
        if not name.lower().endswith(".mtreplay"):
            errors.append({"file": name, "error": "Это не файл .mtreplay"})
            continue
        try:
            data = await _read_limited(upload, min(config.MAX_FILE_SIZE, budget))
            budget -= len(data)
            # The client-supplied name is metadata only; nothing is ever written to disk.
            battle = battle_to_dict(parse_replay_bytes(data, Path(name).name))
            # What /api/battles/save will accept: this exact parse, signed (src/tickets.py).
            battle["ticket"] = tickets.issue(battle)
            results.append(battle)
        except ValueError as e:
            errors.append({"file": name, "error": str(e)})
        except Exception as e:
            traceback.print_exc()
            errors.append({"file": name, "error": f"Не удалось разобрать: {e}"})
        if budget <= 0 and i < len(files) - 1:
            errors.append({"file": "...", "error": "Достигнут лимит объёма загрузки, остальные файлы пропущены"})
            break

    results.sort(key=lambda r: r["date_time"], reverse=True)
    return {"battles": results, "errors": errors}


# Fields save_battle() needs, and the type each is coerced to before it reaches SQL.
_BATTLE_FIELDS = {
    "filename": str, "map_name": str, "date_time": str, "battle_hash": str,
    "player_team": int, "winner_team": int,
}
_PLAYER_FIELDS = {
    "name": str, "vehicle": str, "team": int, "damage_dealt": int, "damage_received": int,
    "blocked": int, "assists": int, "frags": int, "xp": int, "survived": bool,
    "shots": int, "hits": int, "piercings": int,
}
_MAX_TEXT = 200
_BATTLE_DETAIL_KEYS = frozenset((*BATTLE_META_FIELDS, "mapDisplayName", *BATTLE_COMMON_FIELDS))
_RECORDER_KEYS = frozenset((*RECORDER_FIELDS, *RECORDER_AVATAR_FIELDS))
_PLAYER_DETAIL_KEYS = frozenset(
    ("vehicleID", *PLAYER_ROSTER_FIELDS, *PLAYER_ACCOUNT_FIELDS, *PLAYER_RESULT_FIELDS)
)
_MAX_NESTED_ITEMS = 64


def _clean_value(value, depth: int = 0):
    """A JSON value as the replay could have written it: scalars, plus the short lists
    and maps some fields hold (badges, teamHealth). Anything else is dropped."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if abs(value) < 2 ** 63 else None
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    if isinstance(value, str):
        return value[:_MAX_TEXT]
    if depth >= 2:
        return None
    if isinstance(value, list):
        return [_clean_value(v, depth + 1) for v in value[:_MAX_NESTED_ITEMS]]
    if isinstance(value, dict):
        return {str(k)[:_MAX_TEXT]: _clean_value(v, depth + 1)
                for k, v in list(value.items())[:_MAX_NESTED_ITEMS]}
    return None


def _clean_details(raw, allowed: frozenset) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {k: _clean_value(v) for k, v in raw.items() if k in allowed}


def _coerce(value, kind):
    if kind is bool:
        return bool(value)
    if kind is int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    return str(value or "")[:_MAX_TEXT]


def _battle_result(winner_team: int, player_team: int) -> str:
    if winner_team == 0:
        return "draw"
    return "win" if winner_team == player_team else "loss"


def _clean_battle(raw) -> dict:
    """Rebuild a battle payload from scratch so only known, typed fields reach the database."""
    if not isinstance(raw, dict) or not isinstance(raw.get("players"), list):
        raise HTTPException(400, "Некорректные данные боя")
    battle = {k: _coerce(raw.get(k), kind) for k, kind in _BATTLE_FIELDS.items()}
    if not battle["battle_hash"]:
        raise HTTPException(400, "У боя нет battle_hash")
    if not battle["player_team"]:
        raise HTTPException(400, "В бою не определена команда игрока")
    # Derived here rather than taken from the client, so the stored result can never
    # contradict the teams it is supposed to follow from.
    battle["result"] = _battle_result(battle["winner_team"], battle["player_team"])

    battle["details"] = _clean_details(raw.get("details"), _BATTLE_DETAIL_KEYS)
    # Standard / clan / Onslaught, from the replay's bonusType: decides whose rows count.
    battle["mode"] = battle_mode(battle["details"])
    battle["recorder"] = _clean_details(raw.get("recorder"), _RECORDER_KEYS)

    battle["players"] = [
        {**{k: _coerce(p.get(k), kind) for k, kind in _PLAYER_FIELDS.items()},
         "details": _clean_details(p.get("details"), _PLAYER_DETAIL_KEYS)}
        for p in raw["players"] if isinstance(p, dict)
    ]
    if not battle["players"]:
        raise HTTPException(400, "В бою нет игроков")
    # An unfinished replay carries a roster and nothing else; storing it would add a
    # phantom draw and a zero row for every player in it.
    if raw.get("complete") is False or not any(
        p["shots"] or p["damage_dealt"] or p["damage_received"] or p["blocked"]
        for p in battle["players"]
    ):
        raise HTTPException(400, "Незавершённый реплей: в нём нет результатов боя")
    return battle


@app.post("/api/battles/save")
def save_battles(body: dict, user: dict = Depends(require_user)):
    """Store battles from the tickets /api/upload handed out. Battle JSON itself is refused:
    the numbers must come from the server's own parse of a replay, not from the client."""
    require_db()
    if "tickets" not in body and "battles" in body:
        raise HTTPException(400, "Бои сохраняются по билетам из /api/upload — загрузи реплеи заново")
    issued = body.get("tickets", [])
    if not isinstance(issued, list):
        raise HTTPException(400, "Поле tickets должно быть списком")
    if len(issued) > config.MAX_BATTLES_PER_SAVE:
        raise HTTPException(413, "Слишком много боёв в одном запросе")

    # Every ticket is checked before anything is written: a request saves all or nothing.
    battles = []
    for ticket in issued:
        raw = tickets.open_ticket(ticket)
        if raw is None:
            raise HTTPException(400, "Бой не прошёл проверку или устарел — загрузи реплей заново")
        battles.append(_clean_battle(raw))

    counts = {"created": 0, "linked": 0, "already": 0}
    for battle in battles:
        counts[save_battle(battle, user_id=user["id"])] += 1
    return {
        "saved": counts["created"] + counts["linked"],
        "created": counts["created"],
        "linked": counts["linked"],
        "skipped": counts["already"],
    }


# --- Battle management ---

_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_scope(user: dict, scope: str | None, mine: bool = False, mode: str | None = None,
                  date_from: str | None = None, date_to: str | None = None) -> Scope:
    """What the viewer asked to see, checked against what they may see.

    scope: "mine" | "visible" (default: mine plus my teams) | "team:<id>" | "all" (admins only).
    `mine=true` is the older spelling of scope=mine. date_from / date_to: inclusive YYYY-MM-DD.
    """
    if mode is not None and mode not in MODES:
        raise HTTPException(400, f"Режим — один из: {', '.join(MODES)}")
    for value in (date_from, date_to):
        if value is not None and not _DAY.match(value):
            raise HTTPException(400, "Дата — в формате ГГГГ-ММ-ДД")
    period = {"mode": mode, "date_from": date_from, "date_to": date_to}
    scope = "mine" if mine else (scope or "visible")
    if scope == "mine":
        return Scope("mine", user_id=user["id"], **period)
    if scope == "visible":
        return Scope("visible", user_id=user["id"], **period)
    if scope == "all":
        if not user.get("is_admin"):
            raise HTTPException(403, "Вся база доступна только администратору")
        return Scope("all", **period)
    if scope.startswith("team:"):
        try:
            team_id = int(scope[5:])
        except ValueError:
            raise HTTPException(400, "Команда указывается как team:<id>")
        if not user.get("is_admin") and not get_member_role(team_id, user["id"]):
            raise HTTPException(404, "Команда не найдена")
        return Scope("team", team_id=team_id, **period)
    raise HTTPException(400, "Охват — mine, visible, team:<id> или all")


@app.get("/api/battles")
def list_battles(scope: str | None = None, mine: bool = False, mode: str | None = None,
                 date_from: str | None = None, date_to: str | None = None,
                 user: dict = Depends(require_user)):
    require_db()
    return {"battles": get_battles_list(resolve_scope(user, scope, mine, mode, date_from, date_to),
                                        viewer_id=user["id"])}


@app.delete("/api/battles/{battle_id}")
def remove_battle(battle_id: int, user: dict = Depends(require_user)):
    require_db()
    outcome = delete_battle(battle_id, user["id"])
    if outcome == "not_found":
        raise HTTPException(404, "Бой не найден среди твоих загрузок")
    return {"battle_id": battle_id, "outcome": outcome}


# --- Hidden players ---

@app.get("/api/players/hidden")
def list_hidden_players(user: dict = Depends(require_user)):
    require_db()
    return {"hidden": get_hidden_players(user["id"])}


@app.post("/api/players/hidden")
def add_hidden_player(body: dict, user: dict = Depends(require_user)):
    require_db()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Не указано имя игрока")
    hide_player(user["id"], name)
    return {"hidden": name}


@app.delete("/api/players/hidden/{name}")
def remove_hidden_player(name: str, user: dict = Depends(require_user)):
    require_db()
    if not unhide_player(user["id"], name):
        raise HTTPException(404, "Этот игрок не скрыт")
    return {"restored": name}


# --- Analytics ---

@app.get("/api/analytics/totals")
def analytics_totals(scope: str | None = None, mine: bool = False, mode: str | None = None,
                     date_from: str | None = None, date_to: str | None = None,
                     user: dict = Depends(require_user)):
    """Just the counters, for the header strip on the upload page."""
    require_db()
    return get_db_totals(resolve_scope(user, scope, mine, mode, date_from, date_to))


@app.get("/api/analytics/daily")
def analytics_daily(scope: str | None = None, mode: str | None = None,
                    date_from: str | None = None, date_to: str | None = None,
                    user: dict = Depends(require_user)):
    """Per-day series and period totals for the overview: results plus our players' averages."""
    require_db()
    return get_daily_summary(resolve_scope(user, scope, False, mode, date_from, date_to), viewer_id=user["id"])


@app.get("/api/analytics/summary")
def analytics_summary(
    scope: str | None = None,
    mine: bool = False,
    mode: str | None = None,
    min_battles: int = 1,
    date_from: str | None = None,
    date_to: str | None = None,
    user: dict = Depends(require_user),
):
    if not _DB_AVAILABLE:
        return JSONResponse({"error": "database_unavailable"}, status_code=503)
    viewer_id = user["id"]
    scope_id = resolve_scope(user, scope, mine, mode, date_from, date_to)
    min_battles = min(max(min_battles, 1), 100)
    try:
        players = get_player_summary(scope_id, viewer_id=viewer_id, min_battles=min_battles)
        return {
            "totals":        get_db_totals(scope_id),
            "players":       players,
            "players_total": count_players(scope_id, viewer_id=viewer_id),
            "min_battles":   min_battles,
            "maps":          get_map_summary(scope_id),
            # Same people as the table above, so the two never disagree.
            "vehicles":      get_vehicle_summary(scope_id, viewer_id=viewer_id,
                                                 names=[p["name"] for p in players]),
            "hidden":        get_hidden_players(viewer_id),
        }
    except Exception:
        # The details (SQL, table names) stay in the server log.
        traceback.print_exc()
        return JSONResponse({"error": "Внутренняя ошибка сервера"}, status_code=500)


# --- Teams ---
# A team is a set of users who see each other's uploads. The owner names it, hands out
# invite links and removes members; everyone else can only leave.

_TEAM_NAME_LIMITS = (2, 40)


def _team_name(body: dict) -> str:
    name = " ".join(str(body.get("name") or "").split())
    if not _TEAM_NAME_LIMITS[0] <= len(name) <= _TEAM_NAME_LIMITS[1]:
        raise HTTPException(400, "Название команды — от 2 до 40 символов")
    return name


def _team_role(team_id: int, user: dict, owner: bool = False) -> str:
    """The viewer's role in the team, or 404 as if it did not exist; 403 if owner is needed."""
    role = get_member_role(team_id, user["id"])
    if role is None and not user.get("is_admin"):
        raise HTTPException(404, "Команда не найдена")
    if owner and role != "owner":
        raise HTTPException(403, "Это может только владелец команды")
    return role or "admin"


@app.get("/api/teams")
def list_teams(user: dict = Depends(require_user)):
    require_db()
    return {"teams": get_user_teams(user["id"])}


@app.post("/api/teams")
def new_team(body: dict = Depends(json_body), user: dict = Depends(require_user)):
    require_db()
    name = _team_name(body)
    owned = sum(1 for t in get_user_teams(user["id"]) if t["role"] == "owner")
    if owned >= config.MAX_TEAMS_PER_USER:
        raise HTTPException(409, f"Можно владеть не больше чем {config.MAX_TEAMS_PER_USER} командами")
    return create_team(name, user["id"])


@app.get("/api/teams/{team_id}")
def team_details(team_id: int, user: dict = Depends(require_user)):
    require_db()
    role = _team_role(team_id, user)
    team = get_team(team_id)
    if not team:
        raise HTTPException(404, "Команда не найдена")
    return {
        "id": team["id"], "name": team["name"], "role": role,
        "members": get_team_members(team_id),
        # Invite codes are as good as membership, so only the owner sees them.
        "invites": list_invites(team_id) if role == "owner" else [],
    }


@app.patch("/api/teams/{team_id}")
def update_team(team_id: int, body: dict = Depends(json_body), user: dict = Depends(require_user)):
    require_db()
    _team_role(team_id, user, owner=True)
    rename_team(team_id, _team_name(body))
    return {"id": team_id, "name": _team_name(body)}


@app.delete("/api/teams/{team_id}")
def remove_team(team_id: int, user: dict = Depends(require_user)):
    require_db()
    _team_role(team_id, user, owner=True)
    delete_team(team_id)
    return {"deleted": team_id}


@app.delete("/api/teams/{team_id}/members/{member_id}")
def remove_team_member(team_id: int, member_id: int, user: dict = Depends(require_user)):
    """The owner removes someone, or a member leaves (member_id is their own id)."""
    require_db()
    role = _team_role(team_id, user)
    if member_id == user["id"]:
        if role == "owner":
            raise HTTPException(409, "Владелец не может выйти — передай или удали команду")
    elif role != "owner":
        raise HTTPException(403, "Это может только владелец команды")
    if not remove_member(team_id, member_id):
        raise HTTPException(404, "Такого участника нет")
    return {"removed": member_id}


@app.post("/api/teams/{team_id}/invites")
def new_invite(team_id: int, body: dict = Depends(json_body), user: dict = Depends(require_user)):
    require_db()
    _team_role(team_id, user, owner=True)
    try:
        days = int(body.get("days") or 7)
        max_uses = int(body["max_uses"]) if body.get("max_uses") else None
    except (TypeError, ValueError):
        raise HTTPException(400, "days и max_uses — целые числа")
    days = min(max(days, 1), config.INVITE_MAX_DAYS)
    if max_uses is not None:
        max_uses = min(max(max_uses, 1), config.MAX_TEAM_MEMBERS)
    # 16 random bytes: the link is the only thing standing between a stranger and the team.
    return create_invite(team_id, user["id"], secrets.token_urlsafe(16), days, max_uses)


@app.delete("/api/teams/{team_id}/invites/{code}")
def drop_invite(team_id: int, code: str, user: dict = Depends(require_user)):
    require_db()
    _team_role(team_id, user, owner=True)
    if not revoke_invite(team_id, code):
        raise HTTPException(404, "Приглашение не найдено")
    return {"revoked": code}


@app.get("/api/invites/{code}")
def invite_info(code: str, user: dict = Depends(require_user)):
    """What the join page shows before the user accepts."""
    require_db()
    invite = get_invite(code)
    if not invite:
        raise HTTPException(404, "Приглашение не найдено, истекло или уже использовано")
    return {"team_id": invite["team_id"], "team_name": invite["team_name"],
            "already_member": get_member_role(invite["team_id"], user["id"]) is not None}


@app.post("/api/invites/{code}/accept")
def join_team(code: str, user: dict = Depends(require_user)):
    require_db()
    invite = get_invite(code)
    if invite and len(get_team_members(invite["team_id"])) >= config.MAX_TEAM_MEMBERS:
        raise HTTPException(409, f"В команде уже {config.MAX_TEAM_MEMBERS} участников")
    outcome, team_id = accept_invite(code, user["id"])
    if outcome == "invalid":
        raise HTTPException(404, "Приглашение не найдено, истекло или уже использовано")
    return {"team_id": team_id, "outcome": outcome}


# --- Admin ---

def require_admin(user: dict = Depends(require_user)) -> dict:
    if not user.get("is_admin"):
        raise HTTPException(403, "Только для администратора")
    return user


@app.get("/api/admin/overview")
def admin_overview(user: dict = Depends(require_admin)):
    require_db()
    return get_admin_overview()


# --- The site itself ---
# Registered last, so every API route above wins. Any other GET is either a file of the
# build (hashed, cached for a year) or a client-side route that gets index.html (never
# cached, so a deploy reaches everyone on their next page load).

_IMMUTABLE = "public, max-age=31536000, immutable"


@app.get("/{path:path}", include_in_schema=False)
def serve_frontend(path: str):
    if path == "api" or path.startswith("api/"):
        raise HTTPException(404, "Нет такого метода API")
    if not (FRONTEND_DIR / "index.html").is_file():
        raise HTTPException(503, "Интерфейс не собран: npm run build в frontend/")
    root = FRONTEND_DIR.resolve()
    target = (FRONTEND_DIR / path).resolve()
    if path and target.is_file() and target.is_relative_to(root):
        cache = _IMMUTABLE if path.startswith("assets/") else "no-cache"
        return FileResponse(str(target), headers={"Cache-Control": cache})
    return FileResponse(str(FRONTEND_DIR / "index.html"), headers={"Cache-Control": "no-cache"})
