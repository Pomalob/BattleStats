import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import threading
import time
import traceback
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import config
import database
from auth import create_token, decode_token, hash_password, verify_password
from database import (
    create_user, delete_battle, get_battles_list, get_db_totals, get_hidden_players,
    get_map_summary, get_player_summary, get_user_by_username, get_vehicle_summary,
    hide_player, init_db, save_battle, unhide_player,
)
from replay_parser import parse_replay_bytes

STATIC_DIR = Path(__file__).parent.parent / "static"

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


app = FastAPI(title="StatsBattle", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- Auth helpers ---

def _current_user(authorization: str | None) -> dict | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    payload = decode_token(authorization.split(" ", 1)[1])
    if not payload:
        return None
    try:
        return {"id": int(payload["sub"]), "username": payload["username"]}
    except (KeyError, TypeError, ValueError):
        return None


def require_user(authorization: str | None = Header(default=None)) -> dict:
    """FastAPI dependency: the signed-in user, or 401."""
    user = _current_user(authorization)
    if not user:
        raise HTTPException(401, "Нужно войти в аккаунт")
    return user


def require_db() -> None:
    if not _DB_AVAILABLE:
        raise HTTPException(503, "База данных недоступна")


# --- Login throttling ---
# In-memory and therefore per-instance: enough to blunt online password guessing
# on a single-container deployment, not a substitute for a real rate limiter.

_login_failures: dict[tuple[str, str], list[float]] = {}
_login_lock = threading.Lock()


def _throttle_key(request: Request, username: str) -> tuple[str, str]:
    client = request.client.host if request.client else "unknown"
    return (client, username.lower())


def _check_login_allowed(key: tuple[str, str]) -> None:
    cutoff = time.monotonic() - config.LOGIN_LOCKOUT_SECONDS
    with _login_lock:
        recent = [t for t in _login_failures.get(key, []) if t > cutoff]
        if recent:
            _login_failures[key] = recent
        else:
            _login_failures.pop(key, None)
        if len(recent) >= config.LOGIN_MAX_ATTEMPTS:
            raise HTTPException(429, "Слишком много неудачных попыток, попробуй позже")


def _record_login_failure(key: tuple[str, str]) -> None:
    with _login_lock:
        _login_failures.setdefault(key, []).append(time.monotonic())


def _clear_login_failures(key: tuple[str, str]) -> None:
    with _login_lock:
        _login_failures.pop(key, None)


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
            }
            for p in b.players
        ],
    }


# --- Pages ---

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/analytics", include_in_schema=False)
def analytics():
    return FileResponse(str(STATIC_DIR / "analytics.html"))


@app.get("/login", include_in_schema=False)
def login_page():
    return FileResponse(str(STATIC_DIR / "login.html"))


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

@app.post("/api/auth/register")
def register(body: dict, request: Request):
    require_db()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not 3 <= len(username) <= 32:
        raise HTTPException(400, "Имя пользователя — от 3 до 32 символов")
    if len(password) < 6:
        raise HTTPException(400, "Пароль — минимум 6 символов")
    user = create_user(username, hash_password(password))
    if not user:
        raise HTTPException(409, "Такое имя уже занято")
    _clear_login_failures(_throttle_key(request, username))
    return {"token": create_token(user["id"], user["username"]), "username": user["username"]}


@app.post("/api/auth/login")
def login(body: dict, request: Request):
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
    return {"token": create_token(user["id"], user["username"]), "username": user["username"]}


@app.get("/api/auth/me")
def me(user: dict = Depends(require_user)):
    return user


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
            results.append(battle_to_dict(parse_replay_bytes(data, Path(name).name)))
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

    battle["players"] = [
        {k: _coerce(p.get(k), kind) for k, kind in _PLAYER_FIELDS.items()}
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
    require_db()
    battles = body.get("battles", [])
    if not isinstance(battles, list):
        raise HTTPException(400, "Поле battles должно быть списком")
    if len(battles) > config.MAX_FILES_PER_UPLOAD:
        raise HTTPException(413, "Слишком много боёв в одном запросе")

    counts = {"created": 0, "linked": 0, "already": 0}
    for raw in battles:
        counts[save_battle(_clean_battle(raw), user_id=user["id"])] += 1
    return {
        "saved": counts["created"] + counts["linked"],
        "created": counts["created"],
        "linked": counts["linked"],
        "skipped": counts["already"],
    }


# --- Battle management ---

@app.get("/api/battles")
def list_battles(mine: bool = False, user: dict = Depends(require_user)):
    require_db()
    viewer_id = user["id"]
    return {"battles": get_battles_list(viewer_id if mine else None, viewer_id=viewer_id)}


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

@app.get("/api/analytics/summary")
def analytics_summary(mine: bool = False, user: dict = Depends(require_user)):
    if not _DB_AVAILABLE:
        return JSONResponse({"error": "database_unavailable"}, status_code=503)
    viewer_id = user["id"]
    scope_id = viewer_id if mine else None
    try:
        return {
            "totals":   get_db_totals(scope_id),
            "players":  get_player_summary(scope_id, viewer_id=viewer_id),
            "maps":     get_map_summary(scope_id),
            "vehicles": get_vehicle_summary(scope_id, viewer_id=viewer_id),
            "hidden":   get_hidden_players(viewer_id),
        }
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
