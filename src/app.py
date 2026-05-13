import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import traceback
from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import tempfile

from replay_parser import parse_replay
from database import (
    init_db, save_battle,
    get_player_summary, get_map_summary, get_vehicle_summary, get_db_totals,
    create_user, get_user_by_username, delete_player,
)
from auth import hash_password, verify_password, create_token, decode_token

app = FastAPI(title="StatsBattle")

STATIC_DIR = Path(__file__).parent.parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_DB_AVAILABLE = False


@app.on_event("startup")
def startup():
    global _DB_AVAILABLE
    import os
    url = os.environ.get("DATABASE_URL", "")
    print(f"[DB] DATABASE_URL set: {bool(url)}, prefix: {url[:20] if url else 'none'}")
    try:
        init_db()
        _DB_AVAILABLE = True
        print("[DB] init OK")
    except Exception as e:
        print(f"[DB] init failed: {e}")
        traceback.print_exc()


def _get_current_user(authorization: str | None) -> dict | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    payload = decode_token(authorization.split(" ", 1)[1])
    if not payload:
        return None
    return {"id": int(payload["sub"]), "username": payload["username"]}


def require_auth(authorization: str | None = Header(default=None)) -> dict:
    user = _get_current_user(authorization)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


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
            }
            for p in b.players
        ],
    }


# --- Pages ---

@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/analytics")
def analytics():
    return FileResponse(str(STATIC_DIR / "analytics.html"))

@app.get("/login")
def login_page():
    return FileResponse(str(STATIC_DIR / "login.html"))


# --- Auth endpoints ---

@app.post("/api/auth/register")
def register(body: dict):
    if not _DB_AVAILABLE:
        raise HTTPException(503, "Database unavailable")
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if len(username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if len(password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    user = create_user(username, hash_password(password))
    if not user:
        raise HTTPException(409, "Username already taken")
    return {"token": create_token(user["id"], user["username"]), "username": user["username"]}


@app.post("/api/auth/login")
def login(body: dict):
    if not _DB_AVAILABLE:
        raise HTTPException(503, "Database unavailable")
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    user = get_user_by_username(username)
    if not user or not verify_password(password, user["password_hash"]):
        raise HTTPException(401, "Invalid username or password")
    return {"token": create_token(user["id"], user["username"]), "username": user["username"]}


@app.get("/api/auth/me")
def me(authorization: str | None = Header(default=None)):
    user = _get_current_user(authorization)
    if not user:
        raise HTTPException(401, "Not authenticated")
    return user


# --- Upload ---

@app.post("/api/upload")
async def upload_replays(
    files: list[UploadFile] = File(...),
    authorization: str | None = Header(default=None),
):
    results, errors = [], []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        for upload in files:
            if not upload.filename.endswith(".mtreplay"):
                errors.append({"file": upload.filename, "error": "Not a .mtreplay file"})
                continue
            dest = tmp_path / upload.filename
            dest.write_bytes(await upload.read())
            try:
                battle = parse_replay(dest)
                results.append(battle_to_dict(battle))
            except Exception as e:
                errors.append({"file": upload.filename, "error": str(e)})

    results.sort(key=lambda r: r["date_time"], reverse=True)
    return {"battles": results, "errors": errors}


@app.post("/api/battles/save")
def save_battles(
    body: dict,
    authorization: str | None = Header(default=None),
):
    if not _DB_AVAILABLE:
        raise HTTPException(503, "Database unavailable")
    user = _get_current_user(authorization)
    battles = body.get("battles", [])
    if not isinstance(battles, list):
        raise HTTPException(400, "battles must be a list")
    saved, skipped = 0, 0
    for d in battles:
        is_new = save_battle(d, user_id=user["id"] if user else None)
        if is_new:
            saved += 1
        else:
            skipped += 1
    return {"saved": saved, "skipped": skipped}


# --- Player management ---

@app.delete("/api/players/{name}")
def remove_player(name: str, authorization: str | None = Header(default=None)):
    if not _DB_AVAILABLE:
        raise HTTPException(503, "Database unavailable")
    require_auth(authorization)
    count = delete_player(name)
    if count == 0:
        raise HTTPException(404, "Player not found")
    return {"deleted": count}


# --- Analytics ---


@app.get("/api/analytics/summary")
def analytics_summary(
    mine: bool = False,
    authorization: str | None = Header(default=None),
):
    if not _DB_AVAILABLE:
        return JSONResponse({"error": "database_unavailable"}, status_code=503)
    user = _get_current_user(authorization)
    user_id = user["id"] if (mine and user) else None
    try:
        return {
            "totals":   get_db_totals(user_id),
            "players":  get_player_summary(user_id),
            "maps":     get_map_summary(user_id),
            "vehicles": get_vehicle_summary(user_id),
        }
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
