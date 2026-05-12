import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import tempfile
import traceback

from replay_parser import parse_replay, BattleResult
from database import init_db, save_battle, get_player_summary, get_map_summary, get_vehicle_summary, get_db_totals

app = FastAPI(title="StatsBattle")

STATIC_DIR = Path(__file__).parent.parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_DB_AVAILABLE = False


@app.on_event("startup")
def startup():
    global _DB_AVAILABLE
    try:
        init_db()
        _DB_AVAILABLE = True
    except Exception as e:
        print(f"[DB] init failed: {e}")
        _DB_AVAILABLE = False


def battle_to_dict(b: BattleResult) -> dict:
    return {
        "filename": b.filename,
        "map_name": b.map_name,
        "date_time": b.date_time,
        "result": b.result,
        "winner_team": b.winner_team,
        "player_team": b.player_team,
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


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/analytics")
def analytics():
    return FileResponse(str(STATIC_DIR / "analytics.html"))


@app.post("/api/upload")
async def upload_replays(files: list[UploadFile] = File(...)):
    results = []
    errors = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        for upload in files:
            if not upload.filename.endswith(".mtreplay"):
                errors.append({"file": upload.filename, "error": "Not a .mtreplay file"})
                continue

            dest = tmp_path / upload.filename
            content = await upload.read()
            dest.write_bytes(content)

            try:
                battle = parse_replay(dest)
                d = battle_to_dict(battle)
                results.append(d)
                if _DB_AVAILABLE:
                    save_battle(d)
            except Exception as e:
                errors.append({"file": upload.filename, "error": str(e)})

    results.sort(key=lambda r: r["date_time"], reverse=True)
    return {"battles": results, "errors": errors}


@app.get("/api/analytics/summary")
def analytics_summary():
    if not _DB_AVAILABLE:
        return JSONResponse({"error": "database_unavailable"}, status_code=503)
    try:
        return {
            "totals":   get_db_totals(),
            "players":  get_player_summary(),
            "maps":     get_map_summary(),
            "vehicles": get_vehicle_summary(),
        }
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
