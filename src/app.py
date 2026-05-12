import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import tempfile
import asyncio

from replay_parser import parse_replay, BattleResult

app = FastAPI(title="StatsBattle")

STATIC_DIR = Path(__file__).parent.parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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
                results.append(battle_to_dict(battle))
            except Exception as e:
                errors.append({"file": upload.filename, "error": str(e)})

    results.sort(key=lambda r: r["date_time"], reverse=True)

    return {"battles": results, "errors": errors}
