import os
import psycopg2
from psycopg2.extras import RealDictCursor

_DDL = """
CREATE TABLE IF NOT EXISTS battles (
    id          SERIAL PRIMARY KEY,
    filename    TEXT NOT NULL UNIQUE,
    map_name    TEXT NOT NULL,
    date_time   TEXT NOT NULL,
    result      TEXT NOT NULL,
    player_team INTEGER NOT NULL,
    winner_team INTEGER NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS player_stats (
    id              SERIAL PRIMARY KEY,
    battle_id       INTEGER NOT NULL REFERENCES battles(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    vehicle         TEXT NOT NULL,
    team            INTEGER NOT NULL,
    damage_dealt    INTEGER NOT NULL DEFAULT 0,
    damage_received INTEGER NOT NULL DEFAULT 0,
    blocked         INTEGER NOT NULL DEFAULT 0,
    assists         INTEGER NOT NULL DEFAULT 0,
    frags           INTEGER NOT NULL DEFAULT 0,
    xp              INTEGER NOT NULL DEFAULT 0,
    survived        BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_player_stats_battle ON player_stats(battle_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_name   ON player_stats(name);
CREATE INDEX IF NOT EXISTS idx_battles_date        ON battles(date_time);
"""


def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    # Railway requires SSL; append if not already specified
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()


def save_battle(battle_dict: dict) -> int | None:
    """Insert battle + player_stats. Returns battle id, or None if duplicate."""
    sql_battle = """
        INSERT INTO battles (filename, map_name, date_time, result, player_team, winner_team)
        VALUES (%(filename)s, %(map_name)s, %(date_time)s, %(result)s, %(player_team)s, %(winner_team)s)
        ON CONFLICT (filename) DO NOTHING
        RETURNING id
    """
    sql_player = """
        INSERT INTO player_stats
            (battle_id, name, vehicle, team, damage_dealt, damage_received,
             blocked, assists, frags, xp, survived)
        VALUES
            (%(battle_id)s, %(name)s, %(vehicle)s, %(team)s, %(damage_dealt)s,
             %(damage_received)s, %(blocked)s, %(assists)s, %(frags)s, %(xp)s, %(survived)s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_battle, battle_dict)
            row = cur.fetchone()
            if not row:
                return None
            battle_id = row["id"]
            for p in battle_dict["players"]:
                cur.execute(sql_player, {**p, "battle_id": battle_id})
        conn.commit()
    return battle_id


def get_player_summary() -> list[dict]:
    sql = """
        SELECT
            ps.name,
            COUNT(DISTINCT ps.battle_id)                                  AS battles,
            ROUND(AVG(ps.damage_dealt))                                   AS avg_damage,
            ROUND(AVG(ps.assists))                                        AS avg_assists,
            ROUND(AVG(ps.blocked))                                        AS avg_blocked,
            ROUND(AVG(ps.damage_received))                                AS avg_received,
            ROUND(AVG(ps.frags)::numeric, 1)                              AS avg_frags,
            SUM(CASE WHEN b.result = 'win'  THEN 1 ELSE 0 END)           AS wins,
            SUM(CASE WHEN b.result = 'loss' THEN 1 ELSE 0 END)           AS losses,
            SUM(CASE WHEN b.result = 'draw' THEN 1 ELSE 0 END)           AS draws,
            ROUND(AVG(CASE WHEN b.result = 'win'  THEN ps.damage_dealt END)) AS avg_dmg_win,
            ROUND(AVG(CASE WHEN b.result = 'loss' THEN ps.damage_dealt END)) AS avg_dmg_loss
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        WHERE ps.team = b.player_team
        GROUP BY ps.name
        ORDER BY avg_damage DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]


def get_map_summary() -> list[dict]:
    sql = """
        SELECT
            map_name,
            COUNT(*)                                            AS battles,
            SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END)  AS losses,
            SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)  AS draws,
            ROUND(AVG(CASE WHEN result = 'win'  THEN 1 ELSE 0 END) * 100) AS winrate
        FROM battles
        GROUP BY map_name
        ORDER BY battles DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]


def get_vehicle_summary() -> list[dict]:
    sql = """
        SELECT
            ps.name,
            ps.vehicle,
            COUNT(*)                                                      AS battles,
            ROUND(AVG(ps.damage_dealt))                                   AS avg_damage,
            ROUND(AVG(ps.assists))                                        AS avg_assists,
            ROUND(AVG(ps.frags)::numeric, 1)                              AS avg_frags,
            SUM(CASE WHEN b.result = 'win'  THEN 1 ELSE 0 END)           AS wins,
            SUM(CASE WHEN b.result = 'loss' THEN 1 ELSE 0 END)           AS losses
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        WHERE ps.team = b.player_team
        GROUP BY ps.name, ps.vehicle
        ORDER BY ps.name, battles DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]


def get_db_totals() -> dict:
    sql = """
        SELECT
            COUNT(*)                                           AS total_battles,
            SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS draws,
            MIN(date_time)                                     AS first_battle,
            MAX(date_time)                                     AS last_battle
        FROM battles
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return dict(cur.fetchone())
