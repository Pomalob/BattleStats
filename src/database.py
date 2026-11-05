import ipaddress
import threading
from contextlib import contextmanager
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from config import DATABASE_URL, DB_POOL_MAX, DB_POOL_MIN

_DDL = """
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS battles (
    id          SERIAL PRIMARY KEY,
    filename    TEXT NOT NULL,
    map_name    TEXT NOT NULL,
    date_time   TEXT NOT NULL,
    battle_hash TEXT UNIQUE,
    result      TEXT NOT NULL,
    player_team INTEGER NOT NULL,
    winner_team INTEGER NOT NULL,
    details     JSONB,
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
    survived        BOOLEAN NOT NULL DEFAULT FALSE,
    shots           INTEGER NOT NULL DEFAULT 0,
    hits            INTEGER NOT NULL DEFAULT 0,
    piercings       INTEGER NOT NULL DEFAULT 0,
    details         JSONB
);

CREATE TABLE IF NOT EXISTS user_battles (
    user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    battle_id INTEGER NOT NULL REFERENCES battles(id) ON DELETE CASCADE,
    recorder  JSONB,
    PRIMARY KEY (user_id, battle_id)
);

CREATE TABLE IF NOT EXISTS hidden_players (
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_player_stats_battle ON player_stats(battle_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_name   ON player_stats(name);
CREATE INDEX IF NOT EXISTS idx_battles_date        ON battles(date_time);
CREATE INDEX IF NOT EXISTS idx_user_battles_user   ON user_battles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_battles_battle ON user_battles(battle_id);
"""

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "db", "postgres"}

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _is_internal_host(host: str) -> bool:
    """Same machine or a private network: a container name without a dot, or a private IP.

    Hosting platforms reach their own Postgres over an internal network that usually
    has no certificate, so requiring TLS there only breaks the connection.
    """
    if host in _LOCAL_HOSTS or "." not in host and ":" not in host:
        return True
    try:
        return ipaddress.ip_address(host).is_private
    except ValueError:
        return False


# TLS options a connection string may carry. `ssl` is not a libpq option at all, but
# platform dashboards paste it in, and libpq refuses to connect over an unknown one.
_TLS_PARAMS = {"sslmode", "ssl", "channel_binding"}


def build_dsn(url: str) -> str:
    """Normalise a DATABASE_URL: postgres:// -> postgresql://, TLS for remote hosts only."""
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if _is_internal_host(host):
        # A Postgres reachable only from the platform's private network serves no TLS,
        # and asking for it fails with "server does not support SSL connections".
        query = [(k, v) for k, v in parse_qsl(parts.query) if k.lower() not in _TLS_PARAMS]
        query.append(("sslmode", "disable"))
        return urlunsplit(parts._replace(query=urlencode(query)))
    # Managed Postgres (Neon, Render, Railway) requires TLS.
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(
                    DB_POOL_MIN, DB_POOL_MAX, build_dsn(DATABASE_URL),
                    cursor_factory=RealDictCursor,
                )
    return _pool


def close_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.closeall()
            _pool = None


@contextmanager
def get_conn():
    """Borrow a pooled connection; commit on success, roll back and hand it back on error."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(conn, close=bool(conn.closed))
        except Exception:
            pass


def ping() -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            return cur.fetchone()["ok"] == 1


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        # Migrations run one statement per transaction so a failure cannot block startup
        _run_migration(conn, "ALTER TABLE battles DROP CONSTRAINT IF EXISTS battles_filename_key")
        _run_migration(conn, "ALTER TABLE battles DROP CONSTRAINT IF EXISTS battles_datetime_map_key")
        _run_migration(conn, "ALTER TABLE battles ADD COLUMN IF NOT EXISTS battle_hash TEXT")
        _run_migration(conn, "ALTER TABLE battles ADD CONSTRAINT battles_hash_key UNIQUE (battle_hash)")
        _run_migration(conn, "ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS shots INTEGER NOT NULL DEFAULT 0")
        _run_migration(conn, "ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS hits INTEGER NOT NULL DEFAULT 0")
        _run_migration(conn, "ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS piercings INTEGER NOT NULL DEFAULT 0")
        _run_migration(conn, "ALTER TABLE battles ADD COLUMN IF NOT EXISTS details JSONB")
        _run_migration(conn, "ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS details JSONB")
        _run_migration(conn, "ALTER TABLE user_battles ADD COLUMN IF NOT EXISTS recorder JSONB")
        # Fails if the table already holds names that differ only by case; that is a
        # data problem to resolve by hand, not a reason to block startup.
        _run_migration(conn, "CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_key ON users (lower(username))")
        # Old rows stored the replay's own "DD.MM.YYYY HH:MM:SS", which sorts wrong
        # as text; rewrite them to the same ISO order the parser produces now.
        _run_migration(conn, r"""
            UPDATE battles
            SET date_time = substring(date_time from 7 for 4) || '-'
                         || substring(date_time from 4 for 2) || '-'
                         || substring(date_time from 1 for 2)
                         || substring(date_time from 11)
            WHERE date_time ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}'
        """)
        _migrate_map_names(conn)


def _run_migration(conn, sql: str):
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()


def _migrate_map_names(conn):
    """Rewrite map names stored by older versions: internal IDs and misspelled display names."""
    from replay_parser import MAP_NAME_FIXES, MAP_NAMES
    renames = {**MAP_NAMES, **MAP_NAME_FIXES}
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT map_name FROM battles")
        stored = {r["map_name"] for r in cur.fetchall()}
        for old, new in renames.items():
            if old in stored:
                cur.execute("UPDATE battles SET map_name = %s WHERE map_name = %s", (new, old))
    conn.commit()


# --- Auth ---

def create_user(username: str, password_hash: str) -> dict | None:
    """Create a user, or return None when the name is taken (case-insensitively)."""
    sql = """
        INSERT INTO users (username, password_hash)
        VALUES (%s, %s)
        RETURNING id, username
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username, password_hash))
                row = cur.fetchone()
    except psycopg2.errors.UniqueViolation:
        return None
    return dict(row) if row else None


def get_user_by_username(username: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash FROM users WHERE lower(username) = lower(%s)",
                (username,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


# --- Battles ---

def save_battle(battle_dict: dict, user_id: int) -> str:
    """Insert battle + stats, deduplicated by battle_hash.

    Returns "created" (new battle), "linked" (someone had already uploaded this battle
    and it is now in this user's history too) or "already" (nothing changed).

    A battle stored before extended details were parsed gets them from the next
    upload of the same replay, so old history can be filled in by uploading it again.
    """
    sql_battle = """
        INSERT INTO battles (filename, map_name, date_time, battle_hash, result, player_team, winner_team, details)
        VALUES (%(filename)s, %(map_name)s, %(date_time)s, %(battle_hash)s, %(result)s, %(player_team)s, %(winner_team)s, %(details)s)
        ON CONFLICT (battle_hash) DO NOTHING
        RETURNING id
    """
    sql_player = """
        INSERT INTO player_stats
            (battle_id, name, vehicle, team, damage_dealt, damage_received,
             blocked, assists, frags, xp, survived, shots, hits, piercings, details)
        VALUES
            (%(battle_id)s, %(name)s, %(vehicle)s, %(team)s, %(damage_dealt)s,
             %(damage_received)s, %(blocked)s, %(assists)s, %(frags)s, %(xp)s, %(survived)s,
             %(shots)s, %(hits)s, %(piercings)s, %(details)s)
    """
    details = _json_or_null(battle_dict.get("details"))
    recorder = _json_or_null(battle_dict.get("recorder"))
    players = [{**p, "details": _json_or_null(p.get("details"))} for p in battle_dict["players"]]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_battle, {**battle_dict, "details": details})
            row = cur.fetchone()

            if row:
                battle_id = row["id"]
                for p in players:
                    cur.execute(sql_player, {**p, "battle_id": battle_id})
                outcome = "created"
            else:
                cur.execute(
                    "SELECT id FROM battles WHERE battle_hash = %s",
                    (battle_dict["battle_hash"],),
                )
                existing = cur.fetchone()
                battle_id = existing["id"] if existing else None
                outcome = "already"
                if battle_id and details is not None:
                    _backfill_details(cur, battle_id, details, players)

            if battle_id:
                cur.execute(
                    "INSERT INTO user_battles (user_id, battle_id, recorder) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (user_id, battle_id, recorder),
                )
                # A battle someone else uploaded is not a duplicate for this user: the
                # row already existed, but it has only now joined their history.
                if outcome == "already" and cur.rowcount:
                    outcome = "linked"
                elif recorder is not None:
                    cur.execute(
                        "UPDATE user_battles SET recorder = %s"
                        " WHERE user_id = %s AND battle_id = %s AND recorder IS NULL",
                        (recorder, user_id, battle_id),
                    )
    return outcome


def _json_or_null(value):
    return Json(value) if value else None


def _backfill_details(cur, battle_id: int, details, players: list[dict]) -> None:
    """Fill in extended details on a battle that was stored without them."""
    cur.execute(
        "UPDATE battles SET details = %s WHERE id = %s AND details IS NULL",
        (details, battle_id),
    )
    if not cur.rowcount:
        return
    for p in players:
        if p["details"] is not None:
            cur.execute(
                "UPDATE player_stats SET details = %s"
                " WHERE battle_id = %s AND name = %s AND details IS NULL",
                (p["details"], battle_id, p["name"]),
            )


def get_battles_list(user_id: int | None = None, viewer_id: int | None = None) -> list[dict]:
    join, params = _scope_join(user_id)
    sql = f"""
        SELECT b.id, b.date_time, b.map_name, b.result, b.filename,
               COUNT(DISTINCT ps.id) AS player_count,
               BOOL_OR(own.user_id IS NOT NULL) AS mine
        FROM battles b
        {join}
        LEFT JOIN player_stats ps ON ps.battle_id = b.id
        LEFT JOIN user_battles own ON own.battle_id = b.id AND own.user_id = %s
        GROUP BY b.id, b.date_time, b.map_name, b.result, b.filename
        ORDER BY b.date_time DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, viewer_id])
            return [dict(r) for r in cur.fetchall()]


def get_battles_export(
    user_id: int | None = None,
    viewer_id: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Every battle in scope with all its players, oldest first.

    date_from / date_to are inclusive YYYY-MM-DD days, compared against the date part
    of date_time. The recorder block is the viewer's own upload of the battle, if any.
    """
    join, params = _scope_join(user_id)
    where, where_params = [], []
    if date_from:
        where.append("left(b.date_time, 10) >= %s")
        where_params.append(date_from)
    if date_to:
        where.append("left(b.date_time, 10) <= %s")
        where_params.append(date_to)
    sql_battles = f"""
        SELECT b.id, b.date_time, b.map_name, b.result, b.winner_team, b.player_team,
               b.battle_hash, b.filename, b.details,
               own.recorder, (own.user_id IS NOT NULL) AS mine
        FROM battles b
        {join}
        LEFT JOIN user_battles own ON own.battle_id = b.id AND own.user_id = %s
        {"WHERE " + " AND ".join(where) if where else ""}
        ORDER BY b.date_time, b.id
    """
    sql_players = """
        SELECT battle_id, name, vehicle, team, damage_dealt, damage_received, blocked,
               assists, frags, xp, survived, shots, hits, piercings, details
        FROM player_stats
        WHERE battle_id = ANY(%s)
        ORDER BY battle_id, team, damage_dealt DESC, id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_battles, [*params, viewer_id, *where_params])
            battles = [dict(r) for r in cur.fetchall()]
            if not battles:
                return []
            by_id = {b["id"]: {**b, "players": []} for b in battles}
            cur.execute(sql_players, ([b["id"] for b in battles],))
            for r in cur.fetchall():
                player = dict(r)
                by_id[player.pop("battle_id")]["players"].append(player)
    return list(by_id.values())


def delete_battle(battle_id: int, user_id: int) -> str:
    """Unlink a battle from this user, and drop it entirely once nobody owns it.

    Returns "deleted" (row gone), "unlinked" (someone else still owns it) or
    "not_found" (no such battle, or this user does not own it).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_battles WHERE battle_id = %s AND user_id = %s",
                (battle_id, user_id),
            )
            if cur.rowcount == 0:
                return "not_found"
            cur.execute("SELECT 1 FROM user_battles WHERE battle_id = %s LIMIT 1", (battle_id,))
            if cur.fetchone():
                return "unlinked"
            cur.execute("DELETE FROM battles WHERE id = %s", (battle_id,))
            return "deleted"


# --- Hidden players ---

def get_hidden_players(user_id: int) -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM hidden_players WHERE user_id = %s ORDER BY name",
                (user_id,),
            )
            return [r["name"] for r in cur.fetchall()]


def hide_player(user_id: int, name: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO hidden_players (user_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, name),
            )


def unhide_player(user_id: int, name: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM hidden_players WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            return cur.rowcount > 0


# --- Analytics ---

def _scope_join(user_id: int | None) -> tuple[str, list]:
    """JOIN that narrows `battles b` down to the ones this user uploaded."""
    if user_id:
        return "JOIN user_battles ub ON ub.battle_id = b.id AND ub.user_id = %s", [user_id]
    return "", []


def _hidden_filter(viewer_id: int | None) -> tuple[str, list]:
    """WHERE fragment that drops the players this viewer has hidden."""
    if viewer_id:
        return (
            "AND ps.name NOT IN (SELECT name FROM hidden_players WHERE user_id = %s)",
            [viewer_id],
        )
    return "", []


def get_db_totals(user_id: int | None = None) -> dict:
    join, params = _scope_join(user_id)
    sql = f"""
        SELECT
            COUNT(*)                                           AS total_battles,
            SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END)   AS wins,
            SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END)   AS losses,
            SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)   AS draws,
            MIN(date_time)                                     AS first_battle,
            MAX(date_time)                                     AS last_battle
        FROM battles b {join}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return dict(cur.fetchone())


def count_players(user_id: int | None = None, viewer_id: int | None = None) -> int:
    """How many distinct team-mates appear at all, before any min_battles filter."""
    join, params = _scope_join(user_id)
    hidden, hidden_params = _hidden_filter(viewer_id)
    sql = f"""
        SELECT COUNT(DISTINCT ps.name) AS n
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {hidden}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params])
            return int(cur.fetchone()["n"])


def get_player_summary(
    user_id: int | None = None,
    viewer_id: int | None = None,
    min_battles: int = 1,
) -> list[dict]:
    """Team-mate averages. min_battles drops the one-off randoms that otherwise
    bury the people you actually play with — with a few hundred battles uploaded,
    most names in the table appear exactly once."""
    join, params = _scope_join(user_id)
    hidden, hidden_params = _hidden_filter(viewer_id)
    sql = f"""
        SELECT
            ps.name,
            COUNT(DISTINCT ps.battle_id)                                     AS battles,
            ROUND(AVG(ps.damage_dealt))                                      AS avg_damage,
            ROUND(AVG(ps.assists))                                           AS avg_assists,
            ROUND(AVG(ps.blocked))                                           AS avg_blocked,
            ROUND(AVG(ps.damage_received))                                   AS avg_received,
            ROUND(AVG(ps.frags)::numeric, 1)                                 AS avg_frags,
            SUM(CASE WHEN b.result = 'win'  THEN 1 ELSE 0 END)               AS wins,
            SUM(CASE WHEN b.result = 'loss' THEN 1 ELSE 0 END)               AS losses,
            SUM(CASE WHEN b.result = 'draw' THEN 1 ELSE 0 END)               AS draws,
            ROUND(AVG(CASE WHEN b.result = 'win'  THEN ps.damage_dealt END)) AS avg_dmg_win,
            ROUND(AVG(CASE WHEN b.result = 'loss' THEN ps.damage_dealt END)) AS avg_dmg_loss,
            ROUND(AVG(ps.shots))                                             AS avg_shots,
            ROUND(AVG(ps.hits))                                              AS avg_hits,
            ROUND(AVG(ps.piercings))                                         AS avg_piercings
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {hidden}
        GROUP BY ps.name
        HAVING COUNT(DISTINCT ps.battle_id) >= %s
        ORDER BY avg_damage DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params, max(1, min_battles)])
            return [dict(r) for r in cur.fetchall()]


def get_map_summary(user_id: int | None = None) -> list[dict]:
    join, params = _scope_join(user_id)
    sql = f"""
        SELECT
            map_name,
            COUNT(*)                                                     AS battles,
            SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END)             AS wins,
            SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END)             AS losses,
            SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)             AS draws,
            ROUND(AVG(CASE WHEN result = 'win' THEN 1 ELSE 0 END) * 100) AS winrate
        FROM battles b {join}
        GROUP BY map_name
        ORDER BY battles DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def get_vehicle_summary(
    user_id: int | None = None,
    viewer_id: int | None = None,
    names: list[str] | None = None,
) -> list[dict]:
    """Per-tank averages, restricted to `names` so this table shows the same
    players as the summary above."""
    join, params = _scope_join(user_id)
    hidden, hidden_params = _hidden_filter(viewer_id)
    name_filter, name_params = ("AND ps.name = ANY(%s)", [list(names)]) if names is not None else ("", [])
    sql = f"""
        SELECT
            ps.name,
            ps.vehicle,
            COUNT(*)                                            AS battles,
            ROUND(AVG(ps.damage_dealt))                         AS avg_damage,
            ROUND(AVG(ps.assists))                              AS avg_assists,
            ROUND(AVG(ps.frags)::numeric, 1)                    AS avg_frags,
            SUM(CASE WHEN b.result = 'win'  THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN b.result = 'loss' THEN 1 ELSE 0 END)  AS losses
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {hidden}
        {name_filter}
        GROUP BY ps.name, ps.vehicle
        ORDER BY ps.name, battles DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params, *name_params])
            return [dict(r) for r in cur.fetchall()]
