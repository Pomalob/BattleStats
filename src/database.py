import ipaddress
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from config import ADMIN_USER_IDS, DATABASE_URL, DB_POOL_MAX, DB_POOL_MIN
from replay_parser import SQUAD_MODES, battle_mode
from wn8 import Tally

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

CREATE TABLE IF NOT EXISTS teams (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- role: owner (manages the team and its invites) or member
CREATE TABLE IF NOT EXISTS team_members (
    team_id   INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role      TEXT NOT NULL DEFAULT 'member',
    joined_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, user_id)
);

CREATE TABLE IF NOT EXISTS team_invites (
    code       TEXT PRIMARY KEY,
    team_id    INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    max_uses   INTEGER,
    uses       INTEGER NOT NULL DEFAULT 0
);

-- One-off data migrations that must not run again, whatever the tables hold later.
CREATE TABLE IF NOT EXISTS schema_flags (
    name    TEXT PRIMARY KEY,
    done_at TIMESTAMPTZ DEFAULT NOW()
);

-- AI analyses. cache_key hashes the facts, the prompt version and the model, so the same
-- period asked twice is answered from here; failed attempts are kept for the daily limit.
CREATE TABLE IF NOT EXISTS explanations (
    id         SERIAL PRIMARY KEY,
    cache_key  TEXT NOT NULL,
    user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
    mode       TEXT,
    status     TEXT NOT NULL,
    answer     JSONB,
    problems   JSONB,
    model      TEXT,
    usage      JSONB,
    error_kind TEXT,
    error      JSONB,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_explanations_key  ON explanations(cache_key) WHERE status = 'ok';
CREATE INDEX IF NOT EXISTS idx_explanations_user ON explanations(user_id, created_at);

CREATE INDEX IF NOT EXISTS idx_team_members_user   ON team_members(user_id);
CREATE INDEX IF NOT EXISTS idx_team_invites_team   ON team_invites(team_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_battle ON player_stats(battle_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_name   ON player_stats(name);
-- The squad rule (_SQUAD_FOCUS) looks up the uploader by name inside one battle.
CREATE INDEX IF NOT EXISTS idx_player_stats_battle_name ON player_stats(battle_id, name);
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
        _run_migration(conn, "ALTER TABLE battles ADD COLUMN IF NOT EXISTS mode TEXT")
        _run_migration(conn, "CREATE INDEX IF NOT EXISTS idx_battles_mode ON battles(mode)")
        _migrate_battle_modes(conn)
        _migrate_initial_team(conn)
        _run_migration(conn, "ALTER TABLE users ADD COLUMN IF NOT EXISTS token_version INTEGER NOT NULL DEFAULT 0")
        _run_migration(conn, "ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_version TEXT")
        _run_migration(conn, "ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_at TIMESTAMPTZ")
        _run_migration(conn, "ALTER TABLE explanations ADD COLUMN IF NOT EXISTS error_kind TEXT")
        _run_migration(conn, "ALTER TABLE explanations ADD COLUMN IF NOT EXISTS error JSONB")
        _run_migration(conn, "ALTER TABLE explanations ADD COLUMN IF NOT EXISTS duration_ms INTEGER")


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


def _migrate_battle_modes(conn):
    """Fill battles.mode for rows stored before the column existed, from their details.

    A failure is logged, not raised: startup must not fail over it, and the next start retries.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, details FROM battles WHERE mode IS NULL")
            for row in cur.fetchall():
                cur.execute("UPDATE battles SET mode = %s WHERE id = %s", (battle_mode(row["details"]), row["id"]))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB] battle mode backfill failed, will retry on next start: {e}")


INITIAL_TEAM_NAME = "Наша команда"


def _migrate_initial_team(conn):
    """Once, when teams first appear: everyone already registered becomes one team.

    Before teams every user saw every battle, so this keeps the existing group seeing what
    it saw. The flag stops it from ever running again, so later sign-ups stay teamless.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO schema_flags (name) VALUES ('initial_team') ON CONFLICT DO NOTHING RETURNING name")
            if cur.fetchone() is None:
                conn.rollback()
                return
            cur.execute("SELECT id, username FROM users ORDER BY id")
            users = cur.fetchall()
            if users:
                admins = [u for u in users if u["id"] in ADMIN_USER_IDS]
                owner = (admins or users)[0]["id"]
                cur.execute("INSERT INTO teams (name, created_by) VALUES (%s, %s) RETURNING id",
                            (INITIAL_TEAM_NAME, owner))
                team_id = cur.fetchone()["id"]
                for u in users:
                    cur.execute("INSERT INTO team_members (team_id, user_id, role) VALUES (%s, %s, %s)",
                                (team_id, u["id"], "owner" if u["id"] == owner else "member"))
        conn.commit()
    except Exception as e:
        # The flag goes back with the rollback, so the next start tries again.
        conn.rollback()
        print(f"[DB] initial team migration failed, will retry on next start: {e}")


# --- Auth ---

def create_user(username: str, password_hash: str, consent_version: str | None = None) -> dict | None:
    """Create a user, or return None when the name is taken (case-insensitively).

    consent_version records which consent text the user accepted at sign-up, and when.
    """
    sql = """
        INSERT INTO users (username, password_hash, consent_version, consent_at)
        VALUES (%s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE NOW() END)
        RETURNING id, username
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username, password_hash, consent_version, consent_version))
                row = cur.fetchone()
    except psycopg2.errors.UniqueViolation:
        return None
    return dict(row) if row else None


def get_user_by_username(username: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash, token_version FROM users WHERE lower(username) = lower(%s)",
                (username,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def get_user_auth(user_id: int) -> dict | None:
    """What every authenticated request checks: the account still exists, the token's
    version is current, and which consent it has given."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, password_hash, token_version, consent_version"
                        " FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def set_password(user_id: int, password_hash: str) -> int:
    """New password; every token issued before it stops working. Returns the new version."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET password_hash = %s, token_version = token_version + 1"
                        " WHERE id = %s RETURNING token_version", (password_hash, user_id))
            return cur.fetchone()["token_version"]


def bump_token_version(user_id: int) -> int:
    """Sign out everywhere: every token issued so far stops working."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET token_version = token_version + 1 WHERE id = %s"
                        " RETURNING token_version", (user_id,))
            return cur.fetchone()["token_version"]


def record_consent(user_id: int, version: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET consent_version = %s, consent_at = NOW() WHERE id = %s",
                        (version, user_id))


def delete_user(user_id: int) -> dict:
    """Delete an account and what belongs only to it, in one transaction.

    - Battles only this user uploaded are deleted with their stats; battles someone else
      also uploaded stay, as theirs.
    - Teams this user owns pass to the member who joined earliest; a team left empty goes.
    - The account row goes last, taking its memberships, hidden players and uploads with it.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM battles b
                WHERE b.id IN (SELECT battle_id FROM user_battles WHERE user_id = %s)
                  AND NOT EXISTS (SELECT 1 FROM user_battles o WHERE o.battle_id = b.id AND o.user_id <> %s)
            """, (user_id, user_id))
            battles = cur.rowcount
            cur.execute("SELECT team_id FROM team_members WHERE user_id = %s AND role = 'owner'", (user_id,))
            moved = deleted_teams = 0
            for row in cur.fetchall():
                cur.execute("""
                    SELECT user_id FROM team_members WHERE team_id = %s AND user_id <> %s
                    ORDER BY joined_at, user_id LIMIT 1
                """, (row["team_id"], user_id))
                heir = cur.fetchone()
                if heir:
                    cur.execute("UPDATE team_members SET role = 'owner' WHERE team_id = %s AND user_id = %s",
                                (row["team_id"], heir["user_id"]))
                    moved += 1
                else:
                    cur.execute("DELETE FROM teams WHERE id = %s", (row["team_id"],))
                    deleted_teams += 1
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    return {"battles_deleted": battles, "teams_handed_over": moved, "teams_deleted": deleted_teams}


# --- Battles ---

def save_battle(battle_dict: dict, user_id: int) -> str:
    """Insert battle + stats, deduplicated by battle_hash.

    Returns "created" (new battle), "linked" (someone had already uploaded this battle
    and it is now in this user's history too) or "already" (nothing changed).

    A battle stored before extended details were parsed gets them from the next
    upload of the same replay, so old history can be filled in by uploading it again.
    """
    sql_battle = """
        INSERT INTO battles (filename, map_name, date_time, battle_hash, result, player_team, winner_team, mode, details)
        VALUES (%(filename)s, %(map_name)s, %(date_time)s, %(battle_hash)s, %(result)s, %(player_team)s, %(winner_team)s, %(mode)s, %(details)s)
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
            mode = battle_dict.get("mode") or battle_mode(battle_dict.get("details"))
            cur.execute(sql_battle, {**battle_dict, "mode": mode, "details": details})
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


def get_battles_list(scope=None, viewer_id: int | None = None) -> list[dict]:
    join, params = _scope_join(scope)
    sql = f"""
        SELECT b.id, b.date_time, b.map_name, b.result, b.filename, b.mode,
               COUNT(DISTINCT ps.id) AS player_count,
               BOOL_OR(own.user_id IS NOT NULL) AS mine
        FROM battles b
        {join}
        LEFT JOIN player_stats ps ON ps.battle_id = b.id
        LEFT JOIN user_battles own ON own.battle_id = b.id AND own.user_id = %s
        GROUP BY b.id, b.date_time, b.map_name, b.result, b.filename, b.mode
        ORDER BY b.date_time DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, viewer_id])
            return [dict(r) for r in cur.fetchall()]


def get_battles_export(
    scope=None,
    viewer_id: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Every battle in scope with all its players, oldest first.

    date_from / date_to are inclusive YYYY-MM-DD days, compared against the date part
    of date_time. The recorder block is the viewer's own upload of the battle, if any.
    """
    join, params = _scope_join(scope)
    where, where_params = [], []
    if date_from:
        where.append("left(b.date_time, 10) >= %s")
        where_params.append(date_from)
    if date_to:
        where.append("left(b.date_time, 10) <= %s")
        where_params.append(date_to)
    sql_battles = f"""
        SELECT b.id, b.date_time, b.map_name, b.result, b.winner_team, b.player_team,
               b.battle_hash, b.filename, b.mode, b.details,
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

@dataclass(frozen=True)
class Scope:
    """Which battles a query looks at.

    kind: "mine" (uploaded by user_id), "team" (uploaded by any member of team_id),
    "visible" (mine plus every team user_id is in) or "all" (the whole database, admin only).
    mode narrows any of them to one battle mode, date_from / date_to (inclusive YYYY-MM-DD)
    to a period.
    """
    kind: str
    user_id: int | None = None
    team_id: int | None = None
    mode: str | None = None
    date_from: str | None = None
    date_to: str | None = None


def _as_scope(scope) -> Scope:
    if isinstance(scope, Scope):
        return scope
    # A bare user id is the "mine" scope, None the whole database.
    return Scope("mine", user_id=scope) if scope else Scope("all")


def _scope_join(scope) -> tuple[str, list]:
    """JOIN that narrows `battles b` down to the battles in scope."""
    scope = _as_scope(scope)
    conds, cond_params = [], []
    if scope.mode:
        conds.append("bs.mode = %s")
        cond_params.append(scope.mode)
    if scope.date_from:
        conds.append("left(bs.date_time, 10) >= %s")
        cond_params.append(scope.date_from)
    if scope.date_to:
        conds.append("left(bs.date_time, 10) <= %s")
        cond_params.append(scope.date_to)
    if scope.kind == "all":
        if not conds:
            return "", []
        return (f"JOIN (SELECT bs.id AS battle_id FROM battles bs WHERE {' AND '.join(conds)}) sc"
                " ON sc.battle_id = b.id", cond_params)
    if scope.kind == "mine":
        owners, params = "ub.user_id = %s", [scope.user_id]
    elif scope.kind == "team":
        owners, params = "ub.user_id IN (SELECT user_id FROM team_members WHERE team_id = %s)", [scope.team_id]
    elif scope.kind == "visible":
        owners = ("(ub.user_id = %s OR ub.user_id IN (SELECT m2.user_id FROM team_members m1"
                  " JOIN team_members m2 ON m2.team_id = m1.team_id WHERE m1.user_id = %s))")
        params = [scope.user_id, scope.user_id]
    else:
        raise ValueError(f"unknown scope {scope.kind!r}")
    extra = "".join(f" AND {c}" for c in conds)
    return (
        "JOIN (SELECT DISTINCT ub.battle_id FROM user_battles ub JOIN battles bs ON bs.id = ub.battle_id"
        f" WHERE {owners}{extra}) sc ON sc.battle_id = b.id",
        [*params, *cond_params],
    )


# Standard and Onslaught battles are matchmade: everyone else in them is a stranger, so
# only whoever uploaded the battle and their platoon (same non-zero prebattleID; solo
# players have 0) count as "ours". Clan battles are played by the team itself: everyone.
_SQUAD_FOCUS = f"""
        AND (b.mode IS NULL OR b.mode NOT IN ({", ".join(f"'{m}'" for m in SQUAD_MODES)}) OR EXISTS (
            SELECT 1 FROM user_battles fu
            JOIN player_stats me ON me.battle_id = fu.battle_id AND me.name = fu.recorder->>'playerName'
            WHERE fu.battle_id = ps.battle_id
              AND (me.id = ps.id OR (COALESCE(me.details->>'prebattleID', '0') <> '0'
                                     AND me.details->>'prebattleID' = ps.details->>'prebattleID'))
        ))"""


def _hidden_filter(viewer_id: int | None) -> tuple[str, list]:
    """WHERE fragment that drops the players this viewer has hidden."""
    if viewer_id:
        return (
            "AND ps.name NOT IN (SELECT name FROM hidden_players WHERE user_id = %s)",
            [viewer_id],
        )
    return "", []


def get_db_totals(scope=None) -> dict:
    join, params = _scope_join(scope)
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


def count_players(scope=None, viewer_id: int | None = None) -> int:
    """How many distinct team-mates appear at all, before any min_battles filter."""
    join, params = _scope_join(scope)
    hidden, hidden_params = _hidden_filter(viewer_id)
    sql = f"""
        SELECT COUNT(DISTINCT ps.name) AS n
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {_SQUAD_FOCUS}
        {hidden}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params])
            return int(cur.fetchone()["n"])


def _wn8_by(scope, viewer_id: int | None, per_vehicle: bool, names: list[str] | None = None) -> dict:
    """WN8 per player (or per player and tank) over the same rows the summaries average.

    The totals are grouped by tank here and folded in Python, because the expected values
    live in a file (wn8.py), not in the database."""
    join, params = _scope_join(scope)
    hidden, hidden_params = _hidden_filter(viewer_id)
    name_filter, name_params = ("AND ps.name = ANY(%s)", [list(names)]) if names is not None else ("", [])
    sql = f"""
        SELECT
            ps.name,
            ps.vehicle,
            ps.details->>'typeCompDescr'                                        AS tank_id,
            COUNT(*)                                                            AS battles,
            SUM(ps.damage_dealt)                                                AS damage,
            SUM(ps.frags)                                                       AS frags,
            SUM(NULLIF(ps.details->>'spotted', '')::numeric)                    AS spotted,
            SUM(NULLIF(ps.details->>'droppedCapturePoints', '')::numeric)       AS defence,
            SUM(CASE WHEN b.result = 'win' THEN 1 ELSE 0 END)                   AS wins
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
          AND ps.details->>'typeCompDescr' ~ '^[0-9]+$'
        {_SQUAD_FOCUS}
        {hidden}
        {name_filter}
        GROUP BY ps.name, ps.vehicle, ps.details->>'typeCompDescr'
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params, *name_params])
            rows = cur.fetchall()
    tallies: dict = {}
    for r in rows:
        key = (r["name"], r["vehicle"]) if per_vehicle else r["name"]
        tallies.setdefault(key, Tally()).add(
            r["tank_id"], r["battles"], r["damage"], r["frags"], r["spotted"], r["defence"], r["wins"])
    return {key: t.score() for key, t in tallies.items()}


def get_period_rows(scope=None, viewer_id: int | None = None) -> list[dict]:
    """Every row of the recorder's side in the battles in scope, oldest first, for the facts
    the language model is given (insights.py).

    `focus` marks "our" players: the ones every other summary averages (_SQUAD_FOCUS, minus the
    players this viewer hid). The rest of the side stays in as the baseline: the allies in the
    very same battles."""
    join, params = _scope_join(scope)
    hidden, hidden_params = _hidden_filter(viewer_id)
    sql = f"""
        SELECT
            b.id AS battle_id, b.date_time, b.result, b.map_name, b.mode,
            NULLIF(b.details->>'duration', '')::numeric              AS duration,
            ps.name, ps.vehicle,
            ps.details->>'typeCompDescr'                             AS tank_id,
            ps.damage_dealt, ps.assists, ps.frags, ps.blocked, ps.damage_received,
            ps.survived, ps.shots, ps.hits, ps.piercings,
            NULLIF(ps.details->>'spotted', '')::numeric              AS spotted,
            NULLIF(ps.details->>'lifeTime', '')::numeric             AS lifetime,
            NULLIF(ps.details->>'comp7PrestigePoints', '')::numeric  AS prestige,
            COALESCE(NULLIF(ps.details->>'prebattleID', ''), '0')    AS platoon,
            (TRUE {_SQUAD_FOCUS} {hidden})                           AS focus
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        ORDER BY b.date_time, b.id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*hidden_params, *params])
            return [dict(r) for r in cur.fetchall()]


def get_rating_rows(scope, user_id: int) -> list[dict]:
    """The user's own Onslaught standing after each battle in scope, oldest first. Only the
    recorder's rating is in a replay, so this is per uploader, never for teammates."""
    join, params = _scope_join(scope)
    sql = f"""
        SELECT b.id AS battle_id, b.date_time, b.result,
               ub.recorder->>'playerName'                                   AS name,
               NULLIF(ub.recorder->>'comp7Rating', '')::int                AS rating,
               NULLIF(ub.recorder->>'comp7RatingDelta', '')::int           AS delta,
               COALESCE((ub.recorder->>'comp7QualActive')::boolean, FALSE) AS qualifying
        FROM battles b
        {join}
        JOIN user_battles ub ON ub.battle_id = b.id AND ub.user_id = %s
        WHERE b.mode = 'onslaught'
        ORDER BY b.date_time, b.id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, user_id])
            return [dict(r) for r in cur.fetchall()]


def get_explanation(cache_key: str) -> dict | None:
    """A stored answer for exactly these facts, prompt and model, if one exists."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT answer, problems, model, created_at FROM explanations"
                " WHERE cache_key = %s AND status = 'ok' ORDER BY id DESC LIMIT 1",
                (cache_key,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def start_explanation(user_id: int, cache_key: str, mode: str, model: str) -> int:
    """Book an analysis before the model is called, so it counts against the limits at once."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO explanations (cache_key, user_id, mode, status, model)"
                " VALUES (%s, %s, %s, 'pending', %s) RETURNING id",
                (cache_key, user_id, mode, model),
            )
            return int(cur.fetchone()["id"])


def finish_explanation(explanation_id: int, status: str, answer=None, problems=None,
                       model: str | None = None, usage=None, error_kind: str | None = None,
                       error=None, duration_ms: int | None = None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE explanations SET status = %s, answer = %s, problems = %s,"
                " model = COALESCE(%s, model), usage = %s, error_kind = %s, error = %s,"
                " duration_ms = %s WHERE id = %s",
                (status, _json_or_null(answer), _json_or_null(problems), model,
                 _json_or_null(usage), error_kind, _json_or_null(error), duration_ms, explanation_id),
            )


def get_explanation_log(limit: int = 100) -> dict:
    """The admin page's view of the AI analysis: the latest attempts with their causes of
    failure, and per-day totals (statuses, error kinds, tokens) for the last 30 days."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT e.id, e.created_at, u.username, e.mode, e.status, e.error_kind, e.error,"
                "       e.problems, e.model, e.usage, e.duration_ms"
                " FROM explanations e LEFT JOIN users u ON u.id = e.user_id"
                " ORDER BY e.id DESC LIMIT %s",
                (limit,),
            )
            recent = [dict(r) for r in cur.fetchall()]
            cur.execute("""
                SELECT to_char(created_at, 'YYYY-MM-DD') AS day,
                       COUNT(*) FILTER (WHERE status = 'ok')      AS ok,
                       COUNT(*) FILTER (WHERE status = 'failed')  AS failed,
                       COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                       COALESCE(SUM((usage->>'input_tokens')::numeric), 0)  AS input_tokens,
                       COALESCE(SUM((usage->>'output_tokens')::numeric), 0) AS output_tokens,
                       COALESCE(SUM((usage->>'cache_read_input_tokens')::numeric), 0) AS cache_read_tokens
                FROM explanations
                WHERE created_at > NOW() - INTERVAL '30 days'
                GROUP BY 1 ORDER BY 1 DESC
            """)
            by_day = [dict(r) for r in cur.fetchall()]
            cur.execute(
                "SELECT error_kind, COUNT(*) AS n, MAX(created_at) AS last_at FROM explanations"
                " WHERE status = 'failed' AND created_at > NOW() - INTERVAL '7 days'"
                " GROUP BY 1 ORDER BY 2 DESC"
            )
            by_kind = [dict(r) for r in cur.fetchall()]
    return {"recent": recent, "by_day": by_day, "errors_7d": by_kind}


def count_explanations(user_id: int | None, hours: int = 24) -> int:
    """Analyses asked of the model in the last `hours` (cached answers are not asked again):
    by this user, or by everyone when user_id is None. Failed and pending ones count too."""
    who, params = ("user_id = %s AND ", [user_id]) if user_id is not None else ("", [])
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM explanations"
                " WHERE " + who + "created_at > NOW() - make_interval(hours => %s)",
                [*params, hours],
            )
            return int(cur.fetchone()["n"])


def get_recorder_names(user_id: int) -> list[str]:
    """Every in-game name this user's replays were recorded under, most used first: the
    accounts of one person (a main and its alts), "you" in the facts."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT recorder->>'playerName' AS name FROM user_battles"
                " WHERE user_id = %s AND recorder->>'playerName' IS NOT NULL"
                " GROUP BY 1 ORDER BY COUNT(*) DESC, 1",
                (user_id,),
            )
            return [r["name"] for r in cur.fetchall()]


def get_player_summary(
    scope=None,
    viewer_id: int | None = None,
    min_battles: int = 1,
) -> list[dict]:
    """Team-mate averages. min_battles drops the one-off randoms that otherwise
    bury the people you actually play with — with a few hundred battles uploaded,
    most names in the table appear exactly once."""
    join, params = _scope_join(scope)
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
            ROUND(AVG(ps.shots)::numeric, 1)                                 AS avg_shots,
            ROUND(AVG(ps.hits)::numeric, 1)                                  AS avg_hits,
            ROUND(AVG(ps.piercings)::numeric, 1)                             AS avg_piercings,
            -- Ratios of the totals, not of the rounded averages: 7.4 shots and 5.6 hits
            -- would otherwise turn into 6 of 7.
            ROUND(SUM(ps.hits)::numeric * 100 / NULLIF(SUM(ps.shots), 0), 1)     AS accuracy,
            ROUND(SUM(ps.piercings)::numeric * 100 / NULLIF(SUM(ps.hits), 0), 1) AS penetration,
            ROUND(AVG(ps.survived::int) * 100, 1)                            AS survival,
            ROUND(AVG(ps.xp))                                                AS avg_xp,
            ROUND(AVG(NULLIF(ps.details->>'spotted', '')::numeric), 1)       AS avg_spotted,
            ROUND(AVG(NULLIF(ps.details->>'lifeTime', '')::numeric))         AS avg_lifetime
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {_SQUAD_FOCUS}
        {hidden}
        GROUP BY ps.name
        HAVING COUNT(DISTINCT ps.battle_id) >= %s
        ORDER BY avg_damage DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params, max(1, min_battles)])
            players = [dict(r) for r in cur.fetchall()]
    scores = _wn8_by(scope, viewer_id, per_vehicle=False) if players else {}
    for p in players:
        p["wn8"] = scores.get(p["name"])
    return players


def get_daily_summary(scope=None, viewer_id: int | None = None) -> dict:
    """Day by day, and for the whole scope: battles and results, plus the averages of "our"
    players (the recorder's side; in matchmade modes only the uploader and their platoon).

    Averages are per player per battle, so a 7-man clan battle weighs seven rows.
    """
    join, params = _scope_join(scope)
    hidden, hidden_params = _hidden_filter(viewer_id)
    metrics = """
            COUNT(DISTINCT b.id)                                            AS battles,
            COUNT(DISTINCT b.id) FILTER (WHERE b.result = 'win')            AS wins,
            COUNT(DISTINCT b.id) FILTER (WHERE b.result = 'loss')           AS losses,
            COUNT(DISTINCT b.id) FILTER (WHERE b.result = 'draw')           AS draws,
            ROUND(AVG(ps.damage_dealt))                                     AS avg_damage,
            ROUND(AVG(ps.assists))                                          AS avg_assists,
            ROUND(AVG(ps.blocked))                                          AS avg_blocked,
            ROUND(AVG(NULLIF(ps.details->>'spotted', '')::numeric), 2)      AS avg_spotted,
            ROUND(SUM(ps.hits)::numeric * 100 / NULLIF(SUM(ps.shots), 0), 1) AS accuracy
    """
    body = f"""
        FROM battles b
        {join}
        JOIN player_stats ps ON ps.battle_id = b.id AND ps.team = b.player_team
        WHERE TRUE
        {_SQUAD_FOCUS}
        {hidden}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT left(b.date_time, 10) AS day, {metrics} {body} GROUP BY 1 ORDER BY 1",
                        [*params, *hidden_params])
            days = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT {metrics} {body}", [*params, *hidden_params])
            totals = dict(cur.fetchone())
    return {"days": days, "totals": totals}


def get_map_summary(scope=None) -> list[dict]:
    join, params = _scope_join(scope)
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
    scope=None,
    viewer_id: int | None = None,
    names: list[str] | None = None,
) -> list[dict]:
    """Per-tank averages, restricted to `names` so this table shows the same
    players as the summary above."""
    join, params = _scope_join(scope)
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
            SUM(CASE WHEN b.result = 'loss' THEN 1 ELSE 0 END)  AS losses,
            ROUND(AVG(ps.blocked))                              AS avg_blocked,
            ROUND(AVG(ps.shots)::numeric, 1)                    AS avg_shots,
            ROUND(SUM(ps.hits)::numeric * 100 / NULLIF(SUM(ps.shots), 0), 1)     AS accuracy,
            ROUND(SUM(ps.piercings)::numeric * 100 / NULLIF(SUM(ps.hits), 0), 1) AS penetration,
            ROUND(AVG(ps.survived::int) * 100, 1)               AS survival
        FROM player_stats ps
        JOIN battles b ON b.id = ps.battle_id
        {join}
        WHERE ps.team = b.player_team
        {_SQUAD_FOCUS}
        {hidden}
        {name_filter}
        GROUP BY ps.name, ps.vehicle
        ORDER BY ps.name, battles DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, *hidden_params, *name_params])
            vehicles = [dict(r) for r in cur.fetchall()]
    scores = _wn8_by(scope, viewer_id, per_vehicle=True, names=names) if vehicles else {}
    for v in vehicles:
        v["wn8"] = scores.get((v["name"], v["vehicle"]))
    return vehicles


# --- Teams ---

def create_team(name: str, owner_id: int) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO teams (name, created_by) VALUES (%s, %s) RETURNING id, name",
                        (name, owner_id))
            team = dict(cur.fetchone())
            cur.execute("INSERT INTO team_members (team_id, user_id, role) VALUES (%s, %s, 'owner')",
                        (team["id"], owner_id))
    return {**team, "role": "owner", "members": 1}


def get_user_teams(user_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.id, t.name, m.role,
                       (SELECT COUNT(*) FROM team_members x WHERE x.team_id = t.id) AS members
                FROM team_members m JOIN teams t ON t.id = m.team_id
                WHERE m.user_id = %s
                ORDER BY t.name, t.id
            """, (user_id,))
            return [dict(r) for r in cur.fetchall()]


def get_team(team_id: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, created_at FROM teams WHERE id = %s", (team_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def get_member_role(team_id: int, user_id: int) -> str | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM team_members WHERE team_id = %s AND user_id = %s", (team_id, user_id))
            row = cur.fetchone()
    return row["role"] if row else None


def get_team_members(team_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id AS user_id, u.username, m.role, m.joined_at,
                       (SELECT COUNT(*) FROM user_battles ub WHERE ub.user_id = u.id) AS battles
                FROM team_members m JOIN users u ON u.id = m.user_id
                WHERE m.team_id = %s
                ORDER BY m.role = 'owner' DESC, lower(u.username)
            """, (team_id,))
            return [dict(r) for r in cur.fetchall()]


def rename_team(team_id: int, name: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE teams SET name = %s WHERE id = %s", (name, team_id))


def delete_team(team_id: int) -> None:
    """Only the team and its memberships go: the battles belong to whoever uploaded them."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM teams WHERE id = %s", (team_id,))


def remove_member(team_id: int, user_id: int) -> bool:
    """Drop a member. The owner cannot be removed: they delete the team instead."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM team_members WHERE team_id = %s AND user_id = %s AND role <> 'owner'",
                        (team_id, user_id))
            return cur.rowcount > 0


def create_invite(team_id: int, created_by: int, code: str, days: int, max_uses: int | None) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO team_invites (code, team_id, created_by, expires_at, max_uses)
                VALUES (%s, %s, %s, NOW() + make_interval(days => %s), %s)
                RETURNING code, expires_at, max_uses, uses
            """, (code, team_id, created_by, days, max_uses))
            return dict(cur.fetchone())


def list_invites(team_id: int) -> list[dict]:
    """Invites that still work: not expired and not used up."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT code, expires_at, max_uses, uses FROM team_invites
                WHERE team_id = %s AND expires_at > NOW() AND (max_uses IS NULL OR uses < max_uses)
                ORDER BY created_at DESC
            """, (team_id,))
            return [dict(r) for r in cur.fetchall()]


def revoke_invite(team_id: int, code: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM team_invites WHERE team_id = %s AND code = %s", (team_id, code))
            return cur.rowcount > 0


def get_invite(code: str) -> dict | None:
    """A working invite with its team's name, or None if it is unknown, expired or used up."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT i.code, i.team_id, t.name AS team_name, i.expires_at
                FROM team_invites i JOIN teams t ON t.id = i.team_id
                WHERE i.code = %s AND i.expires_at > NOW() AND (i.max_uses IS NULL OR i.uses < i.max_uses)
            """, (code,))
            row = cur.fetchone()
    return dict(row) if row else None


def accept_invite(code: str, user_id: int) -> tuple[str, int | None]:
    """("joined" | "already" | "invalid", team_id). A use is only spent on a real join."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT team_id FROM team_invites
                WHERE code = %s AND expires_at > NOW() AND (max_uses IS NULL OR uses < max_uses)
                FOR UPDATE
            """, (code,))
            row = cur.fetchone()
            if not row:
                return "invalid", None
            team_id = row["team_id"]
            cur.execute("INSERT INTO team_members (team_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (team_id, user_id))
            if not cur.rowcount:
                return "already", team_id
            cur.execute("UPDATE team_invites SET uses = uses + 1 WHERE code = %s", (code,))
            return "joined", team_id


# --- Admin ---

def get_admin_overview() -> dict:
    """Size of the whole database, for the admin page."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT (SELECT COUNT(*) FROM battles)      AS battles,
                       (SELECT COUNT(*) FROM player_stats) AS player_rows,
                       (SELECT COUNT(*) FROM users)        AS users,
                       (SELECT COUNT(*) FROM teams)        AS teams,
                       (SELECT COUNT(DISTINCT details->>'accountDBID') FROM player_stats) AS accounts
            """)
            overview = dict(cur.fetchone())
            cur.execute("SELECT COALESCE(mode, 'other') AS mode, COUNT(*) AS battles"
                        " FROM battles GROUP BY 1 ORDER BY 2 DESC")
            overview["by_mode"] = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT left(date_time, 10) AS day, COUNT(*) AS battles FROM battles"
                        " GROUP BY 1 ORDER BY 1 DESC LIMIT 30")
            overview["by_day"] = [dict(r) for r in cur.fetchall()]
    return overview
