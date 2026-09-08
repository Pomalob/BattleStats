"""Clear data from the database.

Usage:
    DATABASE_URL=<url> python scripts/clear_db.py [--users] [--yes]

By default keeps user accounts and only clears battles and stats.
Pass --users to drop the accounts too.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import psycopg2

from config import DATABASE_URL
from database import build_dsn


def main():
    drop_users = "--users" in sys.argv
    tables = "user_battles, hidden_players, player_stats, battles"
    if drop_users:
        tables += ", users"

    if "--yes" not in sys.argv:
        target = DATABASE_URL.rsplit("@", 1)[-1] or "(unset)"
        print(f"About to TRUNCATE {tables}")
        print(f"on {target}")
        if input("Type 'yes' to continue: ").strip().lower() != "yes":
            sys.exit("Aborted.")

    try:
        conn = psycopg2.connect(build_dsn(DATABASE_URL))
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}")

    with conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    conn.close()

    print(f"Cleared: {tables}")


if __name__ == "__main__":
    main()
