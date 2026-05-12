"""
Clear all data from the database.
Usage: DATABASE_URL=<url> python scripts/clear_db.py [--users]

By default keeps users, only clears battles/stats.
Pass --users to also drop all user accounts.
"""
import os
import sys
import psycopg2

def get_conn():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        sys.exit("ERROR: DATABASE_URL not set")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return psycopg2.connect(url)


def main():
    drop_users = "--users" in sys.argv

    conn = get_conn()
    cur = conn.cursor()

    if drop_users:
        cur.execute("TRUNCATE user_battles, player_stats, battles, users RESTART IDENTITY CASCADE")
        print("Cleared: user_battles, player_stats, battles, users")
    else:
        cur.execute("TRUNCATE user_battles, player_stats, battles RESTART IDENTITY CASCADE")
        print("Cleared: user_battles, player_stats, battles  (users kept)")

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
