import sqlite3

DB_NAME = "ppi.db"

def initialize_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_metrics (
        activity_id INTEGER PRIMARY KEY,
        activity_timestamp TEXT,
        stress REAL,
        atl REAL,
        ctl REAL,
        tsb REAL,
        readiness INTEGER
    )
""")

    conn.commit()
    conn.close()


def save_metrics(activity_id, activity_timestamp, stress, atl, ctl, tsb, readiness):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO daily_metrics
        (activity_id, activity_timestamp, stress, atl, ctl, tsb, readiness)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        activity_id,
        activity_timestamp,
        stress,
        atl,
        ctl,
        tsb,
        readiness
    ))

    conn.commit()
    conn.close()


def fetch_all_metrics():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT activity_id, activity_timestamp, stress, atl, ctl, tsb, readiness
        FROM daily_metrics
        ORDER BY activity_timestamp DESC
    """)

    rows = cursor.fetchall()

    conn.close()
    return rows

def fetch_latest_metric():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT activity_id, activity_timestamp, stress, atl, ctl, tsb, readiness
        FROM daily_metrics
        ORDER BY activity_timestamp DESC
        LIMIT 1
    """)

    row = cursor.fetchone()

    conn.close()
    return row