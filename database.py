import sqlite3
from datetime import date, timedelta

DB_NAME = "ppi.db"


def initialize_database():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # ----------------------------------------------------------
    # ATHLETES TABLE
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS athletes (
            athlete_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ----------------------------------------------------------
    # GOALS (Per Athlete)
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS goals (
            goal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            athlete_id INTEGER,
            event_name TEXT,
            distance_km REAL,
            goal_time TEXT,
            race_date TEXT,
            elevation_type TEXT,
            status TEXT,
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
    """)

    # ----------------------------------------------------------
    # DAILY METRICS (Per Athlete)
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            activity_id INTEGER,
            athlete_id INTEGER,
            timestamp TEXT,
            distance_km REAL,
            moving_time_sec REAL,
            avg_hr REAL,
            elevation_gain_m REAL,
            stress REAL,
            atl REAL,
            ctl REAL,
            tsb REAL,
            readiness REAL,
            PRIMARY KEY (activity_id, athlete_id),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
    """)

    # ----------------------------------------------------------
    # HEALTH STATUS (Per Athlete)
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS health_status (
            athlete_id INTEGER,
            log_date TEXT,
            resting_hr INTEGER,
            hr_delta REAL,
            illness_flag INTEGER DEFAULT 0,
            override_active INTEGER DEFAULT 0,
            PRIMARY KEY (athlete_id, log_date),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
    """)

    # ----------------------------------------------------------
    # PRESCRIBED SESSIONS (Per Athlete)
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prescribed_sessions (
            athlete_id INTEGER,
            session_date TEXT,
            session_type TEXT,
            min_km REAL,
            max_km REAL,
            intensity TEXT,
            completed INTEGER DEFAULT 0,
            compliance_score REAL DEFAULT 0,
            PRIMARY KEY (athlete_id, session_date),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
    """)

    conn.commit()
    conn.close()

def create_athlete(name):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO athletes (name)
        VALUES (?)
    """, (name,))

    conn.commit()
    athlete_id = cursor.lastrowid
    conn.close()

    return athlete_id


def fetch_all_athletes():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM athletes")
    rows = cursor.fetchall()

    conn.close()
    return rows    

def insert_goal(athlete_id, event_name, distance_km, goal_time, race_date, elevation_type):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Archive previous ACTIVE goal for this athlete
    cursor.execute("""
        UPDATE goals
        SET status = 'ARCHIVED'
        WHERE athlete_id = ? AND status = 'ACTIVE'
    """, (athlete_id,))

    cursor.execute("""
        INSERT INTO goals
        (athlete_id, event_name, distance_km, goal_time, race_date, elevation_type, status)
        VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
    """, (athlete_id, event_name, distance_km, goal_time, race_date, elevation_type))

    conn.commit()
    conn.close()

def fetch_active_goal(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM goals
        WHERE athlete_id = ? AND status = 'ACTIVE'
        LIMIT 1
    """, (athlete_id,))

    row = cursor.fetchone()
    conn.close()
    return row

def save_metrics(athlete_id, activity_id, timestamp, distance_km,
                 moving_time_sec, avg_hr, elevation_gain_m,
                 stress, atl, ctl, tsb, readiness):

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO daily_metrics
        (activity_id, athlete_id, timestamp, distance_km,
         moving_time_sec, avg_hr, elevation_gain_m,
         stress, atl, ctl, tsb, readiness)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        activity_id,
        athlete_id,
        timestamp,
        distance_km,
        moving_time_sec,
        avg_hr,
        elevation_gain_m,
        stress,
        atl,
        ctl,
        tsb,
        readiness
    ))

    conn.commit()
    conn.close()

def fetch_latest_metric(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM daily_metrics
        WHERE athlete_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (athlete_id,))

    row = cursor.fetchone()
    conn.close()
    return row

def fetch_all_metrics(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM daily_metrics
        WHERE athlete_id = ?
        ORDER BY timestamp ASC
    """, (athlete_id,))

    rows = cursor.fetchall()
    conn.close()
    return rows

def log_health_status(athlete_id, resting_hr, hr_delta, illness_flag, override_active):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    today = date.today().isoformat()

    cursor.execute("""
        INSERT OR REPLACE INTO health_status
        (athlete_id, log_date, resting_hr, hr_delta, illness_flag, override_active)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (athlete_id, today, resting_hr, hr_delta, illness_flag, override_active))

    conn.commit()
    conn.close()

def save_prescribed_session(athlete_id, session_date, session_type, min_km, max_km, intensity):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO prescribed_sessions
        (athlete_id, session_date, session_type, min_km, max_km, intensity)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (athlete_id, session_date, session_type, min_km, max_km, intensity))

    conn.commit()
    conn.close()

def fetch_today_prescription(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    today = date.today().isoformat()

    cursor.execute("""
        SELECT * FROM prescribed_sessions
        WHERE athlete_id = ? AND session_date = ?
    """, (athlete_id, today))

    row = cursor.fetchone()
    conn.close()
    return row

def fetch_today_prescription(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    today = date.today().isoformat()

    cursor.execute("""
        SELECT * FROM prescribed_sessions
        WHERE athlete_id = ? AND session_date = ?
    """, (athlete_id, today))

    row = cursor.fetchone()
    conn.close()
    return row

def update_compliance(athlete_id, session_date, score):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE prescribed_sessions
        SET completed = 1,
            compliance_score = ?
        WHERE athlete_id = ? AND session_date = ?
    """, (score, athlete_id, session_date))

    conn.commit()
    conn.close()        

def get_last_7_day_hr_avg(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    seven_days_ago = (date.today() - timedelta(days=7)).isoformat()

    cursor.execute("""
        SELECT resting_hr FROM health_status
        WHERE athlete_id = ? AND log_date >= ?
    """, (athlete_id, seven_days_ago))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return None

    return sum(r[0] for r in rows) / len(rows)

def fetch_athlete(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM athletes
        WHERE athlete_id = ?
    """, (athlete_id,))

    row = cursor.fetchone()
    conn.close()
    return row

def ensure_default_athlete(name="Sujeeth"):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM athletes")
    rows = cursor.fetchall()

    if not rows:
        cursor.execute("""
            INSERT INTO athletes (name)
            VALUES (?)
        """, (name,))
        conn.commit()
        athlete_id = cursor.lastrowid
    else:
        athlete_id = rows[0][0]

    conn.close()
    return athlete_id

def weekly_compliance(athlete_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    monday = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    sunday = (date.today() + timedelta(days=6 - date.today().weekday())).isoformat()

    cursor.execute("""
        SELECT compliance_score FROM prescribed_sessions
        WHERE athlete_id = ? AND session_date BETWEEN ? AND ?
    """, (athlete_id, monday, sunday))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return 0

    total = sum(r[0] for r in rows if r[0] is not None)
    return round(total / len(rows), 1)
