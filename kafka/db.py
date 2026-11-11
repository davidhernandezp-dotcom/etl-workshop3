"""Acceso a SQLite para el consumer: tablas raw, dimensiones y hechos."""
import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "storage" / "happiness.db"
SCHEMA_PATH = BASE_DIR / "sql" / "create_tables.sql"


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def connect(db_path=DB_PATH):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(Path(SCHEMA_PATH).read_text(encoding="utf-8"))
    conn.commit()
    return conn


# ---------- RAW ----------
def insert_raw(conn, topic, partition, offset, key, payload):
    """
    Guarda el mensaje original tal como llego (antes de validar/transformar).
    Devuelve raw_event_id, o None si ese (topic, partition, offset) ya estaba guardado (reentrega).
    """
    cur = conn.execute(
        """INSERT OR IGNORE INTO raw_happiness_events
           (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload, received_at, processing_status)
           VALUES (?, ?, ?, ?, ?, ?, 'RECEIVED')""",
        (topic, partition, offset, key, payload, now_iso()),
    )
    conn.commit()
    return cur.lastrowid if cur.rowcount else None


def set_status(conn, raw_event_id, status, detail=None):
    conn.execute(
        "UPDATE raw_happiness_events SET processing_status = ?, status_detail = ? WHERE raw_event_id = ?",
        (status, detail, raw_event_id),
    )
    conn.commit()


def event_id_already_valid(conn, event_id):
    """event_id debe ser unico: ya existe un evento VALID con ese id?"""
    row = conn.execute(
        """SELECT 1 FROM raw_happiness_events
           WHERE processing_status = 'VALID' AND json_extract(raw_payload, '$.event_id') = ? LIMIT 1""",
        (event_id,),
    ).fetchone()
    return row is not None


# ---------- DIMENSIONES ----------
def get_or_create_country(conn, name):
    conn.execute("INSERT OR IGNORE INTO dim_country(country_name) VALUES (?)", (name,))
    return conn.execute("SELECT country_id FROM dim_country WHERE country_name = ?", (name,)).fetchone()[0]


def get_or_create_date(conn, event_time_iso):
    dt = datetime.fromisoformat(event_time_iso)
    date_id = int(dt.strftime("%Y%m%d"))
    conn.execute(
        "INSERT OR IGNORE INTO dim_date(date_id, full_date, year, quarter, month, day) VALUES (?, ?, ?, ?, ?, ?)",
        (date_id, dt.date().isoformat(), dt.year, (dt.month - 1) // 3 + 1, dt.month, dt.day),
    )
    return date_id


# ---------- HECHOS ----------
def insert_prediction(conn, raw_event_id, event, predicted_score):
    country_id = get_or_create_country(conn, event["country"])
    date_id = get_or_create_date(conn, event["event_time"])
    actual = float(event["actual_happiness_score"])
    predicted = float(predicted_score)
    error = predicted - actual
    conn.execute(
        """INSERT INTO fact_predictions
           (raw_event_id, country_id, date_id, event_id, observation_year, actual_score, predicted_score,
            prediction_error, abs_error, event_time, prediction_timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (raw_event_id, country_id, date_id, event["event_id"], event["year"], actual, predicted,
         error, abs(error), event["event_time"], now_iso()),
    )
    conn.commit()
    return error
