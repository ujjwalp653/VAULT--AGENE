import sqlite3
import threading
from .config import settings

_db_lock = threading.RLock()

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS nodes (
    node_id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'UNKNOWN',
    capacity_bytes INTEGER NOT NULL DEFAULT 0,
    used_bytes INTEGER NOT NULL DEFAULT 0,
    load REAL NOT NULL DEFAULT 0,
    last_heartbeat REAL,
    last_error TEXT,
    active_ops INTEGER NOT NULL DEFAULT 0,
    total_ops INTEGER NOT NULL DEFAULT 0,
    activity_load REAL NOT NULL DEFAULT 0,
    storage_utilization REAL NOT NULL DEFAULT 0,
    last_operation_at REAL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    object_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    version INTEGER NOT NULL,
    replication_factor INTEGER NOT NULL,
    write_quorum INTEGER NOT NULL DEFAULT 1,
    read_quorum INTEGER NOT NULL DEFAULT 1,
    chunk_size INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'HEALTHY',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    version INTEGER NOT NULL,
    UNIQUE(object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    node_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'HEALTHY',
    last_verified REAL,
    PRIMARY KEY(chunk_id, node_id)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS repair_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_started REAL NOT NULL,
    ts_completed REAL,
    chunk_id TEXT NOT NULL,
    source_node INTEGER,
    target_node INTEGER,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_chunks_object ON chunks(object_id);
CREATE INDEX IF NOT EXISTS idx_replicas_chunk ON replicas(chunk_id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC);
"""


def connect() -> sqlite3.Connection:
    settings.ensure_dirs()
    conn = sqlite3.connect(settings.db_path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str):
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    with _db_lock:
        conn = connect()
        try:
            conn.executescript(SCHEMA)
            # Backward-compatible migrations for databases created by v2.0/v2.1/v2.2.
            _ensure_column(conn, "objects", "write_quorum", "INTEGER NOT NULL DEFAULT 1")
            _ensure_column(conn, "objects", "read_quorum", "INTEGER NOT NULL DEFAULT 1")
            _ensure_column(conn, "nodes", "active_ops", "INTEGER NOT NULL DEFAULT 0")
            _ensure_column(conn, "nodes", "total_ops", "INTEGER NOT NULL DEFAULT 0")
            _ensure_column(conn, "nodes", "activity_load", "REAL NOT NULL DEFAULT 0")
            _ensure_column(conn, "nodes", "storage_utilization", "REAL NOT NULL DEFAULT 0")
            _ensure_column(conn, "nodes", "last_operation_at", "REAL")
            conn.execute("UPDATE objects SET write_quorum=CASE WHEN write_quorum<1 THEN 1 ELSE write_quorum END")
            conn.execute("UPDATE objects SET read_quorum=CASE WHEN read_quorum<1 THEN 1 ELSE read_quorum END")
            conn.commit()
        finally:
            conn.close()


def execute(sql: str, params=()):
    with _db_lock:
        conn = connect()
        try:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()


def executemany(sql: str, rows):
    with _db_lock:
        conn = connect()
        try:
            conn.executemany(sql, rows)
            conn.commit()
        finally:
            conn.close()


def fetchone(sql: str, params=()):
    with _db_lock:
        conn = connect()
        try:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def fetchall(sql: str, params=()):
    with _db_lock:
        conn = connect()
        try:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()
