"""SQLite storage layer.

The database file location is read from the ``DB_PATH`` environment variable
(default ``recovery.db`` in the working directory). The schema is created by
:func:`init_db`, which runs at application startup, so a fresh container
initializes its database automatically.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import List, Set, Tuple

from .models import BatchIn
from .planner import LogSegment, Snapshot

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    applied_seq INTEGER NOT NULL CHECK (applied_seq >= 0),
    format      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS log_segments (
    id         TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    format     TEXT NOT NULL,
    first_seq  INTEGER NOT NULL CHECK (first_seq >= 1),
    last_seq   INTEGER NOT NULL CHECK (last_seq >= first_seq)
);
"""


def get_db_path() -> str:
    return os.environ.get("DB_PATH", "recovery.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def connect_immediate() -> sqlite3.Connection:
    """Open a connection holding a write transaction (BEGIN IMMEDIATE).

    Checks and writes performed through this connection see the same committed
    database state and are serialized against other writers, so read-check
    decisions cannot be invalidated by a concurrent commit.
    """
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN IMMEDIATE")
    return conn


def init_db() -> None:
    path = get_db_path()
    if path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)


def all_ids(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute(
        "SELECT id FROM snapshots UNION SELECT id FROM log_segments"
    ).fetchall()
    return {row["id"] for row in rows}


def insert_batch(conn: sqlite3.Connection, batch: BatchIn) -> None:
    """Insert the whole batch atomically; any failure rolls everything back."""
    with conn:
        conn.executemany(
            "INSERT INTO snapshots (id, created_at, applied_seq, format) VALUES (?, ?, ?, ?)",
            [
                (s.id, s.created_at.isoformat(), s.applied_seq, s.format)
                for s in batch.snapshots
            ],
        )
        conn.executemany(
            "INSERT INTO log_segments (id, created_at, format, first_seq, last_seq) VALUES (?, ?, ?, ?, ?)",
            [
                (s.id, s.created_at.isoformat(), s.format, s.first_seq, s.last_seq)
                for s in batch.log_segments
            ],
        )


def delete_records(conn: sqlite3.Connection, ids: List[str]) -> None:
    """Delete the given ids from both tables within the current transaction.

    Ids are globally unique across the two tables, so at most one row per id
    is affected.
    """
    conn.executemany("DELETE FROM snapshots WHERE id = ?", [(i,) for i in ids])
    conn.executemany("DELETE FROM log_segments WHERE id = ?", [(i,) for i in ids])


def load_all(conn: sqlite3.Connection) -> Tuple[List[Snapshot], List[LogSegment]]:
    snapshots = [
        Snapshot(
            id=row["id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            applied_seq=row["applied_seq"],
            format=row["format"],
        )
        for row in conn.execute(
            "SELECT id, created_at, applied_seq, format FROM snapshots"
        )
    ]
    segments = [
        LogSegment(
            id=row["id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            format=row["format"],
            first_seq=row["first_seq"],
            last_seq=row["last_seq"],
        )
        for row in conn.execute(
            "SELECT id, created_at, format, first_seq, last_seq FROM log_segments"
        )
    ]
    return snapshots, segments
