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
from typing import List, Optional, Set, Tuple

from .models import BatchIn
from .planner import LogSegment, RecoveryPlan, Snapshot

#: Busy timeout so concurrent BEGIN IMMEDIATE writers wait for each other
#: instead of failing immediately with "database is locked".
BUSY_TIMEOUT_SECONDS = 30.0

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
CREATE TABLE IF NOT EXISTS recovery_jobs (
    id                TEXT PRIMARY KEY,
    at_utc            TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('in_progress', 'finished')),
    snapshot_id       TEXT NOT NULL,
    snapshot_created  TEXT NOT NULL,
    snapshot_seq      INTEGER NOT NULL,
    snapshot_format   TEXT NOT NULL,
    endpoint          INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_job_steps (
    job_id      TEXT NOT NULL,
    step_order  INTEGER NOT NULL,
    segment_id  TEXT NOT NULL,
    from_seq    INTEGER NOT NULL,
    to_seq      INTEGER NOT NULL,
    PRIMARY KEY (job_id, step_order)
);
CREATE INDEX IF NOT EXISTS idx_job_steps_lookup
    ON recovery_job_steps (segment_id);
"""


def get_db_path() -> str:
    return os.environ.get("DB_PATH", "recovery.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path(), timeout=BUSY_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    return conn


def connect_immediate() -> sqlite3.Connection:
    """Open a connection holding a write transaction (BEGIN IMMEDIATE).

    Checks and writes performed through this connection see the same committed
    database state and are serialized against other writers, so read-check
    decisions cannot be invalidated by a concurrent commit.
    """
    conn = sqlite3.connect(get_db_path(), timeout=BUSY_TIMEOUT_SECONDS)
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


# Recovery jobs are frozen plans: the snapshot fields and replay intervals are
# copied into the job tables at creation, so they stay readable after the
# underlying records are deleted once the job is finished.
JOB_STATUS_IN_PROGRESS = "in_progress"
JOB_STATUS_FINISHED = "finished"


def insert_job(conn: sqlite3.Connection, job_id: str, at: datetime, plan: RecoveryPlan) -> None:
    """Persist a new in-progress job with its frozen snapshot and replay steps.

    Runs inside the caller's write transaction; the caller commits.
    """
    snap = plan.snapshot
    conn.execute(
        """
        INSERT INTO recovery_jobs
            (id, at_utc, status, snapshot_id, snapshot_created,
             snapshot_seq, snapshot_format, endpoint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            at.isoformat(),
            JOB_STATUS_IN_PROGRESS,
            snap.id,
            snap.created_at.isoformat(),
            snap.applied_seq,
            snap.format,
            plan.endpoint,
        ),
    )
    conn.executemany(
        """
        INSERT INTO recovery_job_steps
            (job_id, step_order, segment_id, from_seq, to_seq)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (job_id, order, step.segment_id, step.from_seq, step.to_seq)
            for order, step in enumerate(plan.steps)
        ],
    )


def load_job(conn: sqlite3.Connection, job_id: str) -> Optional[sqlite3.Row]:
    """Return the job row for ``job_id`` or ``None`` if it does not exist."""
    return conn.execute(
        "SELECT * FROM recovery_jobs WHERE id = ?", (job_id,)
    ).fetchone()


def load_job_steps(conn: sqlite3.Connection, job_id: str) -> List[sqlite3.Row]:
    """Return the frozen replay steps of a job in replay order."""
    return list(
        conn.execute(
            """
            SELECT segment_id, from_seq, to_seq
            FROM recovery_job_steps
            WHERE job_id = ?
            ORDER BY step_order
            """,
            (job_id,),
        )
    )


def finish_job(conn: sqlite3.Connection, job_id: str) -> None:
    """Mark a job finished. Idempotent: a finished job stays finished.

    Runs inside the caller's write transaction; the caller commits.
    """
    conn.execute(
        "UPDATE recovery_jobs SET status = ? WHERE id = ?",
        (JOB_STATUS_FINISHED, job_id),
    )


def in_progress_usage(conn: sqlite3.Connection, ids: Set[str]) -> dict:
    """Map record ids used by in-progress jobs onto the using job ids.

    Only the snapshot chosen for a job and the segments actually replayed by
    it count as "used". Finished jobs do not protect anything.
    """
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    arg_ids = list(ids)
    usage: dict = {record_id: set() for record_id in arg_ids}
    snapshot_rows = conn.execute(
        f"""
        SELECT j.id AS job_id, j.snapshot_id AS record_id
        FROM recovery_jobs j
        WHERE j.status = ? AND j.snapshot_id IN ({placeholders})
        """,
        [JOB_STATUS_IN_PROGRESS, *arg_ids],
    ).fetchall()
    for row in snapshot_rows:
        usage[row["record_id"]].add(row["job_id"])
    step_rows = conn.execute(
        f"""
        SELECT j.id AS job_id, s.segment_id AS record_id
        FROM recovery_jobs j
        JOIN recovery_job_steps s ON s.job_id = j.id
        WHERE j.status = ? AND s.segment_id IN ({placeholders})
        """,
        [JOB_STATUS_IN_PROGRESS, *arg_ids],
    ).fetchall()
    for row in step_rows:
        usage[row["record_id"]].add(row["job_id"])
    return {record_id: job_ids for record_id, job_ids in usage.items() if job_ids}
