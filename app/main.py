"""FastAPI application: batch registration, recovery planning and jobs."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query

from . import db
from .models import BatchIn, CleanupIn, RecoveryJobIn, to_utc
from .planner import build_plan


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()  # initialize the SQLite schema on startup
    yield


app = FastAPI(
    title="Recovery Planner",
    version="1.0.0",
    description=(
        "Register database snapshots and log segments in batches, ask for the "
        "optimal recovery plan (snapshot + continuous same-format log replay) "
        "at a given UTC time, and start long-lived recovery jobs that freeze a "
        "plan and protect the records they use from cleanup until they end."
    ),
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/batches", status_code=201)
def register_batch(batch: BatchIn) -> dict:
    """Register a batch of snapshots and log segments.

    The batch is stored atomically: duplicate ids (within the batch or
    against already-registered items), invalid fields, or more than 100
    items cause the whole batch to be rejected.
    """
    conn = db.connect()
    try:
        incoming = [s.id for s in batch.snapshots] + [s.id for s in batch.log_segments]
        conflicts = sorted(set(incoming) & db.all_ids(conn))
        if conflicts:
            raise HTTPException(
                status_code=409,
                detail=f"id(s) already registered: {conflicts}; batch rejected",
            )
        db.insert_batch(conn, batch)
        return {
            "stored_snapshots": len(batch.snapshots),
            "stored_log_segments": len(batch.log_segments),
        }
    finally:
        conn.close()


@app.get("/recovery-plan")
def recovery_plan(
    at: datetime = Query(
        ...,
        description="UTC query time (ISO 8601). Only items created at or before this moment are used.",
    ),
) -> dict:
    """Compute the optimal recovery plan for the given UTC query time."""
    at_utc = to_utc(at)
    conn = db.connect()
    try:
        snapshots, segments = db.load_all(conn)
    finally:
        conn.close()

    plan = build_plan(snapshots, segments, at_utc)
    if plan is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "no recovery point: no snapshot created at or before "
                f"{at_utc.isoformat()}"
            ),
        )

    snap = plan.snapshot
    return {
        "snapshot": {
            "id": snap.id,
            "created_at": snap.created_at.isoformat(),
            "applied_seq": snap.applied_seq,
            "format": snap.format,
        },
        "replay": [
            {"id": step.segment_id, "from_seq": step.from_seq, "to_seq": step.to_seq}
            for step in plan.steps
        ],
        "endpoint": plan.endpoint,
    }


def _job_payload(conn, job_row) -> dict:
    """Serialize a stored job (with its frozen plan) for API responses."""
    return {
        "id": job_row["id"],
        "at": job_row["at_utc"],
        "status": job_row["status"],
        "snapshot": {
            "id": job_row["snapshot_id"],
            "created_at": job_row["snapshot_created"],
            "applied_seq": job_row["snapshot_seq"],
            "format": job_row["snapshot_format"],
        },
        "replay": [
            {"id": row["segment_id"], "from_seq": row["from_seq"], "to_seq": row["to_seq"]}
            for row in db.load_job_steps(conn, job_row["id"])
        ],
        "endpoint": job_row["endpoint"],
    }


@app.post("/recovery-jobs", status_code=201)
def create_recovery_job(req: RecoveryJobIn) -> dict:
    """Start a recovery job for the given UTC moment.

    The best plan visible at ``at`` is computed and frozen into the job:
    chosen snapshot, per-segment replay intervals and recovery endpoint. The
    job starts in progress and holds (protects from cleanup) every record the
    frozen plan uses. With no visible snapshot there is no recovery point, so
    no job is created and ``404`` is returned. The plan lookup and the insert
    run in one write transaction, so every successful job references records
    that were present at commit time.
    """
    at_utc = to_utc(req.at)
    conn = db.connect_immediate()
    try:
        snapshots, segments = db.load_all(conn)
        plan = build_plan(snapshots, segments, at_utc)
        if plan is None:
            conn.rollback()
            raise HTTPException(
                status_code=404,
                detail=(
                    "no recovery point: no snapshot created at or before "
                    f"{at_utc.isoformat()}; job not created"
                ),
            )
        job_id = uuid4().hex
        db.insert_job(conn, job_id, at_utc, plan)
        conn.commit()
        job_row = db.load_job(conn, job_id)
        return _job_payload(conn, job_row)
    finally:
        conn.close()


@app.get("/recovery-jobs/{job_id}")
def get_recovery_job(job_id: str) -> dict:
    """Return a job's frozen plan and its current status."""
    conn = db.connect()
    try:
        job_row = db.load_job(conn, job_id)
        if job_row is None:
            raise HTTPException(status_code=404, detail=f"unknown job id: {job_id}")
        return _job_payload(conn, job_row)
    finally:
        conn.close()


@app.post("/recovery-jobs/{job_id}/end")
def end_recovery_job(job_id: str) -> dict:
    """Finish a recovery job, releasing only the records this job occupied.

    Shared records stay protected until every in-progress job using them has
    ended. Ending is idempotent: repeating it on an already finished job keeps
    it finished and returns the same representation. An unknown id yields
    ``404``.
    """
    conn = db.connect_immediate()
    try:
        job_row = db.load_job(conn, job_id)
        if job_row is None:
            conn.rollback()
            raise HTTPException(status_code=404, detail=f"unknown job id: {job_id}")
        if job_row["status"] == db.JOB_STATUS_IN_PROGRESS:
            db.finish_job(conn, job_id)
            conn.commit()
            job_row = db.load_job(conn, job_id)
        else:
            conn.rollback()
        return _job_payload(conn, job_row)
    finally:
        conn.close()


def _endpoint_rank(endpoint: Optional[int]) -> int:
    """Ordering used when comparing recovery endpoints.

    ``None`` (no recovery point) ranks below every non-negative sequence
    number.
    """
    return -1 if endpoint is None else endpoint


@app.post("/cleanups")
def cleanup_batch(cleanup: CleanupIn) -> dict:
    """Delete registered snapshots/log segments in one batch.

    Two independent gates guard a cleanup, both checked in one write
    transaction (``BEGIN IMMEDIATE``):

    1. Any requested id currently used by an *in-progress* recovery job (its
       frozen snapshot or any replayed segment) rejects the whole batch with
       ``409``, returning the conflicting record ids and the job ids holding
       them. An equivalent recovery chain does not help: the job keeps the
       exact records it was created with. Records shared by several jobs stay
       protected until every holder ends.
    2. For every protected UTC moment the best recovery endpoint is compared
       before the deletion and after deleting the whole requested batch, using
       the existing planning rules. The batch is deleted only when no moment's
       endpoint decreases (``None`` means no recovery point and ranks below
       every sequence number).

    A rejected cleanup deletes nothing.
    """
    conn = db.connect_immediate()
    try:
        existing = db.all_ids(conn)
        missing = sorted(set(cleanup.ids) - existing)
        if missing:
            conn.rollback()
            raise HTTPException(
                status_code=404,
                detail=f"id(s) not registered: {missing}; cleanup rejected",
            )

        ids_to_delete = set(cleanup.ids)

        usage = db.in_progress_usage(conn, ids_to_delete)
        if usage:
            conn.rollback()
            conflicts = [
                {"id": record_id, "job_ids": sorted(job_ids)}
                for record_id, job_ids in sorted(usage.items())
            ]
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        "record(s) are used by in-progress recovery job(s); "
                        "nothing deleted"
                    ),
                    "conflicts": conflicts,
                },
            )

        snapshots, segments = db.load_all(conn)
        remaining_snapshots = [s for s in snapshots if s.id not in ids_to_delete]
        remaining_segments = [s for s in segments if s.id not in ids_to_delete]

        before = []
        after = []
        for at in cleanup.protected_at:
            plan_before = build_plan(snapshots, segments, at)
            plan_after = build_plan(remaining_snapshots, remaining_segments, at)
            before.append(None if plan_before is None else plan_before.endpoint)
            after.append(None if plan_after is None else plan_after.endpoint)

        degraded = [
            {"at": at.isoformat(), "before": b, "after": a}
            for at, b, a in zip(cleanup.protected_at, before, after)
            if _endpoint_rank(a) < _endpoint_rank(b)
        ]
        if degraded:
            conn.rollback()
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "recovery endpoint would decrease at protected moment(s); nothing deleted",
                    "degraded_moments": degraded,
                },
            )

        db.delete_records(conn, cleanup.ids)
        conn.commit()
    finally:
        conn.close()

    return {
        "deleted_ids": list(cleanup.ids),
        "endpoints": [
            {"at": at.isoformat(), "endpoint": endpoint}
            for at, endpoint in zip(cleanup.protected_at, after)
        ],
    }
