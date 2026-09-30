"""FastAPI application: batch registration and recovery planning."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from . import db
from .models import BatchIn, CleanupIn, to_utc
from .planner import build_plan


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()  # initialize the SQLite schema on startup
    yield


app = FastAPI(
    title="Recovery Planner",
    version="1.0.0",
    description=(
        "Register database snapshots and log segments in batches, then ask for "
        "the optimal recovery plan (snapshot + continuous same-format log "
        "replay) at a given UTC time."
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


def _endpoint_rank(endpoint: Optional[int]) -> int:
    """Ordering used when comparing recovery endpoints.

    ``None`` (no recovery point) ranks below every non-negative sequence
    number.
    """
    return -1 if endpoint is None else endpoint


@app.post("/cleanups")
def cleanup_batch(cleanup: CleanupIn) -> dict:
    """Delete registered snapshots/log segments in one batch.

    For every protected UTC moment the best recovery endpoint is compared
    before the deletion and after deleting the whole requested batch, using
    the existing planning rules. The batch is deleted only when no moment's
    endpoint decreases (``None`` means no recovery point and ranks below
    every sequence number); neither the original snapshot nor the original
    replay chain has to be preserved. The check and the deletion run in one
    database transaction, so a rejected cleanup leaves nothing deleted.
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
