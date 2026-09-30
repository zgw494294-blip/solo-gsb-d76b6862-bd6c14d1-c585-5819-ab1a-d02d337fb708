"""Recovery planning: choose the optimal snapshot and log-segment chain.

Rules implemented here:

* Replay starts at ``snapshot.applied_seq + 1`` and only uses log segments
  whose ``format`` matches the snapshot's.
* A segment may be replayed when its interval covers the next unreplayed
  sequence number ``cur`` (``first_seq <= cur <= last_seq``). Replaying it
  covers ``[cur, last_seq]`` — the overlapping prefix is never replayed —
  and advances ``cur`` to ``last_seq + 1``. A gap (no segment covers
  ``cur``) stops the chain.
* The plan maximizes the recovery endpoint. Ties are broken by larger
  ``applied_seq``, then lexicographically smaller snapshot id, then fewer
  replayed segments, then the lexicographically smallest replayed id
  sequence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence, Tuple


@dataclass(frozen=True)
class Snapshot:
    id: str
    created_at: datetime
    applied_seq: int
    format: str


@dataclass(frozen=True)
class LogSegment:
    id: str
    created_at: datetime
    format: str
    first_seq: int
    last_seq: int


@dataclass(frozen=True)
class ReplayStep:
    """The interval actually replayed from one segment."""

    segment_id: str
    from_seq: int
    to_seq: int


@dataclass(frozen=True)
class RecoveryPlan:
    snapshot: Snapshot
    steps: Tuple[ReplayStep, ...]
    endpoint: int


def _chain_better(a: Tuple[int, Tuple[str, ...]], b: Tuple[int, Tuple[str, ...]]) -> bool:
    """Return True if chain ``a`` is strictly better than chain ``b``.

    Chains are ``(endpoint, ids)`` pairs: higher endpoint wins, then fewer
    segments, then the lexicographically smaller id sequence.
    """
    if a[0] != b[0]:
        return a[0] > b[0]
    if len(a[1]) != len(b[1]):
        return len(a[1]) < len(b[1])
    return a[1] < b[1]


def best_chain(
    start_seq: int, fmt: str, segments: Sequence[LogSegment]
) -> Tuple[int, Tuple[str, ...]]:
    """Best replay chain from ``start_seq`` using segments of format ``fmt``.

    Returns ``(endpoint, segment_ids)``. Because every replay strictly
    increases the next unreplayed sequence number, the states form a DAG;
    dynamic programming over the reachable "next needed" values (processed
    from high to low) yields the optimal chain under the tie-break rules.
    """
    usable = [s for s in segments if s.format == fmt]
    states = {start_seq}
    for seg in usable:
        states.add(seg.last_seq + 1)

    # best[cur] = (endpoint, ids) of the optimal chain starting at next-needed=cur
    best: dict[int, Tuple[int, Tuple[str, ...]]] = {}
    for cur in sorted(states, reverse=True):
        champion: Tuple[int, Tuple[str, ...]] = (cur - 1, ())  # stop here
        for seg in usable:
            if seg.first_seq <= cur <= seg.last_seq:
                nxt_endpoint, nxt_ids = best[seg.last_seq + 1]
                candidate = (nxt_endpoint, (seg.id,) + nxt_ids)
                if _chain_better(candidate, champion):
                    champion = candidate
        best[cur] = champion
    return best[start_seq]


def build_plan(
    snapshots: Sequence[Snapshot],
    segments: Sequence[LogSegment],
    at: datetime,
) -> Optional[RecoveryPlan]:
    """Build the optimal recovery plan for the query time ``at``.

    Only items created at or before ``at`` are visible. Returns ``None``
    when no snapshot is visible (no recovery point).
    """
    visible_snapshots = [s for s in snapshots if s.created_at <= at]
    if not visible_snapshots:
        return None
    visible_segments = [s for s in segments if s.created_at <= at]

    chosen: Optional[RecoveryPlan] = None
    chosen_key: Optional[tuple] = None
    for snap in visible_snapshots:
        endpoint, ids = best_chain(snap.applied_seq + 1, snap.format, visible_segments)
        # Lower key is better: maximize endpoint, then applied_seq, then
        # minimize snapshot id, segment count, and the id sequence.
        key = (-endpoint, -snap.applied_seq, snap.id, len(ids), ids)
        if chosen_key is None or key < chosen_key:
            chosen_key = key
            by_id = {s.id: s for s in visible_segments}
            steps = []
            cur = snap.applied_seq + 1
            for seg_id in ids:
                seg = by_id[seg_id]
                steps.append(ReplayStep(segment_id=seg_id, from_seq=cur, to_seq=seg.last_seq))
                cur = seg.last_seq + 1
            chosen = RecoveryPlan(snapshot=snap, steps=tuple(steps), endpoint=endpoint)
    return chosen
