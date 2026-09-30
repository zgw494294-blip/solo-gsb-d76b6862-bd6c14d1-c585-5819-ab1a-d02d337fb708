"""End-to-end tests for the recovery planning API."""

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DB_PATH", ":memory:")  # placeholder, overridden per test

from app.main import app  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def snap(id, created_at, applied_seq, fmt="wal-v1"):
    return {
        "id": id,
        "created_at": created_at,
        "applied_seq": applied_seq,
        "format": fmt,
    }


def seg(id, created_at, first, last, fmt="wal-v1"):
    return {
        "id": id,
        "created_at": created_at,
        "format": fmt,
        "first_seq": first,
        "last_seq": last,
    }


def register(client, snapshots=(), segments=()):
    return client.post(
        "/batches", json={"snapshots": list(snapshots), "log_segments": list(segments)}
    )


def plan(client, at):
    return client.get("/recovery-plan", params={"at": at})


def cleanup(client, ids, protected_at):
    return client.post(
        "/cleanups", json={"ids": list(ids), "protected_at": list(protected_at)}
    )


# ---------------------------------------------------------------- batches


def test_register_batch_ok(client):
    r = register(
        client,
        snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)],
        segments=[seg("l1", "2026-01-01T00:00:00Z", 6, 9)],
    )
    assert r.status_code == 201
    assert r.json() == {"stored_snapshots": 1, "stored_log_segments": 1}


def test_duplicate_ids_within_batch_rejected(client):
    r = register(
        client,
        snapshots=[snap("x", "2026-01-01T00:00:00Z", 1)],
        segments=[seg("x", "2026-01-01T00:00:00Z", 2, 3)],
    )
    assert r.status_code == 422
    # nothing was stored: the same ids can be registered afterwards
    assert register(client, snapshots=[snap("x", "2026-01-01T00:00:00Z", 1)]).status_code == 201


def test_duplicate_id_against_database_rejected(client):
    assert register(client, snapshots=[snap("s1", "2026-01-01T00:00:00Z", 1)]).status_code == 201
    r = register(
        client,
        snapshots=[snap("s2", "2026-01-01T00:00:00Z", 2)],
        segments=[seg("s1", "2026-01-01T00:00:00Z", 2, 3)],
    )
    assert r.status_code == 409
    # whole batch rolled back: s2 was not stored either
    assert register(client, snapshots=[snap("s2", "2026-01-01T00:00:00Z", 2)]).status_code == 201


def test_batch_larger_than_100_rejected(client):
    segments = [seg(f"l{i}", "2026-01-01T00:00:00Z", i, i) for i in range(1, 102)]
    r = register(client, segments=segments)
    assert r.status_code == 422
    # exactly 100 items is fine
    assert register(client, segments=segments[:100]).status_code == 201


@pytest.mark.parametrize(
    "snapshot",
    [
        snap("s", "2026-01-01T00:00:00Z", -1),          # negative applied_seq
        snap("s", "2026-01-01T00:00:00Z", True),        # bool is not an int
        snap("s", "2026-01-01T00:00:00Z", 1.5),         # float is not an int
        snap("s", "2026-01-01T00:00:00Z", "5"),         # string is not an int
        snap("s", "not-a-time", 1),                     # bad timestamp
        snap("", "2026-01-01T00:00:00Z", 1),            # empty id
        snap("s", "2026-01-01T00:00:00Z", 1, fmt=""),   # empty format
    ],
)
def test_invalid_snapshot_fields_rejected(client, snapshot):
    assert register(client, snapshots=[snapshot]).status_code == 422


@pytest.mark.parametrize(
    "segment",
    [
        seg("l", "2026-01-01T00:00:00Z", 0, 3),    # first_seq must be positive
        seg("l", "2026-01-01T00:00:00Z", 5, 3),    # last_seq < first_seq
        seg("l", "2026-01-01T00:00:00Z", 1, True),
        seg("l", "2026-01-01T00:00:00Z", 1, "9"),
    ],
)
def test_invalid_segment_fields_rejected(client, segment):
    assert register(client, segments=[segment]).status_code == 422


def test_unknown_field_rejected(client):
    bad = snap("s", "2026-01-01T00:00:00Z", 1)
    bad["unexpected"] = 1
    assert register(client, snapshots=[bad]).status_code == 422


# ------------------------------------------------------------ recovery plan


def test_plan_no_snapshot_returns_404(client):
    r = plan(client, "2026-01-01T00:00:00Z")
    assert r.status_code == 404
    assert "no recovery point" in r.json()["detail"]


def test_plan_overlap_gap_and_format_filter(client):
    register(
        client,
        snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)],
        segments=[
            seg("l1", "2026-01-01T00:00:00Z", 6, 9),    # replay 6..9
            seg("l2", "2026-01-01T00:00:00Z", 8, 12),   # overlap: replay 10..12 only
            seg("l3", "2026-01-01T00:00:00Z", 14, 15),  # gap at 13 -> chain stops
            seg("l4", "2026-01-01T00:00:00Z", 6, 99, fmt="wal-v2"),  # wrong format
        ],
    )
    r = plan(client, "2026-06-01T00:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert body["snapshot"]["id"] == "s1"
    assert body["replay"] == [
        {"id": "l1", "from_seq": 6, "to_seq": 9},
        {"id": "l2", "from_seq": 10, "to_seq": 12},
    ]
    assert body["endpoint"] == 12


def test_plan_without_segments_ends_at_applied_seq(client):
    register(client, snapshots=[snap("s1", "2026-01-01T00:00:00Z", 42)])
    body = plan(client, "2026-06-01T00:00:00Z").json()
    assert body["replay"] == []
    assert body["endpoint"] == 42


def test_visibility_cutoff_by_query_time(client):
    register(
        client,
        snapshots=[
            snap("old", "2026-01-01T00:00:00Z", 3),
            snap("new", "2026-03-01T00:00:00Z", 100),
        ],
        segments=[seg("l1", "2026-02-01T00:00:00Z", 4, 10)],
    )
    # before the newer snapshot and the segment exist
    body = plan(client, "2026-01-15T00:00:00Z").json()
    assert body["snapshot"]["id"] == "old"
    assert body["replay"] == []
    assert body["endpoint"] == 3
    # after everything: segment extends the old snapshot, but the newer
    # snapshot alone reaches further
    body = plan(client, "2026-04-01T00:00:00Z").json()
    assert body["snapshot"]["id"] == "new"
    assert body["endpoint"] == 100


def test_tiebreak_prefers_higher_applied_seq_then_smaller_id(client):
    register(
        client,
        snapshots=[
            snap("b", "2026-01-01T00:00:00Z", 10),
            snap("a", "2026-01-01T00:00:00Z", 10),  # same endpoint, smaller id wins
            snap("c", "2026-01-01T00:00:00Z", 8),
        ],
        segments=[seg("l1", "2026-01-01T00:00:00Z", 9, 20)],
    )
    # all snapshots can reach endpoint 20 (c: 9..20, a/b: 11..20);
    # higher applied_seq wins, then lexicographically smaller id
    body = plan(client, "2026-06-01T00:00:00Z").json()
    assert body["snapshot"]["id"] == "a"
    assert body["replay"] == [{"id": "l1", "from_seq": 11, "to_seq": 20}]
    assert body["endpoint"] == 20


def test_chain_tiebreak_fewest_segments_then_lexicographic(client):
    register(
        client,
        snapshots=[snap("s", "2026-01-01T00:00:00Z", 0)],
        segments=[
            seg("b", "2026-01-01T00:00:00Z", 1, 4),   # reaches further in one hop
            seg("a", "2026-01-01T00:00:00Z", 1, 3),   # ...but "a" sorts first
            seg("c", "2026-01-01T00:00:00Z", 3, 10),
        ],
    )
    # both [b, c] and [a, c] reach endpoint 10 in 2 segments;
    # the lexicographically smaller id sequence ["a", "c"] must win
    body = plan(client, "2026-06-01T00:00:00Z").json()
    assert [step["id"] for step in body["replay"]] == ["a", "c"]
    assert body["replay"] == [
        {"id": "a", "from_seq": 1, "to_seq": 3},
        {"id": "c", "from_seq": 4, "to_seq": 10},
    ]
    assert body["endpoint"] == 10


def test_chain_prefers_fewer_segments_over_lexicographic(client):
    register(
        client,
        snapshots=[snap("s", "2026-01-01T00:00:00Z", 0)],
        segments=[
            seg("z", "2026-01-01T00:00:00Z", 1, 10),  # one segment suffices
            seg("a", "2026-01-01T00:00:00Z", 1, 5),
            seg("b", "2026-01-01T00:00:00Z", 6, 10),
        ],
    )
    body = plan(client, "2026-06-01T00:00:00Z").json()
    assert [step["id"] for step in body["replay"]] == ["z"]
    assert body["endpoint"] == 10


def test_chain_skips_fully_covered_segments(client):
    register(
        client,
        snapshots=[snap("s", "2026-01-01T00:00:00Z", 0)],
        segments=[
            seg("wide", "2026-01-01T00:00:00Z", 1, 10),
            seg("inner", "2026-01-01T00:00:00Z", 3, 5),  # entirely inside "wide"
        ],
    )
    body = plan(client, "2026-06-01T00:00:00Z").json()
    assert [step["id"] for step in body["replay"]] == ["wide"]
    assert body["endpoint"] == 10


def test_timezone_offsets_and_naive_timestamps(client):
    register(
        client,
        # 2026-01-01T02:00:00+02:00 == 2026-01-01T00:00:00Z
        snapshots=[snap("s", "2026-01-01T02:00:00+02:00", 7)],
        segments=[seg("l", "2026-01-01T00:00:00Z", 8, 8)],
    )
    body = plan(client, "2026-01-01T00:00:00Z").json()  # naive -> treated as UTC
    assert body["endpoint"] == 8
    assert body["replay"] == [{"id": "l", "from_seq": 8, "to_seq": 8}]


def test_snapshot_created_after_query_time_invisible(client):
    register(client, snapshots=[snap("s", "2026-05-01T00:00:00Z", 1)])
    assert plan(client, "2026-04-01T00:00:00Z").status_code == 404
    assert plan(client, "2026-05-01T00:00:00Z").status_code == 200  # inclusive


def test_invalid_query_time_rejected(client):
    assert plan(client, "not-a-time").status_code == 422


# ---------------------------------------------------------------- cleanups


def test_cleanup_deletes_unused_items_and_returns_endpoints(client):
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T00:00:00Z", 5),
            snap("s2", "2026-06-01T00:00:00Z", 3),
        ],
        segments=[
            seg("l1", "2026-01-01T00:00:00Z", 6, 9),
            seg("unused", "2026-01-01T00:00:00Z", 100, 110),  # gap, never replayed
        ],
    )
    r = cleanup(
        client,
        ["unused", "s2"],
        ["2026-06-15T00:00:00Z", "2020-01-01T00:00:00Z"],
    )
    assert r.status_code == 200
    assert r.json() == {
        "deleted_ids": ["unused", "s2"],
        "endpoints": [
            {"at": "2026-06-15T00:00:00+00:00", "endpoint": 9},
            {"at": "2020-01-01T00:00:00+00:00", "endpoint": None},
        ],
    }
    # the records are really gone
    assert cleanup(client, ["s2"], ["2026-06-15T00:00:00Z"]).status_code == 404


def test_cleanup_allows_alternative_snapshot_and_chain(client):
    # deleting the winning snapshot and its chain is fine as long as another
    # visible combination reaches the same endpoint; the replacement may use a
    # different snapshot and a different replay chain
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T08:00:00Z", 100),
            snap("s2", "2026-01-01T12:00:00Z", 100),  # tie on endpoint, larger id loses
        ],
        segments=[
            seg("l1", "2026-01-01T09:00:00Z", 101, 150),
            seg("l2", "2026-01-01T10:00:00Z", 140, 180),
        ],
    )
    body = plan(client, "2026-09-01T00:00:00Z").json()
    assert body["snapshot"]["id"] == "s1"
    assert body["endpoint"] == 180
    # after dropping s1, s2 replays l1+l2 and still reaches 180
    r = cleanup(client, ["s1"], ["2026-09-01T00:00:00Z"])
    assert r.status_code == 200
    body = plan(client, "2026-09-01T00:00:00Z").json()
    assert body["snapshot"]["id"] == "s2"
    assert body["replay"] == [
        {"id": "l1", "from_seq": 101, "to_seq": 150},
        {"id": "l2", "from_seq": 151, "to_seq": 180},
    ]
    assert body["endpoint"] == 180


def test_cleanup_rejected_when_endpoint_decreases(client):
    # s1 + l1 + l2 reaches endpoint 60; s2 alone reaches 50. Removing l2
    # drops the best endpoint from 60 to 50.
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T00:00:00Z", 5),
            snap("s2", "2026-06-01T00:00:00Z", 50),
        ],
        segments=[
            seg("l1", "2026-01-01T00:00:00Z", 6, 9),
            seg("l2", "2026-02-01T00:00:00Z", 10, 60),
        ],
    )
    r = cleanup(client, ["l2"], ["2026-09-01T00:00:00Z"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["degraded_moments"] == [
        {"at": "2026-09-01T00:00:00+00:00", "before": 60, "after": 50}
    ]
    # nothing was deleted: l2 still drives the plan
    assert plan(client, "2026-09-01T00:00:00Z").json()["endpoint"] == 60


def test_cleanup_rejected_when_endpoint_becomes_null(client):
    register(client, snapshots=[snap("only", "2026-01-01T00:00:00Z", 7)])
    r = cleanup(client, ["only"], ["2026-09-01T00:00:00Z"])
    assert r.status_code == 409
    assert r.json()["detail"]["degraded_moments"] == [
        {"at": "2026-09-01T00:00:00+00:00", "before": 7, "after": None}
    ]
    assert plan(client, "2026-09-01T00:00:00Z").status_code == 200


def test_cleanup_null_stays_null_is_allowed(client):
    register(client, snapshots=[snap("s1", "2026-06-01T00:00:00Z", 7)])
    # at 2020 there is no recovery point before or after: deletion is allowed
    r = cleanup(client, ["s1"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 200
    assert r.json()["endpoints"] == [
        {"at": "2020-01-01T00:00:00+00:00", "endpoint": None}
    ]
    assert plan(client, "2026-09-01T00:00:00Z").status_code == 404


def test_cleanup_reports_all_degraded_moments(client):
    register(client, snapshots=[snap("s1", "2026-03-01T00:00:00Z", 7)])
    r = cleanup(
        client,
        ["s1"],
        ["2026-01-01T00:00:00Z", "2026-05-01T00:00:00Z", "2026-09-01T00:00:00Z"],
    )
    assert r.status_code == 409
    moments = r.json()["detail"]["degraded_moments"]
    # the first moment was null before and after; the other two drop 7 -> null
    assert moments == [
        {"at": "2026-05-01T00:00:00+00:00", "before": 7, "after": None},
        {"at": "2026-09-01T00:00:00+00:00", "before": 7, "after": None},
    ]


def test_cleanup_mixed_snapshot_and_segment_ids(client):
    register(
        client,
        snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)],
        segments=[seg("l1", "2026-01-01T00:00:00Z", 6, 9)],
    )
    r = cleanup(client, ["s1", "l1"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 200
    assert sorted(r.json()["deleted_ids"]) == ["l1", "s1"]
    assert plan(client, "2026-09-01T00:00:00Z").status_code == 404


def test_cleanup_unknown_id_rejects_whole_batch(client):
    register(client, snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)])
    r = cleanup(client, ["s1", "ghost"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 404
    assert "ghost" in r.json()["detail"]
    # s1 must not have been deleted even though its removal alone was safe
    assert plan(client, "2026-09-01T00:00:00Z").json()["endpoint"] == 5


def test_cleanup_duplicate_ids_rejected(client):
    register(client, snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)])
    r = cleanup(client, ["s1", "s1"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 422
    assert plan(client, "2026-09-01T00:00:00Z").json()["endpoint"] == 5


@pytest.mark.parametrize(
    "payload",
    [
        {"ids": [], "protected_at": ["2026-01-01T00:00:00Z"]},               # no ids
        {"ids": [f"x{i}" for i in range(101)],
         "protected_at": ["2026-01-01T00:00:00Z"]},                          # >100 ids
        {"ids": ["x"], "protected_at": []},                                  # no moments
        {"ids": ["x"], "protected_at": ["2026-01-01T00:00:00Z"] * 9},        # >8 moments
        {"ids": [""], "protected_at": ["2026-01-01T00:00:00Z"]},             # empty id
        {"ids": [1], "protected_at": ["2026-01-01T00:00:00Z"]},              # non-string id
        {"ids": ["x"], "protected_at": ["not-a-time"]},                      # bad moment
        {"ids": ["x"], "protected_at": ["2026-01-01T00:00:00Z"], "extra": 1},
    ],
)
def test_cleanup_invalid_payload_rejected(client, payload):
    assert client.post("/cleanups", json=payload).status_code == 422


def test_cleanup_normalizes_timezone_offsets_and_naive_times(client):
    register(
        client,
        # 2026-03-01T02:00:00+02:00 == 2026-03-01T00:00:00Z
        snapshots=[snap("s1", "2026-03-01T02:00:00+02:00", 7)],
    )
    # naive moment (treated as UTC) at exactly the snapshot's UTC time:
    # deleting would turn endpoint 7 into null -> rejected
    r = cleanup(client, ["s1"], ["2026-03-01T00:00:00"])
    assert r.status_code == 409
    assert r.json()["detail"]["degraded_moments"][0]["before"] == 7
    # with an offset one hour earlier, the snapshot is invisible: null->null
    r = cleanup(client, ["s1"], ["2026-02-28T23:00:00Z"])
    assert r.status_code == 200
