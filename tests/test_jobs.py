"""End-to-end tests for recovery jobs and their interaction with cleanups."""

import os
import threading

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DB_PATH", ":memory:")  # placeholder, overridden per test

from app.main import app  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as c:
        c.db_path = str(tmp_path / "test.db")
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


def cleanup(client, ids, protected_at):
    return client.post(
        "/cleanups", json={"ids": list(ids), "protected_at": list(protected_at)}
    )


def create_job(client, at):
    return client.post("/recovery-jobs", json={"at": at})


def get_job(client, job_id):
    return client.get(f"/recovery-jobs/{job_id}")


def end_job(client, job_id):
    return client.post(f"/recovery-jobs/{job_id}/end")


def base_dataset(client):
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T00:00:00Z", 5),
            snap("s2", "2026-06-01T00:00:00Z", 3),
        ],
        segments=[
            seg("l1", "2026-01-01T00:00:00Z", 6, 9),
            seg("l2", "2026-02-01T00:00:00Z", 10, 12),
            seg("unused", "2026-01-01T00:00:00Z", 100, 110),
        ],
    )


# ------------------------------------------------------------- create / read


def test_create_job_freezes_best_plan(client):
    base_dataset(client)
    r = create_job(client, "2026-06-15T00:00:00Z")
    assert r.status_code == 201
    body = r.json()
    job_id = body.pop("id")
    assert len(job_id) >= 16
    assert body == {
        "at": "2026-06-15T00:00:00+00:00",
        "status": "in_progress",
        "snapshot": {
            "id": "s1",
            "created_at": "2026-01-01T00:00:00+00:00",
            "applied_seq": 5,
            "format": "wal-v1",
        },
        "replay": [
            {"id": "l1", "from_seq": 6, "to_seq": 9},
            {"id": "l2", "from_seq": 10, "to_seq": 12},
        ],
        "endpoint": 12,
    }


def test_create_job_normalizes_naive_and_offset_times(client):
    base_dataset(client)
    # 2026-01-01T02:00:00+02:00 == 2026-01-01T00:00:00Z -> s1 visible
    r = create_job(client, "2026-01-01T02:00:00+02:00")
    assert r.status_code == 201
    assert r.json()["at"] == "2026-01-01T00:00:00+00:00"
    # naive is treated as UTC: at 2020 no snapshot is visible
    assert create_job(client, "2020-01-01T00:00:00").status_code == 404


def test_create_job_without_recovery_point_returns_404_and_creates_nothing(client):
    base_dataset(client)
    r = create_job(client, "2020-01-01T00:00:00Z")
    assert r.status_code == 404
    assert "no recovery point" in r.json()["detail"]
    # a later successful create proves nothing weird was stored; the 404
    # response carries no job id and the unknown id reads back as 404
    ok = create_job(client, "2026-06-15T00:00:00Z")
    assert ok.status_code == 201
    assert get_job(client, "does-not-exist").status_code == 404


def test_create_job_invalid_payload(client):
    assert client.post("/recovery-jobs", json={"at": "not-a-time"}).status_code == 422
    assert client.post("/recovery-jobs", json={}).status_code == 422
    r = client.post(
        "/recovery-jobs",
        json={"at": "2026-06-15T00:00:00Z", "extra": 1},
    )
    assert r.status_code == 422


def test_get_unknown_job_returns_404(client):
    assert get_job(client, "ghost").status_code == 404
    assert end_job(client, "ghost").status_code == 404


def test_read_returns_same_frozen_plan(client):
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    body = get_job(client, job_id).json()
    assert body["status"] == "in_progress"
    assert body["snapshot"]["id"] == "s1"
    assert [step["id"] for step in body["replay"]] == ["l1", "l2"]
    assert body["endpoint"] == 12


# ------------------------------------------------------------------ ending


def test_end_job_finishes_and_is_idempotent(client):
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    r = end_job(client, job_id)
    assert r.status_code == 200
    assert r.json()["status"] == "finished"
    # the plan is still fully readable after ending
    body = get_job(client, job_id).json()
    assert body["status"] == "finished"
    assert body["snapshot"]["id"] == "s1"
    assert body["endpoint"] == 12
    # ending again keeps it finished and still returns the representation
    r2 = end_job(client, job_id)
    assert r2.status_code == 200
    assert r2.json()["status"] == "finished"
    assert get_job(client, job_id).json()["status"] == "finished"


def test_later_registration_does_not_change_stored_plan(client):
    register(
        client,
        snapshots=[snap("s1", "2026-01-01T00:00:00Z", 5)],
        segments=[seg("l1", "2026-01-01T00:00:00Z", 6, 9)],
    )
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    # a strictly better snapshot arrives later
    register(client, snapshots=[snap("s-new", "2026-07-01T00:00:00Z", 999)])
    body = get_job(client, job_id).json()
    assert body["snapshot"]["id"] == "s1"
    assert body["endpoint"] == 9
    # live planning does use the new snapshot; the frozen job does not
    r = client.get("/recovery-plan", params={"at": "2026-08-01T00:00:00Z"})
    assert r.json()["snapshot"]["id"] == "s-new"


# ---------------------------------------------------- cleanup vs. job usage


def test_cleanup_conflicts_with_any_record_used_by_in_progress_job(client):
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]  # s1, l1, l2
    r = cleanup(client, ["l2", "unused"], ["2026-06-15T00:00:00Z"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["conflicts"] == [{"id": "l2", "job_ids": [job_id]}]
    assert "nothing deleted" in detail["message"]
    # whole batch kept: the otherwise-deletable "unused" survived too
    r2 = cleanup(client, ["unused"], ["2020-01-01T00:00:00Z"])
    assert r2.status_code == 200


def test_cleanup_blocked_on_snapshot_even_with_equivalent_chain(client):
    # s1+l1+l2 and s2+l1+l2 both reach 180; s1 has the smaller id and wins the
    # tiebreak. After deleting s1 the endpoint stays 180 via s2 (equivalent
    # chain), but the in-progress job still holds s1 itself.
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T08:00:00Z", 100),
            snap("s2", "2026-01-01T12:00:00Z", 100),
        ],
        segments=[
            seg("l1", "2026-01-01T09:00:00Z", 101, 150),
            seg("l2", "2026-01-01T10:00:00Z", 140, 180),
        ],
    )
    job_id = create_job(client, "2026-09-01T00:00:00Z").json()["id"]
    body = get_job(client, job_id).json()
    assert body["snapshot"]["id"] == "s1"
    assert body["endpoint"] == 180
    # the plain endpoint rule alone would allow this (s2 still reaches 180)...
    r = cleanup(client, ["s1"], ["2026-09-01T00:00:00Z"])
    assert r.status_code == 409
    assert r.json()["detail"]["conflicts"] == [{"id": "s1", "job_ids": [job_id]}]
    # ...and nothing was deleted; s1 still drives the live plan
    assert client.get(
        "/recovery-plan", params={"at": "2026-09-01T00:00:00Z"}
    ).json()["snapshot"]["id"] == "s1"


def test_cleanup_allowed_once_job_ends_and_endpoint_rule_passes(client):
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    # l2 is needed for endpoint 12 -> blocked by the job while in progress
    assert cleanup(client, ["l2"], ["2026-06-15T00:00:00Z"]).status_code == 409
    end_job(client, job_id)
    # finished job no longer protects l2; the protected_at rule now governs and
    # deleting l2 would drop 12 -> 9, so still 409 via degraded moments
    r = cleanup(client, ["l2"], ["2026-06-15T00:00:00Z"])
    assert r.status_code == 409
    assert "degraded_moments" in r.json()["detail"]
    # an unused record deletable under protected_at goes through
    r = cleanup(client, ["unused"], ["2026-06-15T00:00:00Z"])
    assert r.status_code == 200


def test_unused_and_finished_records_still_follow_protected_at_rule(client):
    base_dataset(client)
    create_job(client, "2026-06-15T00:00:00Z")
    # "unused" is not part of any job; at 2020 there is no recovery point
    # before or after, so the protected_at rule permits it even while the job
    # is in progress
    r = cleanup(client, ["unused"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 200


def test_shared_record_protected_until_all_holders_end(client):
    base_dataset(client)
    j1 = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    j2 = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    r = cleanup(client, ["l1"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 409
    job_ids = r.json()["detail"]["conflicts"][0]["job_ids"]
    assert sorted(job_ids) == sorted([j1, j2])
    end_job(client, j1)
    # j2 still holds l1
    r = cleanup(client, ["l1"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 409
    assert r.json()["detail"]["conflicts"][0]["job_ids"] == [j2]
    end_job(client, j2)
    # both ended: the 2020 protected moment has null->null, deletion succeeds
    assert cleanup(client, ["l1"], ["2020-01-01T00:00:00Z"]).status_code == 200


def test_ending_releases_only_that_jobs_records(client):
    base_dataset(client)
    j1 = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    register(
        client,
        snapshots=[snap("s3", "2026-05-01T00:00:00Z", 70)],
        segments=[seg("l3", "2026-05-01T00:00:00Z", 71, 80)],
    )
    j2 = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    # j2 freezes s3 (endpoint 80) and thus uses s3/l3; j1 does not
    body = get_job(client, j2).json()
    assert body["snapshot"]["id"] == "s3"
    end_job(client, j1)
    # j1's records are releasable (protected_at permits at 2020)...
    assert cleanup(client, ["l1"], ["2020-01-01T00:00:00Z"]).status_code == 200
    # ...while j2's records stay protected
    r = cleanup(client, ["l3"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 409
    assert r.json()["detail"]["conflicts"] == [{"id": "l3", "job_ids": [j2]}]


def test_finished_job_remains_readable_after_its_records_are_deleted(client):
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    end_job(client, job_id)
    # delete everything once the job is finished (2020: null -> null)
    assert cleanup(
        client, ["s1", "s2", "l1", "l2", "unused"], ["2020-01-01T00:00:00Z"]
    ).status_code == 200
    body = get_job(client, job_id).json()
    assert body["status"] == "finished"
    assert body["snapshot"]["id"] == "s1"
    assert [step["id"] for step in body["replay"]] == ["l1", "l2"]
    assert body["endpoint"] == 12
    # and the records really are gone
    assert client.get(
        "/recovery-plan", params={"at": "2026-09-01T00:00:00Z"}
    ).status_code == 404


def test_cleanup_unknown_id_takes_precedence_over_job_conflict(client):
    base_dataset(client)
    create_job(client, "2026-06-15T00:00:00Z")
    r = cleanup(client, ["l1", "ghost"], ["2020-01-01T00:00:00Z"])
    assert r.status_code == 404
    assert "ghost" in r.json()["detail"]


# ----------------------------------------------------------- restart / sqlite


def test_jobs_survive_restart(client, tmp_path, monkeypatch):
    db_path = client.db_path
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    end_job(client, job_id)

    # simulate a process restart against the same SQLite file
    with TestClient(app) as restarted:
        body = restarted.get(f"/recovery-jobs/{job_id}").json()
        assert body["status"] == "finished"
        assert body["snapshot"]["id"] == "s1"
        assert body["replay"] == [
            {"id": "l1", "from_seq": 6, "to_seq": 9},
            {"id": "l2", "from_seq": 10, "to_seq": 12},
        ]
        assert body["endpoint"] == 12


def test_in_progress_job_remains_protective_after_restart(client):
    db_path = client.db_path
    base_dataset(client)
    job_id = create_job(client, "2026-06-15T00:00:00Z").json()["id"]
    with TestClient(app) as restarted:
        r = restarted.post(
            "/cleanups",
            json={"ids": ["l1"], "protected_at": ["2020-01-01T00:00:00Z"]},
        )
        assert r.status_code == 409
        assert r.json()["detail"]["conflicts"] == [{"id": "l1", "job_ids": [job_id]}]


# --------------------------------------------------------------- concurrency


def test_create_end_and_cleanup_concurrently_keep_invariants(client):
    """Stress run against one shared dataset: a successful in-progress job must
    never reference a deleted record, and ending one job must never release a
    record another in-progress job still holds. Unused records stay deletable
    under the ordinary protected_at rule while jobs are running."""
    register(
        client,
        snapshots=[
            snap("s1", "2026-01-01T00:00:00Z", 5),
            snap("s2", "2026-06-01T00:00:00Z", 50),
        ],
        segments=[
            seg("l1", "2026-01-01T00:00:00Z", 6, 9),
            seg("l2", "2026-02-01T00:00:00Z", 10, 40),
            seg("unused", "2026-01-01T00:00:00Z", 100, 110),  # gap, never used
        ],
    )

    n_creators = 6
    n_cleaners = 4
    barrier = threading.Barrier(n_creators + n_cleaners + 1)
    errors = []

    def creator():
        try:
            barrier.wait()
            for _ in range(8):
                r = create_job(client, "2026-09-01T00:00:00Z")
                if r.status_code == 404:
                    continue  # another thread deleted every snapshot
                assert r.status_code == 201, r.text
                body = r.json()
                used = [body["snapshot"]["id"]] + [
                    step["id"] for step in body["replay"]
                ]
                # while this job is in progress, its records cannot be deleted;
                # the only possible rejection is the job-usage gate
                held = cleanup(client, used, ["2020-01-01T00:00:00Z"])
                assert held.status_code == 409, held.text
                held_detail = held.json()["detail"]
                assert "conflicts" in held_detail, held_detail
                held_ids = {c["id"] for c in held_detail["conflicts"]}
                assert set(used) <= held_ids
                assert body["id"] in {
                    j for c in held_detail["conflicts"] for j in c["job_ids"]
                }
                end_job(client, body["id"])
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    def cleaner(idx):
        try:
            barrier.wait()
            for _ in range(8):
                # unused record is never part of any plan: still deletable under
                # protected_at even while jobs run (unique id per attempt)
                garbage = f"g{idx}-{_}"
                register(
                    client,
                    segments=[seg(garbage, "2026-01-01T00:00:00Z", 200 + _, 210 + _)],
                )
                r = cleanup(client, [garbage], ["2020-01-01T00:00:00Z"])
                assert r.status_code == 200, r.text
                # try to take the plan records; at 2020 the endpoint rule is
                # null->null, so 409 here can only mean live jobs hold them
                r = cleanup(client, ["s1", "l1", "l2"], ["2020-01-01T00:00:00Z"])
                # 200: all holders had ended; 409: live jobs hold them;
                # 404: another cleaner already deleted them
                assert r.status_code in (200, 404, 409), r.text
                if r.status_code == 409:
                    assert "conflicts" in r.json()["detail"]
                    for conflict in r.json()["detail"]["conflicts"]:
                        assert conflict["id"] in ("s1", "l1", "l2")
                        assert conflict["job_ids"]
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    threads = [threading.Thread(target=creator) for _ in range(n_creators)]
    threads += [threading.Thread(target=cleaner, args=(i,)) for i in range(n_cleaners)]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()
    assert not errors

    # Final invariant at the database level: every still-in-progress job
    # references only records that exist.
    import sqlite3

    conn = sqlite3.connect(client.db_path)
    conn.row_factory = sqlite3.Row
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT id FROM snapshots UNION SELECT id FROM log_segments"
        )
    }
    for row in conn.execute(
        """
        SELECT j.snapshot_id AS record_id
        FROM recovery_jobs j WHERE j.status = 'in_progress'
        UNION ALL
        SELECT s.segment_id
        FROM recovery_job_steps s
        JOIN recovery_jobs j ON j.id = s.job_id
        WHERE j.status = 'in_progress'
        """
    ):
        assert row["record_id"] in existing
    conn.close()


def test_concurrent_create_and_cleanup_race_keeps_newly_held_record(client):
    """Race a job creation against the cleanup of the exact records it uses.
    Whichever serial order wins, a successful create must always protect its
    snapshot and segments from deletion."""
    outcomes = set()
    for trial in range(20):
        sid, lid = f"s{trial}", f"l{trial}"
        register(
            client,
            snapshots=[snap(sid, "2026-01-01T00:00:00Z", 5)],
            segments=[seg(lid, "2026-02-01T00:00:00Z", 6, 40)],
        )
        result = {}
        start = threading.Event()

        def do_create():
            start.wait()
            result["create"] = create_job(client, "2026-09-01T00:00:00Z")

        def do_cleanup():
            start.wait()
            result["cleanup"] = cleanup(
                client, [sid, lid], ["2020-01-01T00:00:00Z"]
            )

        t1 = threading.Thread(target=do_create)
        t2 = threading.Thread(target=do_cleanup)
        t1.start()
        t2.start()
        start.set()
        t1.join()
        t2.join()

        c_status = result["create"].status_code
        x_status = result["cleanup"].status_code
        if c_status == 201:
            # a created in-progress job holds sid+lid, so cleanup had to lose
            assert x_status == 409
            held = {
                c["id"]
                for c in result["cleanup"].json()["detail"]["conflicts"]
            }
            assert {sid, lid} <= held
            outcomes.add("create_holds")
            end_job(client, result["create"].json()["id"])
            assert cleanup(
                client, [sid, lid], ["2020-01-01T00:00:00Z"]
            ).status_code == 200
        else:
            # cleanup committed first: no records remain, so no recovery point
            assert c_status == 404
            assert x_status == 200
            outcomes.add("cleanup_first")

    # real thread interleaving should let both serial orders occur; even if a
    # run only sees one, the assertions above hold for each observed ordering
    assert outcomes <= {"create_holds", "cleanup_first"} and outcomes
