"""Commit timing: one table set, three commit scopes."""

from __future__ import annotations

import sqlite3

import pytest

from caravel import DurabilityOptions, InMemorySource, Pipeline, Stage, Step, run
from caravel.durability.store import RecoveryStore
from caravel.durability.structure import declare

SCHEMA_VERSION = "v1.0.0"


def passthrough(value, *, context):
    return value


def pipeline_for(scope, *, job_id="job-1", stages=None, source=None):
    return Pipeline(
        name="commit_example",
        stages=stages
        or [
            Stage(
                name="alpha", entries=[Step(passthrough, name="a1"), Step(passthrough, name="a2")]
            ),
            Stage(name="beta", entries=[Step(passthrough, name="b1")]),
        ],
        source=source or InMemorySource({"a": 1}),
        durability_options=DurabilityOptions(
            schema_version=SCHEMA_VERSION, job_id=job_id, commit_scope=scope
        ),
    )


def database(tmp_path):
    return tmp_path / ".caravel" / "state.sqlite3"


def durable_rows(tmp_path):
    """Read through a fresh connection, so only committed work is visible."""
    connection = sqlite3.connect(database(tmp_path))
    try:
        steps = connection.execute(
            "SELECT step_name, status FROM steps ORDER BY stage_id, step_order"
        ).fetchall()
        stages = connection.execute(
            "SELECT stage_id, status FROM stages ORDER BY stage_order"
        ).fetchall()
        runs = connection.execute("SELECT job_id, status FROM runs").fetchall()
    finally:
        connection.close()
    return steps, stages, runs


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_every_boundary_is_durable_before_a_successful_return(tmp_path, scope):
    run(pipeline_for(scope), output=tmp_path / "output")
    steps, stages, runs = durable_rows(tmp_path)

    assert [row[1] for row in steps] == ["completed"] * 3
    assert [row[1] for row in stages] == ["completed", "completed"]
    assert runs == [("job-1", "completed")]


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_every_scope_uses_the_same_tables_and_encoding(tmp_path, scope):
    run(pipeline_for(scope), output=tmp_path / "output")
    connection = sqlite3.connect(database(tmp_path))
    try:
        tables = sorted(
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            if not row[0].startswith("sqlite_")
        )
        identities = {
            row[0] for row in connection.execute("SELECT DISTINCT codec_identity FROM steps")
        }
    finally:
        connection.close()

    assert tables == ["durability_metadata", "runs", "stages", "steps"]
    assert identities == {"json/utf-8"}


def test_a_commit_request_with_nothing_pending_performs_no_commit(tmp_path):
    store = RecoveryStore(database(tmp_path))
    try:
        structure = declare(pipeline_for("step"))
        store.begin()
        store.open_run(
            job_id="job-1",
            pipeline_id="p",
            pipeline_name="p",
            commit_scope="step",
            structure=structure,
            now="t0",
        )
        assert store.has_pending is True
        assert store.commit() is True
        assert store.has_pending is False
        assert store.commit() is False
    finally:
        store.close()


def test_a_status_change_after_a_commit_is_persisted_at_the_next_boundary(tmp_path):
    store = RecoveryStore(database(tmp_path))
    try:
        structure = declare(pipeline_for("step"))
        store.begin()
        store.open_run(
            job_id="job-1",
            pipeline_id="p",
            pipeline_name="p",
            commit_scope="step",
            structure=structure,
            now="t0",
        )
        store.commit()

        store.complete_run(job_id="job-1", now="t1")
        assert store.has_pending is True
        assert store.commit() is True

        fresh = sqlite3.connect(database(tmp_path))
        status = fresh.execute("SELECT status FROM runs WHERE job_id='job-1'").fetchone()[0]
        fresh.close()
        assert status == "completed"
    finally:
        store.close()


def test_pending_tracking_clears_after_a_rollback(tmp_path):
    store = RecoveryStore(database(tmp_path))
    try:
        structure = declare(pipeline_for("step"))
        store.begin()
        store.open_run(
            job_id="job-1",
            pipeline_id="p",
            pipeline_name="p",
            commit_scope="step",
            structure=structure,
            now="t0",
        )
        store.rollback()
        assert store.has_pending is False
        assert store.commit() is False
    finally:
        store.close()


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_a_single_step_pipeline_completes_under_every_scope(tmp_path, scope):
    pipeline = pipeline_for(
        scope, stages=[Stage(name="only", entries=[Step(passthrough, name="only_step")])]
    )
    run(pipeline, output=tmp_path / "output")
    steps, stages, runs = durable_rows(tmp_path)
    assert steps == [("only_step", "completed")]
    assert stages == [("stage-001", "completed")]
    assert runs == [("job-1", "completed")]


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_a_resume_restores_every_committed_step(tmp_path, scope):
    output = tmp_path / "output"
    calls = []

    def counted(value, *, context):
        calls.append("x")
        return value

    stages = [
        Stage(name="alpha", entries=[Step(counted, name="a1")]),
        Stage(name="beta", entries=[Step(counted, name="b1")]),
    ]
    run(pipeline_for(scope, stages=stages), output=output)
    assert len(calls) == 2

    calls.clear()
    result = run(pipeline_for(scope, stages=stages), output=output)
    assert calls == []
    assert result.steps_restored == 2
    assert result.steps_executed == 0
