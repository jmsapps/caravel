"""Recovery: what survives a failure, and what runs again."""

from __future__ import annotations

import sqlite3

import pytest

from caravel import (
    DurabilityOptions,
    ExecutorBusyError,
    InMemorySource,
    Pipeline,
    RetryOptions,
    Stage,
    Step,
    run,
)
from caravel.durability.ownership import ExecutorOwnership

SCHEMA_VERSION = "v1.0.0"

NO_DELAY = RetryOptions(max_attempts=3, delay_seconds=lambda attempt: 0.0)


def passthrough(value, *, context):
    return value


def options(scope, job_id="job-1"):
    return DurabilityOptions(schema_version=SCHEMA_VERSION, job_id=job_id, commit_scope=scope)


def database(tmp_path):
    return tmp_path / ".caravel" / "state.sqlite3"


def committed_steps(tmp_path):
    connection = sqlite3.connect(database(tmp_path))
    try:
        return {row[0]: row[1] for row in connection.execute("SELECT step_name, status FROM steps")}
    finally:
        connection.close()


def two_stage_pipeline(scope, *, failing, calls, job_id="job-1"):
    def a1(value, *, context):
        calls.append("a1")
        return value

    def b1(value, *, context):
        calls.append("b1")
        if failing():
            raise RuntimeError("b1 failed")
        return value

    return Pipeline(
        name="crash_example",
        stages=[
            Stage(name="alpha", entries=[Step(a1, name="a1")]),
            Stage(name="beta", entries=[Step(b1, name="b1")]),
        ],
        source=InMemorySource({"a": 1}),
        durability_options=options(scope, job_id),
    )


@pytest.mark.parametrize(
    "scope, survives",
    [("pipeline", False), ("stage", True), ("step", True)],
)
def test_a_failure_in_the_second_stage(tmp_path, scope, survives):
    output = tmp_path / "output"
    calls = []
    failing = [True]

    with pytest.raises(RuntimeError, match="b1 failed"):
        run(two_stage_pipeline(scope, failing=lambda: failing[0], calls=calls), output=output)

    durable = committed_steps(tmp_path)
    if survives:
        assert durable["a1"] == "completed"
    else:
        assert durable.get("a1", "pending") == "pending"

    calls.clear()
    failing[0] = False
    result = run(two_stage_pipeline(scope, failing=lambda: failing[0], calls=calls), output=output)
    assert "b1" in calls
    if survives:
        assert "a1" not in calls
        assert result.steps_restored == 1
    else:
        assert "a1" in calls


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_a_resumed_job_completes(tmp_path, scope):
    output = tmp_path / "output"
    calls = []
    failing = [True]

    with pytest.raises(RuntimeError):
        run(two_stage_pipeline(scope, failing=lambda: failing[0], calls=calls), output=output)

    failing[0] = False
    run(two_stage_pipeline(scope, failing=lambda: failing[0], calls=calls), output=output)

    connection = sqlite3.connect(database(tmp_path))
    status = connection.execute("SELECT status FROM runs WHERE job_id='job-1'").fetchone()[0]
    connection.close()
    assert status == "completed"


def test_a_step_retry_runs_in_memory_and_commits_its_accepted_output(tmp_path):
    attempts = []

    def flaky(value, *, context):
        attempts.append(len(attempts) + 1)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return {"attempts": len(attempts)}

    pipeline = Pipeline(
        name="retry_example",
        stages=[Stage(name="only", entries=[Step(flaky, name="flaky", retry=NO_DELAY)])],
        source=InMemorySource({"a": 1}),
        durability_options=options("step"),
    )
    run(pipeline, output=tmp_path / "output")

    assert len(attempts) == 2
    connection = sqlite3.connect(database(tmp_path))
    snapshot, status = connection.execute(
        "SELECT snapshot, status FROM steps WHERE step_name='flaky'"
    ).fetchone()
    connection.close()
    assert status == "completed"
    assert b'"attempts": 2' in snapshot


def test_a_stage_retry_restarts_its_steps(tmp_path):
    calls = []
    failing = [True]

    def first(value, *, context):
        calls.append("first")
        return value

    def second(value, *, context):
        calls.append("second")
        if failing[0]:
            failing[0] = False
            raise ConnectionError("second failed")
        return value

    pipeline = Pipeline(
        name="stage_retry",
        stages=[
            Stage(
                name="only",
                entries=[Step(first, name="first"), Step(second, name="second")],
                retry=NO_DELAY,
            )
        ],
        source=InMemorySource({"a": 1}),
        durability_options=options("pipeline"),
    )
    run(pipeline, output=tmp_path / "output")

    assert calls == ["first", "second", "first", "second"]
    assert committed_steps(tmp_path) == {"first": "completed", "second": "completed"}


def test_a_stage_retry_reuses_steps_committed_under_step_scope(tmp_path):
    calls = []
    failing = [True]

    def first(value, *, context):
        calls.append("first")
        return value

    def second(value, *, context):
        calls.append("second")
        if failing[0]:
            failing[0] = False
            raise ConnectionError("second failed")
        return value

    pipeline = Pipeline(
        name="stage_retry_step_scope",
        stages=[
            Stage(
                name="only",
                entries=[Step(first, name="first"), Step(second, name="second")],
                retry=NO_DELAY,
            )
        ],
        source=InMemorySource({"a": 1}),
        durability_options=options("step"),
    )
    run(pipeline, output=tmp_path / "output")

    assert calls == ["first", "second", "second"]


def test_each_attempt_receives_the_original_input(tmp_path):
    seen = []
    attempts = []

    def mutating(value, *, context):
        attempts.append(1)
        seen.append(dict(value["nested"]))
        value["nested"]["added"] = len(attempts)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return {"attempts": len(attempts)}

    pipeline = Pipeline(
        name="isolation",
        stages=[Stage(name="only", entries=[Step(mutating, name="mutating", retry=NO_DELAY)])],
        source=InMemorySource({"nested": {"value": 1}}),
        durability_options=options("step"),
    )
    run(pipeline, output=tmp_path / "output")

    assert seen == [{"value": 1}, {"value": 1}]


def test_output_deleted_from_disk_is_regenerated_from_snapshots(tmp_path):
    output = tmp_path / "output"
    calls = []

    def produce(value, *, context):
        calls.append("produce")
        return {"n": 1}

    pipeline = Pipeline(
        name="regenerate",
        stages=[Stage(name="only", entries=[Step(produce, name="produce")])],
        source=InMemorySource({"a": 1}),
        durability_options=options("step"),
    )
    run(pipeline, output=output)
    written = sorted(output.rglob("*.json"))
    assert written

    for path in written:
        path.unlink()

    result = run(pipeline, output=output)
    assert calls == ["produce"]
    assert result.steps_restored == 1
    assert sorted(output.rglob("*.json")) == written


def test_one_executor_owns_a_recovery_database(tmp_path):
    reserved = tmp_path / ".caravel"
    reserved.mkdir(parents=True, exist_ok=True)
    first = ExecutorOwnership(reserved)
    first.acquire(database=reserved / "state.sqlite3")
    try:
        second = ExecutorOwnership(reserved)
        with pytest.raises(ExecutorBusyError, match="state.sqlite3"):
            second.acquire(database=reserved / "state.sqlite3")
    finally:
        first.release()


def test_ownership_is_available_again_after_release(tmp_path):
    reserved = tmp_path / ".caravel"
    reserved.mkdir(parents=True, exist_ok=True)
    database_path = reserved / "state.sqlite3"

    first = ExecutorOwnership(reserved)
    first.acquire(database=database_path)
    first.release()

    second = ExecutorOwnership(reserved)
    second.acquire(database=database_path)
    assert second.held is True
    second.release()


def test_a_run_without_durability_options_executes_its_processors(tmp_path):
    output = tmp_path / "output"
    calls = []

    def produce(value, *, context):
        calls.append("produce")
        return {"n": 1}

    pipeline = Pipeline(
        name="in_memory",
        stages=[Stage(name="only", entries=[Step(produce, name="produce")])],
        source=InMemorySource({"a": 1}),
    )
    result = run(pipeline, output=output)

    assert calls == ["produce"]
    assert result.job_id is None
    assert sorted(path.name for path in output.rglob("*.json")) == ["_001_produce.json"]

    again = run(pipeline, output=output)
    assert calls == ["produce", "produce"]
    assert again.steps_restored == 0
