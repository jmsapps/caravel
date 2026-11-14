"""The v1 recovery store: schema, version, structure, and snapshots."""

from __future__ import annotations

import sqlite3

import pytest

from caravel import (
    DurabilityOptions,
    InMemorySource,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    StructureMismatchError,
    TextDataset,
    UnsupportedStoreVersionError,
    run,
)
from caravel.durability.store import RecoveryStore
from caravel.durability.structure import declare

SCHEMA_VERSION = "v1.0.0"


def passthrough(value, *, context):
    return value


def options(job_id="job-1", commit_scope="pipeline"):
    return DurabilityOptions(
        schema_version=SCHEMA_VERSION, job_id=job_id, commit_scope=commit_scope
    )


def build(tmp_path, *, steps=None, job_id="job-1", commit_scope="pipeline", value=None):
    entries = steps if steps is not None else [Step(passthrough)]
    return Pipeline(
        name="store_example",
        stages=[Stage(name="only", entries=entries)],
        source=InMemorySource({"a": 1} if value is None else value),
        durability_options=options(job_id, commit_scope),
    )


def database(tmp_path):
    return tmp_path / ".caravel" / "state.sqlite3"


def test_store_creates_the_v1_schema(tmp_path):
    store = RecoveryStore(database(tmp_path))
    try:
        assert store.schema_version == SCHEMA_VERSION
        tables = {
            row[0]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"durability_metadata", "runs", "stages", "steps"} <= tables
    finally:
        store.close()


def test_store_reopens_at_its_recorded_version(tmp_path):
    path = database(tmp_path)
    RecoveryStore(path).close()
    store = RecoveryStore(path)
    try:
        assert store.schema_version == SCHEMA_VERSION
    finally:
        store.close()


def test_store_reports_the_version_a_database_states(tmp_path):
    path = database(tmp_path)
    RecoveryStore(path).close()
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE durability_metadata SET value = 'v2.0.0' WHERE key = 'schema_version'"
    )
    connection.commit()
    connection.close()

    with pytest.raises(UnsupportedStoreVersionError, match="v2.0.0"):
        RecoveryStore(path)


def test_declared_structure_is_stored_for_the_job(tmp_path):
    pipeline = build(
        tmp_path, steps=[Step(passthrough, name="first"), Step(passthrough, name="second")]
    )
    run(pipeline, output=tmp_path / "output")

    connection = sqlite3.connect(database(tmp_path))
    rows = connection.execute(
        "SELECT step_id, step_name, step_order, status FROM steps ORDER BY step_order"
    ).fetchall()
    connection.close()

    assert [row[1] for row in rows] == ["first", "second"]
    assert [row[2] for row in rows] == [1, 2]
    assert {row[3] for row in rows} == {"completed"}


def test_resume_matches_the_declared_structure(tmp_path):
    output = tmp_path / "output"
    run(build(tmp_path, steps=[Step(passthrough, name="first")]), output=output)

    widened = build(
        tmp_path, steps=[Step(passthrough, name="first"), Step(passthrough, name="second")]
    )
    with pytest.raises(StructureMismatchError, match="job-1"):
        run(widened, output=output)


def test_a_renamed_step_keeps_its_identity(tmp_path):
    output = tmp_path / "output"
    run(build(tmp_path, steps=[Step(passthrough, name="first")]), output=output)

    result = run(build(tmp_path, steps=[Step(passthrough, name="renamed")]), output=output)
    assert result.steps_restored == 1


@pytest.mark.parametrize(
    "dataset, payload",
    [
        (JSONDataset(), {"n": 1}),
        (PartitionedJSONDataset(), {"doc-1": {"words": 1}}),
        (TextDataset(), "id=doc-1"),
        (JSONDataset(), {"unicode": "café — naïve"}),
    ],
)
def test_supported_datasets_round_trip_through_one_table(tmp_path, dataset, payload):
    def produce(value, *, context):
        return payload

    output = tmp_path / "output"
    pipeline = build(tmp_path, steps=[Step(produce, output=dataset, name="produce")])
    run(pipeline, output=output)

    connection = sqlite3.connect(database(tmp_path))
    stored = connection.execute(
        "SELECT snapshot, codec_identity FROM steps WHERE step_name = 'produce'"
    ).fetchone()
    connection.close()

    assert stored[1] == "json/utf-8"

    restored = run(pipeline, output=output)
    assert restored.steps_restored == 1
    assert restored.steps_executed == 0


def test_a_snapshot_and_its_status_become_visible_together(tmp_path):
    output = tmp_path / "output"
    run(build(tmp_path, commit_scope="step"), output=output)

    fresh = sqlite3.connect(database(tmp_path))
    rows = fresh.execute("SELECT status, snapshot, codec_identity FROM steps").fetchall()
    fresh.close()

    for status, snapshot, identity in rows:
        assert status == "completed"
        assert snapshot is not None
        assert identity is not None


def test_several_jobs_share_one_pipeline_database(tmp_path):
    output = tmp_path / "output"
    run(build(tmp_path, job_id="job-a"), output=output)
    run(build(tmp_path, job_id="job-b", value={"b": 2}), output=output)

    connection = sqlite3.connect(database(tmp_path))
    jobs = {row[0] for row in connection.execute("SELECT job_id FROM runs")}
    counts = connection.execute("SELECT job_id, count(*) FROM steps GROUP BY job_id").fetchall()
    connection.close()

    assert jobs == {"job-a", "job-b"}
    assert dict(counts) == {"job-a": 1, "job-b": 1}


def test_a_new_job_executes_independently(tmp_path):
    output = tmp_path / "output"
    calls = []

    def counted(value, *, context):
        calls.append("x")
        return value

    run(build(tmp_path, steps=[Step(counted)], job_id="job-a"), output=output)
    run(build(tmp_path, steps=[Step(counted)], job_id="job-b"), output=output)
    assert len(calls) == 2


def test_declared_structure_covers_every_branch_route():
    from caravel import Branch

    def route_of(record):
        return record["route"]

    pipeline = Pipeline(
        name="branching",
        stages=[
            Stage(
                name="only",
                entries=[
                    Branch(
                        name="split",
                        by=route_of,
                        routes={
                            "left": [Step(passthrough, name="l1")],
                            "right": [Step(passthrough, name="r1")],
                        },
                    )
                ],
            )
        ],
    )
    structure = declare(pipeline)
    names = [step.name for step in structure.steps()]
    assert names == ["split", "l1", "r1", "split_merge"]
    assert [step.kind for step in structure.steps()] == ["branch", "route", "route", "merge"]


def test_commit_scope_is_recorded_with_the_run(tmp_path):
    run(build(tmp_path, commit_scope="stage"), output=tmp_path / "output")
    connection = sqlite3.connect(database(tmp_path))
    scope = connection.execute("SELECT commit_scope FROM runs").fetchone()[0]
    connection.close()
    assert scope == "stage"
