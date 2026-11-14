"""Branching: one route runs, the others are recorded as skipped."""

from __future__ import annotations

import sqlite3

import pytest

from caravel import (
    Branch,
    DurabilityOptions,
    InMemorySource,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.durability.structure import declare


def tag(value, *, context):
    return value


def left(value, *, context):
    return {**value, "route": "left", "handled_by": "left"}


def right(value, *, context):
    return {**value, "route": "right", "handled_by": "right"}


def by_route(record):
    return record["route"]


def build(job_id="branch-1", commit_scope="step", routes=None):
    return Pipeline(
        name="branch_example",
        stages=[
            Stage(
                name="only",
                entries=[
                    Branch(
                        name="split",
                        by=by_route,
                        routes=routes
                        or {
                            "left": [Step(left, output=JSONDataset(name="left"))],
                            "right": [Step(right, output=JSONDataset(name="right"))],
                        },
                    )
                ],
            )
        ],
        source=InMemorySource({"route": "left", "id": "a"}),
        durability_options=DurabilityOptions(
            schema_version="v1.0.0", job_id=job_id, commit_scope=commit_scope
        ),
    )


def steps(tmp_path):
    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    try:
        return {row[0]: row[1] for row in connection.execute("SELECT step_name, status FROM steps")}
    finally:
        connection.close()


def test_the_chosen_route_runs_and_the_others_are_skipped(tmp_path):
    run(build(), output=tmp_path / "output")
    assert steps(tmp_path) == {
        "split": "completed",
        "left": "completed",
        "right": "skipped",
        "split_merge": "completed",
    }


def test_the_merge_carries_the_chosen_route_result(tmp_path):
    result = run(build(), output=tmp_path / "output")
    merged = JSONDataset().load_from(result.outputs["split_merge"])
    assert merged["handled_by"] == "left"


def test_the_route_decision_is_recorded_with_the_branch_step(tmp_path):
    run(build(), output=tmp_path / "output")
    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    route_key = connection.execute(
        "SELECT route_key FROM steps WHERE step_name = 'split'"
    ).fetchone()[0]
    connection.close()
    assert route_key == "left"


def test_a_resume_reuses_the_committed_route_decision(tmp_path):
    output = tmp_path / "output"
    decisions = []

    def recording(record):
        decisions.append(record["route"])
        return record["route"]

    pipeline = Pipeline(
        name="branch_example",
        stages=[
            Stage(
                name="only",
                entries=[
                    Branch(
                        name="split",
                        by=recording,
                        routes={
                            "left": [Step(left, output=JSONDataset(name="left"))],
                            "right": [Step(right, output=JSONDataset(name="right"))],
                        },
                    )
                ],
            )
        ],
        source=InMemorySource({"route": "left", "id": "a"}),
        durability_options=DurabilityOptions(
            schema_version="v1.0.0", job_id="branch-resume", commit_scope="step"
        ),
    )
    run(pipeline, output=output)
    assert decisions == ["left"]

    run(pipeline, output=output)
    assert decisions == ["left"]


def test_a_stage_with_a_branch_completes(tmp_path):
    run(build(), output=tmp_path / "output")
    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    stage_status = connection.execute("SELECT status FROM stages").fetchone()[0]
    run_status = connection.execute("SELECT status FROM runs").fetchone()[0]
    connection.close()
    assert stage_status == "completed"
    assert run_status == "completed"


def test_every_declared_route_is_part_of_the_structure():
    structure = declare(build())
    assert [step.name for step in structure.steps()] == ["split", "left", "right", "split_merge"]
    chain = structure.route_chain("stage-001-entry-001", "right")
    assert [step.name for step in chain] == ["right"]


def test_a_route_decision_names_a_declared_route(tmp_path):
    def missing(record):
        return "elsewhere"

    pipeline = build()
    pipeline.stages[0].entries[0].by = missing
    with pytest.raises(KeyError, match="elsewhere"):
        run(pipeline, output=tmp_path / "output")


def test_a_source_tag_selects_the_route(tmp_path):
    from caravel import SOURCE_FIELD

    pipeline = Pipeline(
        name="tagged",
        stages=[
            Stage(
                name="only",
                entries=[
                    Branch(
                        name="split",
                        by="source",
                        routes={
                            "feed_a": [Step(left, output=JSONDataset(name="left"))],
                            "feed_b": [Step(right, output=JSONDataset(name="right"))],
                        },
                    )
                ],
            )
        ],
        source=InMemorySource({"one": {SOURCE_FIELD: "feed_b", "value": 1}}),
        durability_options=DurabilityOptions(
            schema_version="v1.0.0", job_id="tagged-1", commit_scope="step"
        ),
    )
    run(pipeline, output=tmp_path / "output")
    assert steps(tmp_path)["right"] == "completed"
    assert steps(tmp_path)["left"] == "skipped"
