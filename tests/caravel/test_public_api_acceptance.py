"""The public surface, exercised only through ``caravel``.

Every import here comes from the package root, so this file states what a user
can rely on without reaching into internals.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    Branch,
    CallableLoader,
    DurabilityOptions,
    ExecutorBusyError,
    InMemorySource,
    JSONDataset,
    MultiSourceLoader,
    PartitionedJSONDataset,
    Pipeline,
    RetryOptions,
    Stage,
    Step,
    StructureMismatchError,
    TextDataset,
    retry_operation,
    run,
)

DOCUMENTS = {"doc-1": {"id": "doc-1", "n": 1}, "doc-2": {"id": "doc-2", "n": 2}}


def collect(value: Any, *, context: Any) -> Any:
    return {key: {**record, "collected": True} for key, record in value.items()}


def summarize(value: Any, *, context: Any) -> Any:
    return {"documents": len(value), "total": sum(r["n"] for r in value.values())}


def options(job_id: str, commit_scope: str = "step") -> DurabilityOptions:
    return DurabilityOptions(schema_version="v1.0.0", job_id=job_id, commit_scope=commit_scope)


# --- declarations ---------------------------------------------------------


def test_a_declaration_needs_only_a_processor(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(collect), Step(summarize)])],
        source=InMemorySource(DOCUMENTS),
    )
    result = run(pipeline, output=tmp_path / "output")
    assert result.pipeline_name == "documents"
    assert result.steps_executed == 2


def test_a_bare_callable_is_a_step(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[collect])],
        source=InMemorySource(DOCUMENTS),
    )
    assert run(pipeline, output=tmp_path / "output").steps_executed == 1


def test_one_execution_entry_point() -> None:
    import caravel

    assert callable(caravel.run)
    assert not hasattr(caravel, "run_durable")


# --- durability is a declaration ------------------------------------------


def test_a_run_without_durability_options_keeps_no_database(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(collect)])],
        source=InMemorySource(DOCUMENTS),
    )
    result = run(pipeline, output=tmp_path / "output")
    assert result.job_id is None
    assert not (tmp_path / ".caravel" / "state.sqlite3").exists()


def test_durability_options_state_their_schema_version() -> None:
    with pytest.raises(ValueError, match="v1.0.0"):
        DurabilityOptions(schema_version="v0.9.0", job_id="j1")  # type: ignore[arg-type]


def test_a_job_id_is_required_for_a_durable_run() -> None:
    with pytest.raises(ValueError, match="job_id"):
        DurabilityOptions(schema_version="v1.0.0", job_id="")


def test_the_commit_scope_defaults_to_the_pipeline_boundary() -> None:
    assert DurabilityOptions(schema_version="v1.0.0", job_id="j1").commit_scope == "pipeline"


@pytest.mark.parametrize("scope", ["pipeline", "stage", "step"])
def test_every_commit_scope_completes_a_run(tmp_path: Path, scope: str) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(collect)])],
        source=InMemorySource(DOCUMENTS),
        durability_options=options("j1", scope),
    )
    assert run(pipeline, output=tmp_path / "output").steps_executed == 1


# --- datasets -------------------------------------------------------------


@pytest.mark.parametrize(
    "dataset, payload",
    [
        (JSONDataset(), {"n": 1}),
        (TextDataset(), "id=doc-1"),
        (PartitionedJSONDataset(), {"doc-1": {"words": 1}}),
    ],
)
def test_declared_datasets_persist_and_read_back(
    tmp_path: Path, dataset: Any, payload: Any
) -> None:
    def produce(value: Any, *, context: Any) -> Any:
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(produce, output=dataset, name="produce")])],
        source=InMemorySource(DOCUMENTS),
    )
    result = run(pipeline, output=tmp_path / "output")
    assert dataset.load_from(result.outputs["produce"]) == payload


# --- loaders --------------------------------------------------------------


def test_a_loader_supplies_the_initial_value(tmp_path: Path) -> None:
    seen: list[Any] = []

    def inspect(value: Any, *, context: Any) -> Any:
        seen.append(value)
        return value

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(inspect)])],
        source=CallableLoader("feed", lambda: {"one": {"n": 1}}),
    )
    run(pipeline, output=tmp_path / "output")
    assert seen == [{"one": {"n": 1}}]


def test_a_multi_source_loader_tags_each_record(tmp_path: Path) -> None:
    from caravel import SOURCE_FIELD

    seen: list[Any] = []

    def inspect(value: Any, *, context: Any) -> Any:
        seen.append({key: record[SOURCE_FIELD] for key, record in value.items()})
        return {"count": len(value)}

    loader = MultiSourceLoader(
        [
            CallableLoader("feed_a", lambda: {"one": {"n": 1}}),
            CallableLoader("feed_b", lambda: {"two": {"n": 2}}),
        ]
    )
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(inspect)])],
        source=loader,
    )
    run(pipeline, output=tmp_path / "output")
    assert seen == [{"one": "feed_a", "two": "feed_b"}]


# --- recovery -------------------------------------------------------------


def test_reopening_a_job_restores_its_committed_steps(tmp_path: Path) -> None:
    output = tmp_path / "output"
    calls: list[str] = []

    def counted(value: Any, *, context: Any) -> Any:
        calls.append("x")
        return value

    def build() -> Pipeline:
        return Pipeline(
            name="documents",
            stages=[Stage(name="prepare", entries=[Step(counted)])],
            source=InMemorySource(DOCUMENTS),
            durability_options=options("j1"),
        )

    run(build(), output=output)
    assert calls == ["x"]

    result = run(build(), output=output)
    assert calls == ["x"]
    assert result.steps_restored == 1


def test_a_new_job_id_executes_independently(tmp_path: Path) -> None:
    output = tmp_path / "output"
    calls: list[str] = []

    def counted(value: Any, *, context: Any) -> Any:
        calls.append("x")
        return value

    def build(job_id: str) -> Pipeline:
        return Pipeline(
            name="documents",
            stages=[Stage(name="prepare", entries=[Step(counted)])],
            source=InMemorySource(DOCUMENTS),
            durability_options=options(job_id),
        )

    run(build("j1"), output=output)
    run(build("j2"), output=output)
    assert len(calls) == 2


def test_resuming_a_job_matches_its_declared_structure(tmp_path: Path) -> None:
    output = tmp_path / "output"

    def build(entries: list[Any]) -> Pipeline:
        return Pipeline(
            name="documents",
            stages=[Stage(name="prepare", entries=entries)],
            source=InMemorySource(DOCUMENTS),
            durability_options=options("j1"),
        )

    run(build([Step(collect)]), output=output)
    with pytest.raises(StructureMismatchError):
        run(build([Step(collect), Step(summarize)]), output=output)


def test_one_executor_runs_a_recovery_database_at_a_time(tmp_path: Path) -> None:
    from caravel.durability.ownership import ExecutorOwnership

    reserved = tmp_path / ".caravel"
    reserved.mkdir(parents=True)
    holder = ExecutorOwnership(reserved)
    holder.acquire(database=reserved / "state.sqlite3")
    try:
        with pytest.raises(ExecutorBusyError):
            ExecutorOwnership(reserved).acquire(database=reserved / "state.sqlite3")
    finally:
        holder.release()


# --- retries --------------------------------------------------------------


def test_a_step_retry_recovers_a_transient_failure(tmp_path: Path) -> None:
    attempts: list[int] = []

    def flaky(value: Any, *, context: Any) -> Any:
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return value

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(flaky, retry=RetryOptions(delay_seconds=lambda attempt: 0.0))],
            )
        ],
        source=InMemorySource(DOCUMENTS),
    )
    run(pipeline, output=tmp_path / "output")
    assert len(attempts) == 2


def test_the_same_options_retry_an_operation_inside_a_processor(tmp_path: Path) -> None:
    attempts: list[int] = []
    shared = RetryOptions(delay_seconds=lambda attempt: 0.0)

    def inner() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("transient")
        return "ok"

    def processor(value: Any, *, context: Any) -> Any:
        result, count = retry_operation(operation=inner, retry=shared)
        return {"result": result, "attempts": count}

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(processor)])],
        source=InMemorySource(DOCUMENTS),
    )
    run(pipeline, output=tmp_path / "output")
    assert len(attempts) == 3


# --- branches -------------------------------------------------------------


def test_a_branch_runs_one_route(tmp_path: Path) -> None:
    def left(value: Any, *, context: Any) -> Any:
        return {**value, "handled": "left"}

    def right(value: Any, *, context: Any) -> Any:
        return {**value, "handled": "right"}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="split",
                        by=lambda record: record["kind"],
                        routes={"a": [Step(left)], "b": [Step(right)]},
                    )
                ],
            )
        ],
        source=InMemorySource({"kind": "b"}),
    )
    result = run(pipeline, output=tmp_path / "output")
    merged = JSONDataset().load_from(result.outputs["split_merge"])
    assert merged["handled"] == "right"


# --- output ---------------------------------------------------------------


def test_a_run_reports_where_each_step_wrote(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(collect, output=PartitionedJSONDataset())])],
        source=InMemorySource(DOCUMENTS),
    )
    result = run(pipeline, output=tmp_path / "output")
    written = Path(result.outputs["collect"])
    assert written.exists()
    payload = json.loads(sorted(written.rglob("*.json"))[0].read_text())
    assert payload["collected"] is True
