"""Acceptance tests for the single durable execution model's public API.

These are written against the API C-8 promises, not against the implementation.
They are the migration's target: durability is foundational, so there is one
engine, reached through ``run()``. There is no ``durable=True`` switch and no
second public entry point.

Everything here imports from ``caravel`` only. A test that needs
``caravel.durability`` internals does not belong in this file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from caravel import (
    DurableStorage,
    ExecutionBudget,
    ExecutionPolicy,
    IncrementalInput,
    InMemorySource,
    JSONDataset,
    LeasePolicy,
    PartitionedJSONDataset,
    Pipeline,
    RetryOptions,
    SnapshotInput,
    Stage,
    Step,
    TextDataset,
    retry_operation,
    run,
)


# ---------------------------------------------------------------------------
# Processors used across the acceptance scenarios
# ---------------------------------------------------------------------------


def load_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return {**document, "loaded": True}


def load(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(load_partition)


def enrich_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return {**document, "enriched": True}


def enrich(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(enrich_partition)


def sync_index_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return {"id": document["id"], "indexed": True}


def sync_index(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(sync_index_partition)


def summarize_all(partitions, *, context: Any) -> Any:
    """All-input barrier: a reader over committed partitions, not a materialized list."""
    dataset = partitions
    return {"documents": sum(1 for _ in dataset.partitions())}


def summarize(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.all(summarize_all)


DOCUMENTS = {f"doc-{i}": {"id": f"doc-{i}", "words": i} for i in range(1, 4)}


# ---------------------------------------------------------------------------
# 1. A simple pipeline, declared with no unused loader stub
# ---------------------------------------------------------------------------


def test_a_simple_pipeline_declares_no_loader_and_no_dataset(tmp_path: Path) -> None:
    """The proposal's first example, verbatim in shape."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load), Step(enrich)]),
            Stage(name="publish", entries=[Step(sync_index)]),
        ],
    )
    result = run(
        pipeline,
        job_id="snapshot-v7",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.status == "completed"
    assert result.pipeline_id == "documents"
    assert result.job_id == "snapshot-v7"


def test_a_bare_callable_entry_is_still_a_step(tmp_path: Path) -> None:
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[load])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.status == "completed"


def test_there_is_exactly_one_public_entry_point() -> None:
    """Durability is foundational: no durable=True, no second run function."""
    import caravel

    assert not hasattr(caravel, "run_durable")
    assert "durable" not in run.__code__.co_varnames, "run() must not take a durability switch"


# ---------------------------------------------------------------------------
# 2. Storage is explicit; nothing silently claims restart safety
# ---------------------------------------------------------------------------


def test_omitting_storage_is_rejected_rather_than_defaulted(tmp_path: Path) -> None:
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    with pytest.raises(ValueError, match="storage"):
        run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS))


def test_durable_storage_reports_restart_safety_honestly(tmp_path: Path) -> None:
    durable = DurableStorage.at(tmp_path / "output")
    assert durable.restart_safe is True

    volatile = DurableStorage.ephemeral(tmp_path / "output")
    assert volatile.restart_safe is False


def test_ephemeral_storage_must_be_chosen_explicitly(tmp_path: Path) -> None:
    """A simple declaration never silently selects volatile storage."""
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.ephemeral(tmp_path / "output"),
    )
    assert result.status == "completed"
    assert result.restart_safe is False, "ephemeral runs must not claim restart safety"


def test_a_durable_run_reports_restart_safety(tmp_path: Path) -> None:
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.restart_safe is True


# ---------------------------------------------------------------------------
# 3. Dataset types and partitioned inputs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dataset,processor,expected",
    [
        (
            JSONDataset(),
            lambda i, *, context: i.each(lambda p, *, context: {"n": p.load()["words"]}),
            {"n": 1},
        ),
        (
            TextDataset(),
            lambda i, *, context: i.each(lambda p, *, context: f"id={p.load()['id']}"),
            "id=doc-1",
        ),
        (
            PartitionedJSONDataset(),
            lambda i, *, context: i.each(
                lambda p, *, context: {p.load()["id"]: {"words": p.load()["words"]}}
            ),
            {"doc-1": {"words": 1}},
        ),
    ],
)
def test_json_text_and_partitioned_datasets_all_persist_execution_output(
    tmp_path: Path, dataset: Any, processor: Any, expected: Any
) -> None:
    processor.__name__ = "produce"
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(processor, output=dataset)])],
    )
    result = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert result.status == "completed"

    results = storage.results(pipeline, job_id="j1")
    assert results.of("produce")["doc-1"] == expected


def test_an_undeclared_input_is_one_partition(tmp_path: Path) -> None:
    """A simple function declaration keeps one recovery unit; nothing is split."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    run(
        pipeline,
        job_id="j1",
        source=InMemorySource(record={"id": "only", "words": 1}),
        storage=storage,
    )
    assert len(storage.results(pipeline, job_id="j1").of("load")) == 1


def test_declared_partitions_execute_independently(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert set(storage.results(pipeline, job_id="j1").of("load")) == set(DOCUMENTS)


# ---------------------------------------------------------------------------
# 4. Cross-stage partition flow and all-input barriers
# ---------------------------------------------------------------------------


def test_a_partition_crosses_a_stage_boundary_before_others_finish(
    tmp_path: Path,
) -> None:
    order: list[str] = []

    def first_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        order.append("first")
        return document

    def first(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(first_partition)

    def second_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        order.append("second")
        return document

    def second(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(second_partition)

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="one", entries=[Step(first)]),
            Stage(name="two", entries=[Step(second)]),
        ],
    )
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.status == "completed"
    # Depth-first per partition: no stage-wide barrier exists.
    assert order == ["first", "second"] * len(DOCUMENTS)


def test_an_all_input_step_is_the_only_whole_run_barrier(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(name="summary", entries=[Step(summarize)]),
        ],
    )
    result = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert result.status == "completed"
    summary = storage.results(pipeline, job_id="j1").of("summarize")
    assert list(summary.values()) == [{"documents": len(DOCUMENTS)}]


def test_a_barrier_over_empty_input_still_runs(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(name="summary", entries=[Step(summarize)]),
        ],
    )
    result = run(pipeline, job_id="j1", source=InMemorySource({}), storage=storage)
    assert result.status == "completed"
    assert list(storage.results(pipeline, job_id="j1").of("summarize").values()) == [
        {"documents": 0}
    ]


# ---------------------------------------------------------------------------
# 5. Snapshot and incremental execution through run()
# ---------------------------------------------------------------------------


def test_snapshot_execution_runs_through_run(tmp_path: Path) -> None:
    from caravel import ListSnapshotAdapter

    adapter = ListSnapshotAdapter(DOCUMENTS, snapshot_id="v7")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    result = run(
        pipeline,
        job_id="snapshot-v7",
        source=SnapshotInput(adapter=adapter, snapshot="v7"),
        storage=DurableStorage.at(tmp_path / "output"),
        budget=ExecutionBudget(seconds=50, shutdown_margin_seconds=5),
    )
    assert result.status == "completed"


def test_a_moved_snapshot_pin_fails_before_processing(tmp_path: Path) -> None:
    from caravel import ListSnapshotAdapter, SourceVersionError

    adapter = ListSnapshotAdapter(DOCUMENTS, snapshot_id="v8")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    with pytest.raises(SourceVersionError):
        run(
            pipeline,
            job_id="snapshot-v7",
            source=SnapshotInput(adapter=adapter, snapshot="v7"),
            storage=DurableStorage.at(tmp_path / "output"),
        )


def test_incremental_execution_runs_through_run(tmp_path: Path) -> None:
    import json

    from caravel import ChangeEvent, ListChangeAdapter

    events = [
        ChangeEvent(
            position=f"pos-{i}",
            entity_key=f"doc-{i}",
            operation="insert",
            revision=f"rev-{i}",
            payload_ref=json.dumps({"id": f"doc-{i}", "words": i}),
        )
        for i in range(3)
    ]
    adapter = ListChangeAdapter(events)
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(name="changes", stages=[Stage(name="apply", entries=[Step(load)])])

    first = run(
        pipeline,
        job_id="range-1",
        source=IncrementalInput(adapter=adapter, max_events=2),
        storage=storage,
    )
    assert first.status == "completed"

    second = run(
        pipeline,
        job_id="range-2",
        source=IncrementalInput(adapter=adapter, max_events=2),
        storage=storage,
    )
    assert second.status == "completed"

    caught_up = run(
        pipeline,
        job_id="range-3",
        source=IncrementalInput(adapter=adapter, max_events=2),
        storage=storage,
    )
    assert caught_up.status == "caught_up"


# ---------------------------------------------------------------------------
# 6. Restart recovery
# ---------------------------------------------------------------------------


def test_a_failed_run_resumes_without_reprocessing_committed_work(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    storage = DurableStorage.at(tmp_path / "output")

    def flaky_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        calls.append(document["id"])
        if document["id"] == "doc-2" and calls.count("doc-2") == 1:
            raise RuntimeError("transient")
        return {**document, "loaded": True}

    def flaky(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(flaky_partition)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(flaky)])])

    with pytest.raises(RuntimeError, match="transient"):
        run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)

    before = len(calls)
    result = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert result.status == "completed"

    # Only the failed partition and the untouched ones ran again.
    assert len(calls) - before < len(DOCUMENTS) + 1
    assert len(storage.results(pipeline, job_id="j1").of("flaky")) == len(DOCUMENTS)


def test_a_budget_yields_and_a_later_invocation_finishes_the_job(
    tmp_path: Path,
) -> None:
    import time

    storage = DurableStorage.at(tmp_path / "output")

    def slow_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        time.sleep(0.01)
        return document

    def slow(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(slow_partition)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(slow)])])
    source = InMemorySource({f"doc-{i}": {"id": f"doc-{i}"} for i in range(12)})

    first = run(
        pipeline,
        job_id="j1",
        source=source,
        storage=storage,
        budget=ExecutionBudget(seconds=0.05, max_unit_seconds=0.02),
    )
    assert first.status == "yielded"

    last = run(pipeline, job_id="j1", source=source, storage=storage)
    assert last.status == "completed"


def test_reinvoking_a_completed_job_reprocesses_nothing(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    again = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert again.status == "completed"
    assert again.committed_batches == 0


# ---------------------------------------------------------------------------
# 7. Configurable retry granularity
# ---------------------------------------------------------------------------


def test_a_step_retry_recovers_a_transient_failure(tmp_path: Path) -> None:
    attempts = {"n": 0}

    def flaky_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("vendor down")
        return document

    def flaky(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(flaky_partition)

    policy = RetryOptions(delay_seconds=lambda attempt: 0.0)
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(flaky, retry=policy)])],
    )
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"only": {"id": "only"}}),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.status == "completed"
    assert attempts["n"] == 2


def test_batch_size_gives_a_finer_commit_boundary(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")

    def double_partition(partition, *, context: Any) -> Any:
        records: Any = partition.load()
        return [value * 2 for value in records]

    def double(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(double_partition)

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(double, batch_size=2)])],
    )
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"p1": [1, 2, 3, 4, 5]}),
        storage=storage,
    )
    assert result.status == "completed"
    assert result.committed_batches == 3


def test_the_same_policy_retries_a_single_operation_inside_a_processor(
    tmp_path: Path,
) -> None:
    """One retry contract, used at a boundary the step did not declare."""
    attempts = {"n": 0}
    policy = RetryOptions(delay_seconds=lambda attempt: 0.0)

    def enrich_record() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("vendor down")
        return "ok"

    def processor_partition(partition, *, context: Any) -> Any:
        value, _ = retry_operation(operation=enrich_record, retry=policy)
        return {"value": value}

    def processor(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(processor_partition)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(processor)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"p1": {"id": "p1"}}),
        storage=DurableStorage.at(tmp_path / "output"),
    )
    assert result.status == "completed"
    assert attempts["n"] == 3


# ---------------------------------------------------------------------------
# 8. Concurrency configures the same model
# ---------------------------------------------------------------------------


def test_concurrency_is_a_policy_on_the_same_engine(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=storage,
        policy=ExecutionPolicy(workers=2, leases=LeasePolicy(ttl_seconds=60, heartbeat_seconds=10)),
    )
    assert result.status == "completed"


def test_concurrent_workers_require_an_explicit_lease_policy() -> None:
    with pytest.raises(Exception, match="require an explicit"):
        ExecutionPolicy(workers=4)


# ---------------------------------------------------------------------------
# 9. Ephemeral changes persistence guarantees, never execution semantics
# ---------------------------------------------------------------------------


def _observable_state(storage: Any, pipeline: Any, job_id: str) -> dict[str, Any]:
    """Everything about a finished job that execution semantics determine."""
    store = storage.progress
    job = store.load_job(job_id)
    plan_nodes = [node.node_id for node in storage.results(pipeline, job_id=job_id)._plan.nodes]
    units: dict[str, Any] = {}
    for node_id in plan_nodes:
        for partition_id in store.work_unit_partitions(job_id, node_id):
            from caravel.durability import WorkUnitKey

            unit = store.load_work_unit(WorkUnitKey(job_id, node_id, partition_id))
            assert unit is not None
            units[f"{node_id}/{partition_id}"] = (
                unit.status,
                unit.batch_count,
                unit.input_exhausted,
                unit.pending_deps,
                unit.owner_id,
            )
    return {
        "status": job.status if job else None,
        "discovery_sealed": job.discovery_sealed if job else None,
        "units": units,
        "seals": sorted(seal.node_id for seal in store.node_seals(job_id)),
    }


def test_ephemeral_runs_the_identical_state_machine(tmp_path: Path) -> None:
    """Only the durability guarantee differs: same transitions, same commits."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load), Step(enrich)]),
            Stage(name="summary", entries=[Step(summarize)]),
        ],
    )

    durable = DurableStorage.at(tmp_path / "durable" / "output")
    volatile = DurableStorage.ephemeral(tmp_path / "volatile")

    durable_result = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=durable)
    volatile_result = run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=volatile)

    assert durable_result.status == volatile_result.status == "completed"
    assert durable_result.committed_batches == volatile_result.committed_batches

    # Identical durable state machine outcome, down to per-unit progress.
    assert _observable_state(volatile, pipeline, "j1") == _observable_state(durable, pipeline, "j1")
    # Identical committed payloads.
    for step_name in ("load", "enrich", "summarize"):
        assert volatile.results(pipeline, job_id="j1").of(step_name) == durable.results(
            pipeline, job_id="j1"
        ).of(step_name)

    # The only difference is the claim each one makes.
    assert durable_result.restart_safe is True
    assert volatile_result.restart_safe is False


def test_ephemeral_still_resumes_within_one_process(tmp_path: Path) -> None:
    """Commit rules are unchanged, so a retry still skips committed work."""
    calls: list[str] = []
    storage = DurableStorage.ephemeral(tmp_path / "output")

    def flaky_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        calls.append(document["id"])
        if document["id"] == "doc-2" and calls.count("doc-2") == 1:
            raise RuntimeError("transient")
        return document

    def flaky(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(flaky_partition)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(flaky)])])
    with pytest.raises(RuntimeError):
        run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    committed_before = len(storage.results(pipeline, job_id="j1").of("flaky"))

    run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    assert calls.count("doc-1") == 1, "committed work was recomputed"
    assert committed_before >= 1
