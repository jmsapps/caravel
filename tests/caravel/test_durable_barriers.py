"""All-input barriers: readiness, restart, empty input, and aggregate identity."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import InputPartition, JSONDataset, Pipeline, Stage, Step, StepInputs, TextDataset
from caravel.datasets import PartitionedJSONDataset
from caravel.durability import (
    DurableStorage,
    AGGREGATE_PARTITION_ID,
    AttemptOutputStore,
    BatchPublisher,
    InMemorySource,
    SQLiteDurableStore,
    WorkUnitKey,
)

PIPELINE_ID = "customer-42/documents"
JOB = "snapshot-2026-11-12-v7"
SUMMARIZE_NODE = "stage-002-entry-001"
LOAD_NODE = "stage-001-entry-001"


def load(inputs, *, context: Any) -> Any:
    [partition] = inputs.partitions()
    payload: Any = partition.load()
    del context
    return {"value": payload}


def summarize(inputs, *, context: Any) -> Any:
    """Read committed partition references without materializing the inputs."""
    del context
    assert isinstance(inputs, StepInputs)
    total = 0
    seen = []
    for partition in inputs.partitions():
        assert isinstance(partition, InputPartition)
        seen.append(partition.partition_id)
        total += partition.load()["value"]
    return {"partitions": sorted(seen), "total": total}


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[SQLiteDurableStore]:
    opened = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        yield opened
    finally:
        opened.close()


@pytest.fixture()
def output(tmp_path: Path) -> AttemptOutputStore:
    return AttemptOutputStore(tmp_path / "artifacts")


def _barrier_pipeline(*, summary_dataset: Any = None, after: list[Step] | None = None) -> Pipeline:
    stages = [
        Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))]),
        Stage(
            name="summary",
            entries=[
                Step(
                    fn=summarize,
                    input_scope="all",
                    output=summary_dataset or JSONDataset(name="summary"),
                )
            ],
        ),
    ]
    if after:
        stages.append(Stage(name="after", entries=after))
    return Pipeline(name="documents", stages=stages)


def _run(
    pipeline: Pipeline,
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    source: InMemorySource,
    **kwargs: Any,
) -> Any:
    return caravel.run(
        pipeline,
        pipeline_id=PIPELINE_ID,
        job_id=JOB,
        source=source,
        storage=DurableStorage(store, output, restart_safe=True),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------


def test_a_barrier_waits_for_every_partition_then_runs_once(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    result = _run(
        _barrier_pipeline(), store, output, InMemorySource({"p1": 10, "p2": 20, "p3": 30})
    )
    assert result.status == "completed"

    key = WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID)
    unit = store.load_work_unit(key)
    assert unit is not None
    assert unit.input_scope == "all"
    assert unit.batch_count == 1

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(key, dataset=JSONDataset(name="summary")) == (
        {"partitions": ["p1", "p2", "p3"], "total": 60},
    )


def test_a_barrier_is_blocked_until_its_last_partition_seals(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    import time

    pipeline = _barrier_pipeline()
    source = InMemorySource({f"p{index}": index for index in range(6)})

    def slow_load(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        time.sleep(0.03)
        return {"value": payload}

    pipeline.stages[0].entries[0] = Step(fn=slow_load, output=JSONDataset(name="load"))

    from caravel.durability import ExecutionBudget

    partial = _run(
        pipeline,
        store,
        output,
        source,
        budget=ExecutionBudget(seconds=0.09, max_unit_seconds=0.025),
    )
    assert partial.status == "yielded"

    barrier = store.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
    assert barrier is not None
    assert barrier.status == "eligible"
    assert barrier.pending_deps > 0, "barrier opened before every partition sealed"
    assert all(unit.key.node_id != SUMMARIZE_NODE for unit in store.eligible_work(JOB, limit=20))

    finished = _run(pipeline, store, output, source)
    assert finished.status == "completed"
    reopened = store.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
    assert reopened is not None and reopened.status == "sealed"


def test_barrier_readiness_survives_a_store_reopen(tmp_path: Path) -> None:
    pipeline = _barrier_pipeline()
    source = InMemorySource({"p1": 1, "p2": 2})
    store_path = tmp_path / "durable.sqlite3"

    opened = SQLiteDurableStore(store_path)
    try:
        from caravel.durability.scheduler import compile_durable_plan, register_plan_discovery

        plan = compile_durable_plan(pipeline)
        job = opened.initialize_job(
            JOB,
            pipeline_id=PIPELINE_ID,
            source_mode="snapshot",
            declaration_fingerprint="decl",
        )
        register_plan_discovery(opened, job, plan, source.discover())
        barrier = opened.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
        assert barrier is not None and barrier.pending_deps == 2
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        barrier = reopened.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
        assert barrier is not None
        assert barrier.pending_deps == 2
        assert all(
            unit.key.node_id != SUMMARIZE_NODE for unit in reopened.eligible_work(JOB, limit=20)
        )
    finally:
        reopened.close()


def test_a_failed_required_partition_keeps_the_barrier_closed(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def failing(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        if payload == 2:
            raise RuntimeError("partition failed")
        return {"value": payload}

    pipeline = _barrier_pipeline()
    pipeline.stages[0].entries[0] = Step(fn=failing, output=JSONDataset(name="load"))

    with pytest.raises(RuntimeError, match="partition failed"):
        _run(pipeline, store, output, InMemorySource({"p1": 1, "p2": 2}))

    barrier = store.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
    assert barrier is not None
    assert barrier.status == "eligible"
    assert barrier.pending_deps == 1
    assert all(unit.key.node_id != SUMMARIZE_NODE for unit in store.eligible_work(JOB, limit=20))


def test_a_failed_unit_is_retried_by_a_later_invocation(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Caller-triggered invocation retry: re-invoking recovers failed work."""
    calls = {"n": 0}

    def fail_once(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"value": payload}

    pipeline = _barrier_pipeline()
    pipeline.stages[0].entries[0] = Step(fn=fail_once, output=JSONDataset(name="load"))

    with pytest.raises(RuntimeError, match="boom"):
        _run(pipeline, store, output, InMemorySource({"p1": 1}))

    failed = store.load_work_unit(WorkUnitKey(JOB, LOAD_NODE, "p1"))
    assert failed is not None and failed.status == "failed"
    assert store.eligible_work(JOB, limit=10) == ()
    assert [item.key for item in store.reclaimable_work(JOB, limit=10)] == [failed.key]

    result = _run(pipeline, store, output, InMemorySource({"p1": 1}))
    assert result.status == "completed"
    retried = store.load_work_unit(WorkUnitKey(JOB, LOAD_NODE, "p1"))
    assert retried is not None and retried.status == "sealed"
    assert retried.error_type is None


def test_a_permanently_failing_step_never_completes_the_job(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def always_fails(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        raise RuntimeError("permanent")

    pipeline = _barrier_pipeline()
    pipeline.stages[0].entries[0] = Step(fn=always_fails, output=JSONDataset(name="load"))
    source = InMemorySource({"p1": 1})

    for _ in range(3):
        with pytest.raises(RuntimeError, match="permanent"):
            _run(pipeline, store, output, source)

    job = store.load_job(JOB)
    assert job is not None and job.status == "open"
    barrier = store.load_work_unit(WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID))
    assert barrier is not None
    assert barrier.status == "eligible"
    assert barrier.pending_deps == 1


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------


def test_a_sealed_empty_input_opens_the_barrier_over_an_empty_dataset(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    result = _run(_barrier_pipeline(), store, output, InMemorySource({}))
    assert result.status == "completed"

    key = WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID)
    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(key, dataset=JSONDataset(name="summary")) == (
        {"partitions": [], "total": 0},
    )
    assert store.work_unit_partitions(JOB, LOAD_NODE) == ()


def test_an_empty_input_without_a_barrier_completes_with_no_work(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])],
    )
    result = _run(pipeline, store, output, InMemorySource({}))
    assert result.status == "completed"
    assert result.committed_batches == 0
    job = store.load_job(JOB)
    assert job is not None and job.status == "completed"


# ---------------------------------------------------------------------------
# Aggregate identity and downstream steps
# ---------------------------------------------------------------------------


def test_aggregate_output_becomes_one_stable_partition_downstream(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def after_summary(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        return {"final": payload["total"]}

    pipeline = _barrier_pipeline(after=[Step(fn=after_summary, output=JSONDataset(name="final"))])
    result = _run(pipeline, store, output, InMemorySource({"p1": 5, "p2": 7}))
    assert result.status == "completed"

    final_node = "stage-003-entry-001"
    assert store.work_unit_partitions(JOB, final_node) == (AGGREGATE_PARTITION_ID,)

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, final_node, AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="final"),
    ) == ({"final": 12},)


def test_the_aggregate_identity_is_stable_across_invocations(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": 1})
    _run(_barrier_pipeline(), store, output, source)
    first = store.work_unit_partitions(JOB, SUMMARIZE_NODE)
    _run(_barrier_pipeline(), store, output, source)
    assert store.work_unit_partitions(JOB, SUMMARIZE_NODE) == first == (AGGREGATE_PARTITION_ID,)


@pytest.mark.parametrize(
    "dataset,expected",
    [
        (JSONDataset(name="summary"), {"partitions": ["p1", "p2"], "total": 3}),
        (TextDataset(name="summary"), "p1,p2"),
        (PartitionedJSONDataset(name="summary"), {"all": {"total": 3}}),
    ],
)
def test_a_barrier_works_with_every_dataset_type(
    store: SQLiteDurableStore, output: AttemptOutputStore, dataset: Any, expected: Any
) -> None:
    def summarize_as(inputs, *, context: Any) -> Any:
        del context
        views = list(inputs.partitions())
        total = sum(view.load()["value"] for view in views)
        names = sorted(view.partition_id for view in views)
        if isinstance(dataset, TextDataset):
            return ",".join(names)
        if isinstance(dataset, PartitionedJSONDataset):
            return {"all": {"total": total}}
        return {"partitions": names, "total": total}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))]),
            Stage(
                name="summary",
                entries=[Step(fn=summarize_as, input_scope="all", output=dataset)],
            ),
        ],
    )
    result = _run(pipeline, store, output, InMemorySource({"p1": 1, "p2": 2}))
    assert result.status == "completed"

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, SUMMARIZE_NODE, AGGREGATE_PARTITION_ID), dataset=dataset
    ) == (expected,)


def test_a_barrier_reads_references_rather_than_a_materialized_dataset(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """The reader exposes counts and identities without loading any payload."""
    inspected: dict[str, Any] = {}

    def inspect(inputs, *, context: Any) -> Any:
        del context
        views = list(inputs.partitions())
        inspected["count"] = len(views)
        inspected["ids"] = sorted(view.partition_id for view in views)
        inspected["counts"] = [view.count for view in views]
        return {"seen": inspected["count"]}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))]),
            Stage(
                name="summary",
                entries=[Step(fn=inspect, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    _run(pipeline, store, output, InMemorySource({"a": 1, "b": 2, "c": 3}))
    assert inspected["count"] == 3
    assert inspected["ids"] == ["a", "b", "c"]
    assert inspected["counts"] == [1, 1, 1]


def test_only_an_explicit_all_scope_imposes_a_whole_run_barrier(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Two partition-scoped stages impose no whole-run wait."""
    order: list[str] = []

    def first(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        order.append("first")
        return {"value": payload}

    def second(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        order.append("second")
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="one", entries=[Step(fn=first, output=JSONDataset(name="first"))]),
            Stage(name="two", entries=[Step(fn=second, output=JSONDataset(name="second"))]),
        ],
    )
    _run(pipeline, store, output, InMemorySource({"p1": 1, "p2": 2}))
    assert order == ["first", "second", "first", "second"]


def count_partitions(inputs, *, context: Any) -> Any:
    """Count whatever the reader yields, source records or committed output."""
    del context
    return {"count": sum(1 for _ in inputs.partitions())}


def test_a_leading_all_input_step_reads_the_source_not_an_empty_reader(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Regression: declared first, a barrier's 'all input' is the source itself.

    This previously handed the step an empty reader, so the step silently
    aggregated nothing and still reported completed.
    """
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="summary",
                entries=[
                    Step(
                        fn=count_partitions,
                        input_scope="all",
                        output=JSONDataset(name="summary"),
                    )
                ],
            )
        ],
    )
    result = _run(pipeline, store, output, InMemorySource({"p1": 1, "p2": 2, "p3": 3}))
    assert result.status == "completed"

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, "stage-001-entry-001", AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="summary"),
    ) == ({"count": 3},)


def test_a_leading_barrier_over_an_empty_source_still_reports_zero(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="summary",
                entries=[
                    Step(
                        fn=count_partitions,
                        input_scope="all",
                        output=JSONDataset(name="summary"),
                    )
                ],
            )
        ],
    )
    assert _run(pipeline, store, output, InMemorySource({})).status == "completed"
    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, "stage-001-entry-001", AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="summary"),
    ) == ({"count": 0},)
