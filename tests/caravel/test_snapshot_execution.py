"""Snapshot execution: pinned identity, version validation, and restart."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    DurableStorage,
    AGGREGATE_PARTITION_ID,
    AttemptOutputStore,
    BatchPublisher,
    ListSnapshotAdapter,
    SnapshotInput,
    SourceVersionError,
    SQLiteDurableStore,
    WorkUnitKey,
)

PIPELINE_ID = "customer-42/documents"
JOB = "snapshot-2026-11-12-v7"
LOAD_NODE = "stage-001-entry-001"


def load(payload: Any, *, context: Any) -> Any:
    del context
    return {"loaded": payload}


def summarize(reader: Any, *, context: Any) -> Any:
    del context
    return {"count": len(list(reader.partitions()))}


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


def _pipeline(*, with_barrier: bool = False) -> Pipeline:
    stages = [Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])]
    if with_barrier:
        stages.append(
            Stage(
                name="summary",
                entries=[Step(fn=summarize, output=JSONDataset(name="summary"), input_scope="all")],
            )
        )
    return Pipeline(name="documents", stages=stages)


def _run(pipeline: Pipeline, store: Any, output: Any, source: Any, **kwargs: Any) -> Any:
    return caravel.run(
        pipeline,
        pipeline_id=PIPELINE_ID,
        job_id=JOB,
        source=source,
        storage=DurableStorage(store, output, restart_safe=True),
        **kwargs,
    )


def test_a_snapshot_job_completes_over_its_pinned_input(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListSnapshotAdapter({"d1": {"n": 1}, "d2": {"n": 2}}, snapshot_id="v7")
    result = _run(_pipeline(), store, output, SnapshotInput(adapter, snapshot="v7"))

    assert result.status == "completed"
    job = store.load_job(JOB)
    assert job is not None
    assert job.source_mode == "snapshot"
    assert job.snapshot_id == "v7"

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, LOAD_NODE, "d1"), dataset=JSONDataset(name="load")
    ) == ({"loaded": {"n": 1}},)


def test_a_moved_snapshot_pin_is_rejected_before_any_processing(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    calls: list[str] = []

    def counted(payload: Any, *, context: Any) -> Any:
        del context
        calls.append("called")
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=counted, output=JSONDataset(name="load"))])],
    )
    adapter = ListSnapshotAdapter({"d1": {"n": 1}}, snapshot_id="v8")

    with pytest.raises(SourceVersionError, match="no longer the adapter's current"):
        _run(pipeline, store, output, SnapshotInput(adapter, snapshot="v7"))

    assert calls == []
    assert store.load_job(JOB) is None, "no durable mutation before validation"


def test_a_changed_partition_under_an_unchanged_pin_is_rejected(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """An upper identity alone does not freeze mutable record contents."""
    adapter = ListSnapshotAdapter({"d1": {"n": 1}, "d2": {"n": 2}}, snapshot_id="v7")
    source = SnapshotInput(adapter, snapshot="v7")

    from caravel.durability.scheduler import (
        compile_durable_plan,
        declaration_fingerprint,
        register_plan_discovery,
    )

    plan = compile_durable_plan(_pipeline())
    job = store.initialize_job(
        JOB,
        pipeline_id=PIPELINE_ID,
        source_mode="snapshot",
        declaration_fingerprint=declaration_fingerprint(plan)[:512],
        snapshot_id="v7",
    )
    register_plan_discovery(store, job, plan, source.discover())

    # The record changes but the snapshot identity does not.
    adapter.replace_record("d1", {"n": 999})

    with pytest.raises(SourceVersionError, match="changed version under pinned"):
        _run(_pipeline(), store, output, source)


def test_a_partition_removed_from_the_pinned_snapshot_is_rejected(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListSnapshotAdapter({"d1": {"n": 1}}, snapshot_id="v7")
    source = SnapshotInput(adapter, snapshot="v7")
    discovered = source.discover()

    del adapter._records["d1"]
    with pytest.raises(SourceVersionError, match="no longer present"):
        source.load(discovered[0])


def test_a_snapshot_job_resumes_without_reprocessing(tmp_path: Path) -> None:
    calls: list[str] = []

    def counted(payload: Any, *, context: Any) -> Any:
        del context
        calls.append(str(payload))
        if len(calls) == 1:
            raise RuntimeError("stop after first")
        return {"loaded": payload}

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=counted, output=JSONDataset(name="load"))])],
    )
    adapter = ListSnapshotAdapter({"d1": {"n": 1}, "d2": {"n": 2}}, snapshot_id="v7")
    store_path = tmp_path / "durable.sqlite3"
    artifacts = tmp_path / "artifacts"

    opened = SQLiteDurableStore(store_path)
    try:
        with pytest.raises(RuntimeError):
            _run(
                pipeline,
                opened,
                AttemptOutputStore(artifacts),
                SnapshotInput(adapter, snapshot="v7"),
            )
    finally:
        opened.close()
    assert len(calls) == 1

    reopened = SQLiteDurableStore(store_path)
    try:
        result = _run(
            pipeline,
            reopened,
            AttemptOutputStore(artifacts),
            SnapshotInput(adapter, snapshot="v7"),
        )
        assert result.status == "completed"
    finally:
        reopened.close()

    # d1 is retried once (its first attempt failed) and d2 runs once. Nothing
    # that already committed is recomputed.
    assert len(calls) == 3


def test_a_snapshot_job_does_not_advance_a_lineage_checkpoint(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListSnapshotAdapter({"d1": {"n": 1}}, snapshot_id="v7")
    result = _run(_pipeline(), store, output, SnapshotInput(adapter, snapshot="v7"))
    assert result.status == "completed"
    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None
    assert checkpoint.completed_position is None


def test_an_all_input_barrier_runs_over_a_pinned_snapshot(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListSnapshotAdapter(
        {"d1": {"n": 1}, "d2": {"n": 2}, "d3": {"n": 3}}, snapshot_id="v7"
    )
    result = _run(
        _pipeline(with_barrier=True), store, output, SnapshotInput(adapter, snapshot="v7")
    )
    assert result.status == "completed"

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, "stage-002-entry-001", AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="summary"),
    ) == ({"count": 3},)


def test_an_empty_pinned_snapshot_completes(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListSnapshotAdapter({}, snapshot_id="v7")
    result = _run(
        _pipeline(with_barrier=True), store, output, SnapshotInput(adapter, snapshot="v7")
    )
    assert result.status == "completed"
    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey(JOB, "stage-002-entry-001", AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="summary"),
    ) == ({"count": 0},)


def test_snapshot_input_requires_a_snapshot_adapter() -> None:
    with pytest.raises(TypeError, match="snapshot adapter"):
        SnapshotInput(object(), snapshot="v7")  # type: ignore[arg-type]
