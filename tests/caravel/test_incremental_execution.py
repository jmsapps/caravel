"""Incremental execution: bounded ranges, lineage checkpoints, and change semantics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    DurableStorage,
    AGGREGATE_PARTITION_ID,
    AllocatedRange,
    AttemptOutputStore,
    BatchPublisher,
    ChangeEvent,
    IncrementalInput,
    ListChangeAdapter,
    RetentionGapError,
    SQLiteDurableStore,
    WorkUnitKey,
)

PIPELINE_ID = "customer-42/document-changes"
LOAD_NODE = "stage-001-entry-001"


def apply_change(inputs, *, context: Any) -> Any:
    [partition] = inputs.partitions()
    payload: Any = partition.load()
    del context
    return {"applied": payload}


def summarize(inputs, *, context: Any) -> Any:
    del context
    return {"changes": len(list(inputs.partitions()))}


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
    stages = [
        Stage(name="apply", entries=[Step(fn=apply_change, output=JSONDataset(name="apply"))])
    ]
    if with_barrier:
        stages.append(
            Stage(
                name="summary",
                entries=[Step(fn=summarize, input_scope="all", output=JSONDataset(name="summary"))],
            )
        )
    return Pipeline(name="changes", stages=stages)


def _event(index: int, key: str, operation: str = "insert", value: Any = None) -> ChangeEvent:
    return ChangeEvent(
        position=f"pos-{index:04d}",
        entity_key=key,
        operation=operation,  # type: ignore[arg-type]
        revision=f"rev-{index}",
        payload_ref=json.dumps({"key": key, "value": value if value is not None else index}),
    )


def _run(pipeline: Pipeline, store: Any, output: Any, source: Any, *, job_id: str) -> Any:
    return caravel.run(
        pipeline,
        pipeline_id=PIPELINE_ID,
        job_id=job_id,
        source=source,
        storage=DurableStorage(store, output, restart_safe=True),
    )


# ---------------------------------------------------------------------------
# Range allocation and checkpoints
# ---------------------------------------------------------------------------


def test_an_incremental_run_allocates_bounds_and_advances_the_checkpoint(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(5)])
    result = _run(
        _pipeline(), store, output, IncrementalInput(adapter, max_events=3), job_id="range-1"
    )
    assert result.status == "completed"

    job = store.load_job("range-1")
    assert job is not None
    assert job.source_mode == "incremental"
    assert (job.range_start, job.range_end) == ("pos-0000", "pos-0002")

    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None
    assert checkpoint.completed_position == "pos-0002"


def test_the_next_range_starts_after_the_committed_checkpoint(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(5)])
    _run(_pipeline(), store, output, IncrementalInput(adapter, max_events=3), job_id="range-1")
    _run(_pipeline(), store, output, IncrementalInput(adapter, max_events=3), job_id="range-2")

    second = store.load_job("range-2")
    assert second is not None
    assert (second.range_start, second.range_end) == ("pos-0003", "pos-0004")
    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None and checkpoint.completed_position == "pos-0004"


def test_a_retried_job_reuses_its_stored_bounds_without_reallocating(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Allocate once, reuse always: later arrivals never expand a live range."""
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(3)])
    calls = {"n": 0}

    def fail_once(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("stop")
        return {"applied": payload}

    pipeline = _pipeline()
    pipeline.stages[0].entries[0] = Step(fn=fail_once, output=JSONDataset(name="apply"))

    with pytest.raises(RuntimeError):
        _run(pipeline, store, output, IncrementalInput(adapter, max_events=10), job_id="range-1")
    job = store.load_job("range-1")
    assert job is not None
    bounds = (job.range_start, job.range_end)
    assert bounds == ("pos-0000", "pos-0002")

    # New events arrive between invocations.
    adapter.append(_event(3, "doc-3"), _event(4, "doc-4"))

    result = _run(
        pipeline, store, output, IncrementalInput(adapter, max_events=10), job_id="range-1"
    )
    assert result.status == "completed"

    resumed = store.load_job("range-1")
    assert resumed is not None
    assert (resumed.range_start, resumed.range_end) == bounds, "range expanded on retry"
    assert len(store.work_unit_partitions("range-1", LOAD_NODE)) == 3


def test_events_arriving_after_allocation_belong_to_the_next_range(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(0, "doc-a")])
    _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1")

    adapter.append(_event(1, "doc-a", operation="update", value="updated"))
    _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-2")

    second = store.load_job("range-2")
    assert second is not None
    assert (second.range_start, second.range_end) == ("pos-0001", "pos-0001")
    partitions = store.work_unit_partitions("range-2", LOAD_NODE)
    assert len(partitions) == 1


def test_an_empty_live_source_reports_caught_up(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([])
    result = _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1")

    assert result.status == "caught_up"
    assert store.load_job("range-1") is None, "no job is allocated when caught up"
    assert store.load_checkpoint(PIPELINE_ID, "default") is None


def test_a_later_arrival_is_processable_after_being_caught_up(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([])
    assert (
        _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1").status
        == "caught_up"
    )

    adapter.append(_event(0, "doc-a"))
    result = _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1")
    assert result.status == "completed"
    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None and checkpoint.completed_position == "pos-0000"


def test_a_caught_up_pipeline_stays_open_after_a_completed_range(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(0, "doc-a")])
    assert (
        _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1").status
        == "completed"
    )
    assert (
        _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-2").status
        == "caught_up"
    )


# ---------------------------------------------------------------------------
# Retention and replay
# ---------------------------------------------------------------------------


def test_a_retention_gap_fails_closed_and_never_resets_a_position(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(4)])
    calls = {"n": 0}

    def fail_once(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("stop")
        return {"applied": payload}

    pipeline = _pipeline()
    pipeline.stages[0].entries[0] = Step(fn=fail_once, output=JSONDataset(name="apply"))

    with pytest.raises(RuntimeError):
        _run(pipeline, store, output, IncrementalInput(adapter), job_id="range-1")

    adapter.expire_through(2)

    with pytest.raises(RetentionGapError, match="no longer available"):
        _run(pipeline, store, output, IncrementalInput(adapter), job_id="range-1")

    job = store.load_job("range-1")
    assert job is not None
    assert (job.range_start, job.range_end) == ("pos-0000", "pos-0003")
    assert job.status == "open"
    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None and checkpoint.completed_position is None


def test_reading_an_expired_range_raises_rather_than_returning_less() -> None:
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(4)])
    allocated = AllocatedRange(start="pos-0000", end="pos-0003")
    adapter.expire_through(2)
    assert adapter.is_replayable(allocated) is False
    with pytest.raises(RetentionGapError, match="expired"):
        adapter.read_range(allocated)


# ---------------------------------------------------------------------------
# Positions stay opaque
# ---------------------------------------------------------------------------


def test_sharded_vector_positions_round_trip_unchanged(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """The engine stores and returns positions; it never compares or parses them."""
    vector_positions = [
        json.dumps({"shard-a": 7, "shard-b": 3}),
        json.dumps({"shard-a": 9, "shard-b": 3}),
    ]
    events = [
        ChangeEvent(
            position=vector_positions[index],
            entity_key=f"doc-{index}",
            operation="insert",
            revision=f"rev-{index}",
            payload_ref=json.dumps({"index": index}),
        )
        for index in range(2)
    ]
    adapter = ListChangeAdapter(events)
    result = _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1")
    assert result.status == "completed"

    job = store.load_job("range-1")
    assert job is not None
    assert job.range_start == vector_positions[0]
    assert job.range_end == vector_positions[1]
    checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
    assert checkpoint is not None
    assert checkpoint.completed_position == vector_positions[1]
    assert json.loads(checkpoint.completed_position) == {"shard-a": 9, "shard-b": 3}


# ---------------------------------------------------------------------------
# Change semantics and a version-aware destination
# ---------------------------------------------------------------------------


class FakeVersionedIndex:
    """A test double that applies changes with source-aware version checks.

    It lives here deliberately: destination semantics are an application
    contract, and no destination-specific behavior belongs in the library.
    """

    def __init__(self) -> None:
        self.entities: dict[str, Any] = {}
        self.revisions: dict[str, str] = {}
        self.deleted: set[str] = set()
        self.rejected: list[tuple[str, str]] = []

    def apply(self, *, key: str, operation: str, revision: str, value: Any) -> None:
        known = self.revisions.get(key)
        if known is not None and revision <= known:
            # Older work must not overwrite a newer value or resurrect a delete.
            self.rejected.append((key, revision))
            return
        self.revisions[key] = revision
        if operation == "delete":
            self.deleted.add(key)
            self.entities.pop(key, None)
            return
        self.deleted.discard(key)
        self.entities[key] = value


def test_change_metadata_reaches_the_durable_partition_record(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter(
        [
            _event(0, "doc-a", operation="insert"),
            _event(1, "doc-a", operation="update"),
            _event(2, "doc-b", operation="delete"),
        ]
    )
    _run(_pipeline(), store, output, IncrementalInput(adapter), job_id="range-1")

    records = [
        store.load_partition("range-1", partition_id)
        for partition_id in store.work_unit_partitions("range-1", LOAD_NODE)
    ]
    assert [record.operation for record in records if record] == [
        "insert",
        "update",
        "delete",
    ]
    assert [record.entity_key for record in records if record] == [
        "doc-a",
        "doc-a",
        "doc-b",
    ]
    assert [record.source_revision for record in records if record] == [
        "rev-0",
        "rev-1",
        "rev-2",
    ]
    assert [record.source_position for record in records if record] == [
        "pos-0000",
        "pos-0001",
        "pos-0002",
    ]


def test_a_version_aware_destination_rejects_a_stale_replayed_write(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    index = FakeVersionedIndex()

    def sync(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        payload: Any = partition.load()
        del context
        index.apply(
            key=payload["key"],
            operation=payload.get("operation", "insert"),
            revision=payload["revision"],
            value=payload.get("value"),
        )
        return payload

    pipeline = Pipeline(
        name="changes",
        stages=[Stage(name="sync", entries=[Step(fn=sync, output=JSONDataset(name="sync"))])],
    )

    def event(index_num: int, key: str, operation: str, value: Any = None) -> ChangeEvent:
        return ChangeEvent(
            position=f"pos-{index_num:04d}",
            entity_key=key,
            operation=operation,  # type: ignore[arg-type]
            revision=f"rev-{index_num}",
            payload_ref=json.dumps(
                {
                    "key": key,
                    "operation": operation,
                    "revision": f"rev-{index_num}",
                    "value": value,
                }
            ),
        )

    adapter = ListChangeAdapter(
        [
            event(0, "doc-a", "insert", "first"),
            event(1, "doc-a", "update", "second"),
            event(2, "doc-b", "insert", "b"),
            event(3, "doc-b", "delete"),
        ]
    )
    result = _run(pipeline, store, output, IncrementalInput(adapter), job_id="range-1")
    assert result.status == "completed"

    assert index.entities["doc-a"] == "second"
    assert "doc-b" in index.deleted
    assert index.rejected == []

    # Replaying an older event must neither overwrite nor resurrect.
    index.apply(key="doc-a", operation="update", revision="rev-0", value="stale")
    index.apply(key="doc-b", operation="insert", revision="rev-2", value="resurrected")
    assert index.entities["doc-a"] == "second"
    assert "doc-b" in index.deleted
    assert index.rejected == [("doc-a", "rev-0"), ("doc-b", "rev-2")]


def test_an_all_input_barrier_operates_over_the_bounded_range_only(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(i, f"doc-{i}") for i in range(5)])
    result = _run(
        _pipeline(with_barrier=True),
        store,
        output,
        IncrementalInput(adapter, max_events=2),
        job_id="range-1",
    )
    assert result.status == "completed"

    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(
        WorkUnitKey("range-1", "stage-002-entry-001", AGGREGATE_PARTITION_ID),
        dataset=JSONDataset(name="summary"),
    ) == ({"changes": 2},)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_incremental_input_validates_its_configuration() -> None:
    adapter = ListChangeAdapter([])
    with pytest.raises(ValueError, match="positive integer"):
        IncrementalInput(adapter, max_events=0)
    with pytest.raises(TypeError, match="incremental adapter"):
        IncrementalInput(object())  # type: ignore[arg-type]


def test_a_change_event_validates_its_fields() -> None:
    with pytest.raises(ValueError, match="operation"):
        ChangeEvent(
            position="p",
            entity_key="k",
            operation="upsert",  # type: ignore[arg-type]
            revision="r",
            payload_ref="{}",
        )
    with pytest.raises(ValueError, match="entity_key"):
        ChangeEvent(position="p", entity_key="", operation="insert", revision="r", payload_ref="{}")


# ---------------------------------------------------------------------------
# Hard-kill replay around range boundaries
# ---------------------------------------------------------------------------

_SCENARIO = """
import os
import sys

sys.path.insert(0, {src!r})

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    AttemptOutputStore,
    ChangeEvent,
    DurableStorage,
    IncrementalInput,
    ListChangeAdapter,
    SQLiteDurableStore,
)
from caravel.durability import commits as commits_module
from caravel.durability import sqlite as sqlite_module
from caravel.loaders import CallableLoader
import json

CRASH_AT = {crash_at!r}


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        os._exit({exit_code})


sqlite_module._failpoint_hook = crash
commits_module._failpoint_hook = crash


def apply_change(inputs, *, context):
    [partition] = inputs.partitions()
    payload = partition.load()
    return {{"applied": payload}}


pipeline = Pipeline(
    name="changes",
    stages=[Stage(name="apply", entries=[Step(fn=apply_change, output=JSONDataset(name="apply"))])],
)

events = [
    ChangeEvent(
        position="pos-%04d" % index,
        entity_key="doc-%d" % index,
        operation="insert",
        revision="rev-%d" % index,
        payload_ref=json.dumps({{"index": index}}),
    )
    for index in range(3)
]

store = SQLiteDurableStore({store_path!r})
result = caravel.run(
    pipeline,
    pipeline_id={pipeline_id!r},
    job_id="range-1",
    source=IncrementalInput(ListChangeAdapter(events), max_events=3),
    storage=DurableStorage(store, AttemptOutputStore({artifacts!r}), restart_safe=True),
)
store.close()
print("STATUS:" + result.status)
"""


def _run_scenario(tmp_path: Path, crash_at: str) -> Any:
    import subprocess
    import sys as _sys
    import textwrap

    script = _SCENARIO.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        crash_at=crash_at,
        exit_code=137,
        store_path=str(tmp_path / "durable.sqlite3"),
        artifacts=str(tmp_path / "artifacts"),
        pipeline_id=PIPELINE_ID,
    )
    return subprocess.run(
        [_sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize(
    "crash_at",
    [
        "after_job_insert",
        "after_discovery_insert",
        "after_discovery_advance",
        "after_batch_progress",
        "after_unit_sealed",
        "after_node_seals",
        "after_job_completed",
        "after_checkpoint_advanced",
    ],
)
def test_a_hard_kill_restarts_with_neither_a_skipped_nor_expanded_range(
    tmp_path: Path, crash_at: str
) -> None:
    killed = _run_scenario(tmp_path, crash_at)
    assert killed.returncode == 137, f"stdout={killed.stdout} stderr={killed.stderr}"

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        job = store.load_job("range-1")
        checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
        if job is not None and job.status == "completed":
            # Completion and checkpoint advancement are one transaction.
            assert checkpoint is not None
            assert checkpoint.completed_position == "pos-0002"
        elif checkpoint is not None:
            assert checkpoint.completed_position is None
        if job is not None:
            # Bounds are never partially written.
            assert (job.range_start, job.range_end) in (
                (None, None),
                ("pos-0000", "pos-0002"),
            )
    finally:
        store.close()

    resumed = _run_scenario(tmp_path, "never")
    assert resumed.returncode == 0, resumed.stderr
    assert "STATUS:completed" in resumed.stdout

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        job = store.load_job("range-1")
        assert job is not None and job.status == "completed"
        assert (job.range_start, job.range_end) == ("pos-0000", "pos-0002")

        checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
        assert checkpoint is not None
        assert checkpoint.completed_position == "pos-0002"

        partitions = store.work_unit_partitions("range-1", LOAD_NODE)
        assert len(partitions) == 3, "range was skipped or expanded"

        publisher = BatchPublisher(store, output)
        for partition_id in partitions:
            key = WorkUnitKey("range-1", LOAD_NODE, partition_id)
            unit = store.load_work_unit(key)
            assert unit is not None and unit.status == "sealed"
            assert len(publisher.committed_references(key)) == 1
    finally:
        store.close()
