"""Regressions for defects D-2 through D-5, found by post-ST-6 code review.

Each test fails against the code as it stood when ST-4 and ST-6 were first
reported complete. The 696-test suite in place at that time caught none of them.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    DurableStorage,
    AttemptOutputStore,
    ChangeEvent,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    FenceViolationError,
    IncrementalInput,
    InMemorySource,
    InvalidTransitionError,
    ListChangeAdapter,
    RetentionGapError,
    SQLiteDurableStore,
    WorkUnitKey,
)
from caravel.retry import RetryOptions

PIPELINE_ID = "customer-42/document-changes"
NODE = "stage-001-entry-001"


def apply_change(inputs, *, context: Any) -> Any:
    [partition] = inputs.partitions()
    payload: Any = partition.load()
    del context
    return {"applied": payload}


def _pipeline(**step_kwargs: Any) -> Pipeline:
    return Pipeline(
        name="changes",
        stages=[
            Stage(
                name="apply",
                entries=[Step(fn=apply_change, output=JSONDataset(name="apply"), **step_kwargs)],
            )
        ],
    )


def _event(index: int) -> ChangeEvent:
    return ChangeEvent(
        position=f"pos-{index:04d}",
        entity_key=f"doc-{index}",
        operation="insert",
        revision=f"rev-{index}",
        payload_ref=json.dumps({"index": index}),
    )


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


# ---------------------------------------------------------------------------
# D-2: a completed job is answered from durable state, never from the source
# ---------------------------------------------------------------------------


def test_reinvoking_a_completed_job_survives_source_retention_expiry(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(index) for index in range(3)])
    first = caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="r1",
        source=IncrementalInput(adapter),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    assert first.status == "completed"

    # The source ages out the range this completed job consumed.
    adapter.expire_through(2)

    again = caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="r1",
        source=IncrementalInput(adapter),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    assert again.status == "completed"
    assert again.committed_batches == 0


def test_an_incomplete_job_still_fails_closed_on_a_retention_gap(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """The D-2 fix must not weaken retention enforcement for unfinished work."""
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
    adapter = ListChangeAdapter([_event(index) for index in range(3)])

    with pytest.raises(RuntimeError):
        caravel.run(
            pipeline,
            pipeline_id=PIPELINE_ID,
            job_id="r1",
            source=IncrementalInput(adapter),
            storage=DurableStorage(store, output, restart_safe=True),
        )
    adapter.expire_through(2)
    with pytest.raises(RetentionGapError):
        caravel.run(
            pipeline,
            pipeline_id=PIPELINE_ID,
            job_id="r1",
            source=IncrementalInput(adapter),
            storage=DurableStorage(store, output, restart_safe=True),
        )


def test_a_completed_job_still_rejects_a_changed_declaration(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Answering from durable state must not skip identity validation."""
    adapter = ListChangeAdapter([_event(0)])
    caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="r1",
        source=IncrementalInput(adapter),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    with pytest.raises(InvalidTransitionError, match="different declaration"):
        caravel.run(
            _pipeline(revision="2"),
            pipeline_id=PIPELINE_ID,
            job_id="r1",
            source=IncrementalInput(adapter),
            storage=DurableStorage(store, output, restart_safe=True),
        )


# ---------------------------------------------------------------------------
# D-3: replay lineage is selectable through the public API
# ---------------------------------------------------------------------------


def test_a_replay_lineage_has_its_own_checkpoint(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(index) for index in range(4)])

    live = caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="live-1",
        source=IncrementalInput(adapter, max_events=2),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    assert live.status == "completed"

    replay = caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="replay-1",
        source=IncrementalInput(adapter, max_events=2),
        storage=DurableStorage(store, output, restart_safe=True),
        lineage="replay-2026-11",
    )
    assert replay.status == "completed"

    default = store.load_checkpoint(PIPELINE_ID, "default")
    replayed = store.load_checkpoint(PIPELINE_ID, "replay-2026-11")
    assert default is not None and replayed is not None
    assert default.completed_position == "pos-0001"
    # The replay lineage starts from its own zero, so it reprocesses the same
    # range instead of resetting the live checkpoint.
    assert replayed.completed_position == "pos-0001"
    assert store.load_job("replay-1").lineage == "replay-2026-11"
    assert store.load_job("live-1").lineage == "default"


def test_a_replay_lineage_does_not_advance_the_live_checkpoint(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    adapter = ListChangeAdapter([_event(index) for index in range(4)])
    caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="live-1",
        source=IncrementalInput(adapter, max_events=4),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    live_before = store.load_checkpoint(PIPELINE_ID, "default")
    assert live_before is not None
    assert live_before.completed_position == "pos-0003"

    caravel.run(
        _pipeline(),
        pipeline_id=PIPELINE_ID,
        job_id="replay-1",
        source=IncrementalInput(adapter, max_events=2),
        storage=DurableStorage(store, output, restart_safe=True),
        lineage="replay-2026-11",
    )
    live_after = store.load_checkpoint(PIPELINE_ID, "default")
    assert live_after == live_before, "a replay must never move the live checkpoint"


# ---------------------------------------------------------------------------
# D-4: a worker presents the fence token it acquired, never a refreshed one
# ---------------------------------------------------------------------------


def _single_unit_job(store: SQLiteDurableStore, clock_job: str = "J") -> WorkUnitKey:
    job = store.initialize_job(
        clock_job, pipeline_id="P", source_mode="snapshot", declaration_fingerprint="f"
    )
    store.register_discovery(
        clock_job,
        expected_generation=job.generation,
        seal=True,
        partitions=[
            DiscoveredPartition(
                partition_id="p1",
                input_ref="src://p1",
                work_units=(
                    DiscoveredWorkUnit(node_id="load", step_revision="r", params_fingerprint="f"),
                ),
            )
        ],
    )
    return WorkUnitKey(clock_job, "load", "p1")


def test_a_fenced_out_worker_cannot_commit_by_reading_a_newer_token(
    tmp_path: Path,
) -> None:
    """The exact escalation the review demonstrated, at the store level."""
    now = dt.datetime(2026, 11, 12, 9, 0, 0, tzinfo=dt.timezone.utc)
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3", clock=lambda: now)
    try:
        key = _single_unit_job(store)
        unit = store.load_work_unit(key)
        assert unit is not None

        # A fixed worker id, as a fixed-size worker pool would use.
        stalled = store.claim_work_unit(
            key, owner_id="worker-1", expected_generation=unit.generation, lease_seconds=30
        )
        now = now + dt.timedelta(seconds=31)
        replacement = store.claim_work_unit(
            key, owner_id="worker-1", expected_generation=stalled.generation, lease_seconds=30
        )
        assert replacement.fence_token > stalled.fence_token

        # Its own token is refused.
        with pytest.raises(FenceViolationError):
            store.commit_batch(
                key,
                owner_id="worker-1",
                fence_token=stalled.fence_token,
                expected_generation=replacement.generation,
                batch_id="b1",
                output_ref="output://stale",
            )
        assert store.lookup_batch(key, "b1") is None
    finally:
        store.close()


def test_a_retry_cannot_adopt_a_newer_token_after_being_fenced_out(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """The exact escalation, through the runner, with a colliding worker id.

    Attempt 1 is displaced by a takeover under the same worker id, then fails.
    Attempt 2 must be refused before its callback runs. The control checks the
    acquired token without adopting the replacement's authority.
    """
    key = WorkUnitKey("r1", NODE, "p1")
    displaced: list[int] = []
    callback_calls: list[int] = []

    def fail_after_being_displaced(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        callback_calls.append(1)
        payload: Any = partition.load()
        del context
        if not displaced:
            held = store.load_work_unit(key)
            assert held is not None
            # A replacement slot with the SAME pool-assigned worker id seizes
            # the unit while this attempt is still running.
            taken = store.claim_work_unit(
                key,
                owner_id="worker-1",
                expected_generation=held.generation,
                lease_seconds=60,
                exclusive=True,
            )
            displaced.append(taken.fence_token)
            raise ConnectionError("vendor down")
        return {"applied": payload}

    pipeline = Pipeline(
        name="changes",
        stages=[
            Stage(
                name="apply",
                entries=[
                    Step(
                        fn=fail_after_being_displaced,
                        output=JSONDataset(name="apply"),
                        retry=RetryOptions(delay_seconds=lambda attempt: 0.0),
                    )
                ],
            )
        ],
    )

    with pytest.raises(FenceViolationError):
        caravel.run(
            pipeline,
            pipeline_id=PIPELINE_ID,
            job_id="r1",
            source=InMemorySource({"p1": 1}),
            storage=DurableStorage(store, output, restart_safe=True),
            worker_id="worker-1",
        )

    assert displaced, "the takeover did not happen; the test proves nothing"
    assert len(callback_calls) == 1, "a displaced worker retried the external operation"
    assert store.lookup_batch(key, "batch-000000") is None, (
        "a fenced-out worker published by adopting the newer token"
    )


# ---------------------------------------------------------------------------
# D-5: reclaimable work agrees with claim_work_unit about lease liveness
# ---------------------------------------------------------------------------


def test_a_claim_without_an_expiry_is_not_offered_as_reclaimable(
    tmp_path: Path,
) -> None:
    """Both predicates must treat a null expiry as a live claim."""
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        key = _single_unit_job(store)
        unit = store.load_work_unit(key)
        assert unit is not None
        held = store.claim_work_unit(
            key, owner_id="worker-a", expected_generation=unit.generation, lease_seconds=None
        )
        assert held.lease_expires_at is None

        # claim_work_unit treats this as live...
        with pytest.raises(FenceViolationError, match="live owner"):
            store.claim_work_unit(key, owner_id="worker-b", expected_generation=held.generation)
        # ...so reclaimable_work must not offer it either.
        assert store.reclaimable_work(key.job_id, limit=10) == ()
        # A sole worker may still adopt it.
        assert [
            item.key for item in store.reclaimable_work(key.job_id, limit=10, include_live=True)
        ] == [key]
    finally:
        store.close()


def test_an_expired_claim_is_still_reclaimable(tmp_path: Path) -> None:
    now = dt.datetime(2026, 11, 12, 9, 0, 0, tzinfo=dt.timezone.utc)
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3", clock=lambda: now)
    try:
        key = _single_unit_job(store)
        unit = store.load_work_unit(key)
        assert unit is not None
        store.claim_work_unit(
            key, owner_id="worker-a", expected_generation=unit.generation, lease_seconds=30
        )
        assert store.reclaimable_work(key.job_id, limit=10) == ()
        now = now + dt.timedelta(seconds=31)
        assert [item.key for item in store.reclaimable_work(key.job_id, limit=10)] == [key]
    finally:
        store.close()


def test_a_failed_unit_remains_reclaimable(tmp_path: Path) -> None:
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        key = _single_unit_job(store)
        unit = store.load_work_unit(key)
        assert unit is not None
        claimed = store.claim_work_unit(
            key, owner_id="worker-a", expected_generation=unit.generation, lease_seconds=3600
        )
        store.fail_work_unit(
            key,
            owner_id="worker-a",
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            error_type="RuntimeError",
        )
        # Owner was released, so it is reclaimable despite a long original lease.
        assert [item.key for item in store.reclaimable_work(key.job_id, limit=10)] == [key]
    finally:
        store.close()
