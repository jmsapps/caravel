"""The committed batch feed: pagination, independent consumers, and retention."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    DurableStorage,
    AttemptOutputStore,
    BatchFeed,
    InMemorySource,
    RetentionExpiredError,
    SQLiteDurableStore,
    WorkUnitKey,
)
from caravel.retry import RetryPolicy

PIPELINE_ID = "customer-42/documents"
JOB = "snapshot-2026-11-12-v7"
NODE = "stage-001-entry-001"


def publish(payload: Any, *, context: Any) -> Any:
    del context
    return {"published": payload}


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


def _pipeline(**step_kwargs: Any) -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[
            Stage(
                name="publish",
                entries=[Step(fn=publish, output=JSONDataset(name="publish"), **step_kwargs)],
            )
        ],
    )


def _run(store: Any, output: Any, partitions: dict[str, Any], **kwargs: Any) -> Any:
    return caravel.run(
        _pipeline(**kwargs.pop("step_kwargs", {})),
        pipeline_id=PIPELINE_ID,
        job_id=JOB,
        source=InMemorySource(partitions),
        storage=DurableStorage(store, output, restart_safe=True),
        **kwargs,
    )


def _feed(store: SQLiteDurableStore, output: AttemptOutputStore) -> BatchFeed:
    return BatchFeed(store, pipeline_id=PIPELINE_ID, job_id=JOB, node_id=NODE, output=output)


# ---------------------------------------------------------------------------
# Ordered, bounded pagination (coverage 8)
# ---------------------------------------------------------------------------


def test_the_feed_lists_committed_references_in_order(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index}": index for index in range(5)})
    page = _feed(store, output).read()

    assert len(page.entries) == 5
    assert [entry.position for entry in page.entries] == sorted(
        entry.position for entry in page.entries
    )
    assert [entry.partition_id for entry in page.entries] == [f"p{i}" for i in range(5)]
    assert page.job_completed is True
    assert page.node_sealed is True


def test_pagination_is_bounded_and_resumable(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index:02d}": index for index in range(10)})
    feed = _feed(store, output)

    seen: list[str] = []
    position = 0
    while True:
        page = feed.read(after_position=position, limit=3)
        if page.is_empty:
            break
        assert len(page.entries) <= 3
        seen.extend(entry.partition_id for entry in page.entries)
        position = page.next_position

    assert seen == [f"p{index:02d}" for index in range(10)]


def test_feed_entries_load_their_committed_payload(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 7})
    feed = _feed(store, output)
    entry = feed.read().entries[0]
    assert feed.load(entry, dataset=JSONDataset(name="publish")) == {"published": 7}


def test_an_empty_ongoing_feed_differs_from_a_sealed_empty_result(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
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
    )
    register_plan_discovery(store, job, plan, InMemorySource({"p1": 1}).discover())

    ongoing = _feed(store, output).read()
    assert ongoing.is_empty is True
    assert ongoing.node_sealed is False
    assert ongoing.job_completed is False
    assert ongoing.at_end is False, "an empty ongoing feed is not end of input"

    _run(store, output, {"p1": 1})
    sealed = _feed(store, output).read(after_position=10_000)
    assert sealed.is_empty is True
    assert sealed.node_sealed is True
    assert sealed.at_end is True


def test_a_sealed_empty_job_publishes_a_discoverable_completion_marker(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    result = _run(store, output, {})
    assert result.status == "completed"
    page = _feed(store, output).read()
    assert page.is_empty is True
    assert page.job_completed is True


def test_completion_markers_identify_final_positions(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 1, "p2": 2})
    seals = {seal.node_id: seal for seal in store.node_seals(JOB)}
    assert NODE in seals
    final = seals[NODE].final_position
    assert final is not None

    page = _feed(store, output).read()
    assert page.final_position == final
    assert page.entries[-1].position == final


def test_completion_markers_survive_a_store_reopen(tmp_path: Path) -> None:
    store_path = tmp_path / "durable.sqlite3"
    artifacts = tmp_path / "artifacts"
    opened = SQLiteDurableStore(store_path)
    try:
        _run(opened, AttemptOutputStore(artifacts), {"p1": 1})
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        page = BatchFeed(
            reopened,
            pipeline_id=PIPELINE_ID,
            job_id=JOB,
            node_id=NODE,
            output=AttemptOutputStore(artifacts),
        ).read()
        assert page.job_completed is True
        assert page.node_sealed is True
        assert page.final_position is not None
    finally:
        reopened.close()


# ---------------------------------------------------------------------------
# Independent consumers (coverage 10)
# ---------------------------------------------------------------------------


def test_independent_consumers_keep_their_own_positions(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index}": index for index in range(4)})
    feed = _feed(store, output)

    fast = feed.read_for("indexer", limit=4)
    feed.acknowledge("indexer", fast.next_position)
    slow = feed.read_for("archiver", limit=2)
    feed.acknowledge("archiver", slow.next_position)

    assert feed.read_for("indexer").is_empty is True
    assert len(feed.read_for("archiver").entries) == 2
    assert feed.position_of("indexer") > feed.position_of("archiver")


def test_a_consumer_acknowledgment_never_changes_producer_progress(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 1})
    before = store.load_work_unit(WorkUnitKey(JOB, NODE, "p1"))
    assert before is not None

    feed = _feed(store, output)
    page = feed.read_for("indexer")
    feed.acknowledge("indexer", page.next_position)

    after = store.load_work_unit(WorkUnitKey(JOB, NODE, "p1"))
    assert after == before


def test_a_duplicate_acknowledgment_never_moves_a_position_backwards(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 1, "p2": 2})
    feed = _feed(store, output)
    page = feed.read_for("indexer")
    advanced = feed.acknowledge("indexer", page.next_position)

    assert feed.acknowledge("indexer", 0) == advanced
    assert feed.acknowledge("indexer", advanced) == advanced
    assert feed.read_for("indexer").is_empty is True


def test_a_crash_between_application_and_acknowledgment_replays_that_batch(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """The destination sees the batch twice; idempotency makes that safe."""
    _run(store, output, {"p1": 1, "p2": 2})
    feed = _feed(store, output)
    applied: list[str] = []
    destination: dict[str, Any] = {}

    def consume_one() -> bool:
        page = feed.read_for("indexer", limit=1)
        if page.is_empty:
            return False
        entry = page.entries[0]
        payload = feed.load(entry, dataset=JSONDataset(name="publish"))
        applied.append(entry.partition_id)
        destination[entry.partition_id] = payload  # idempotent by stable key
        return True

    # Apply the first batch, then die before acknowledging.
    assert consume_one() is True
    assert feed.position_of("indexer") == 0

    # A new consumer process replays the same batch and then acknowledges.
    assert consume_one() is True
    page = feed.read_for("indexer", limit=1)
    feed.acknowledge("indexer", page.entries[0].position)

    assert applied == ["p1", "p1"], "the unacknowledged batch replayed"
    assert destination == {"p1": {"published": 1}}, "idempotent application is unchanged"

    remaining = feed.read_for("indexer")
    assert [entry.partition_id for entry in remaining.entries] == ["p2"]


def test_a_missed_notification_loses_nothing_because_listing_is_authoritative(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index}": index for index in range(3)})
    feed = _feed(store, output)

    # No notification is ever delivered; the consumer simply lists from its position.
    collected: list[str] = []
    while True:
        page = feed.read_for("indexer", limit=1)
        if page.is_empty:
            break
        collected.append(page.entries[0].partition_id)
        feed.acknowledge("indexer", page.entries[0].position)

    assert collected == ["p0", "p1", "p2"]


def test_one_consumers_failure_does_not_block_another(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 1, "p2": 2})
    feed = _feed(store, output)

    # The archiver never acknowledges anything.
    assert len(feed.read_for("archiver").entries) == 2

    while True:
        page = feed.read_for("indexer", limit=1)
        if page.is_empty:
            break
        feed.acknowledge("indexer", page.entries[0].position)

    assert feed.read_for("indexer").is_empty is True
    assert len(feed.read_for("archiver").entries) == 2


# ---------------------------------------------------------------------------
# Retention (coverage 11)
# ---------------------------------------------------------------------------


def test_reading_below_the_retention_floor_raises_explicitly(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index}": index for index in range(4)})
    feed = _feed(store, output)
    entries = feed.read().entries
    floor = entries[2].position
    store.set_retention_floor(JOB, floor_position=floor)

    with pytest.raises(RetentionExpiredError, match="below the retention floor"):
        feed.read(after_position=0)

    # A consumer already past the floor is unaffected.
    page = feed.read(after_position=floor)
    assert [entry.partition_id for entry in page.entries] == ["p3"]


def test_retention_expiry_is_explicit_to_a_lagging_consumer(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {f"p{index}": index for index in range(3)})
    feed = _feed(store, output)
    entries = feed.read().entries
    feed.acknowledge("fast", entries[-1].position)
    store.set_retention_floor(JOB, floor_position=entries[-1].position)

    assert feed.read_for("fast").is_empty is True
    with pytest.raises(RetentionExpiredError):
        feed.read_for("lagging")


def test_the_default_retention_floor_admits_every_consumer(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    _run(store, output, {"p1": 1})
    feed = _feed(store, output)
    assert feed.retention_floor() == 0
    assert len(feed.read(after_position=0).entries) == 1


# ---------------------------------------------------------------------------
# Retry at the step boundary over durable batches (coverage 7)
# ---------------------------------------------------------------------------


def test_a_step_retry_preserves_already_committed_batches(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    attempts = {"n": 0}

    def flaky(records: Any, *, context: Any) -> Any:
        del context
        attempts["n"] += 1
        # Fail once while processing the third batch.
        if records == [4, 5] and attempts["n"] == 3:
            raise ConnectionError("vendor down")
        return [value * 10 for value in records]

    policy = RetryPolicy(jitter=lambda low, high: 0.0, sleep=lambda seconds: None)
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="publish",
                entries=[
                    Step(
                        fn=flaky,
                        output=JSONDataset(name="publish"),
                        batch_size=2,
                        retry=policy,
                    )
                ],
            )
        ],
    )
    result = caravel.run(
        pipeline,
        pipeline_id=PIPELINE_ID,
        job_id=JOB,
        source=InMemorySource({"p1": [0, 1, 2, 3, 4, 5]}),
        storage=DurableStorage(store, output, restart_safe=True),
    )
    assert result.status == "completed"

    key = WorkUnitKey(JOB, NODE, "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    # Three batches committed exactly once each, despite the retried attempt.
    assert unit.batch_count == 3

    from caravel.durability import BatchPublisher

    assert BatchPublisher(store, output).load_work_unit(
        key, dataset=JSONDataset(name="publish")
    ) == ([0, 10], [20, 30], [40, 50])


def test_retry_exhaustion_leaves_the_batch_cursor_uncommitted(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def always_fails(records: Any, *, context: Any) -> Any:
        del context
        raise ConnectionError("vendor down")

    policy = RetryPolicy(max_retries=2, jitter=lambda low, high: 0.0, sleep=lambda seconds: None)
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="publish",
                entries=[
                    Step(
                        fn=always_fails,
                        output=JSONDataset(name="publish"),
                        batch_size=1,
                        retry=policy,
                    )
                ],
            )
        ],
    )
    with pytest.raises(ConnectionError):
        caravel.run(
            pipeline,
            pipeline_id=PIPELINE_ID,
            job_id=JOB,
            source=InMemorySource({"p1": [1, 2]}),
            storage=DurableStorage(store, output, restart_safe=True),
        )

    unit = store.load_work_unit(WorkUnitKey(JOB, NODE, "p1"))
    assert unit is not None
    assert unit.status == "failed"
    assert unit.cursor is None
    assert unit.batch_count == 0


def test_a_step_without_a_policy_acquires_no_retries(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    attempts = {"n": 0}

    def flaky(payload: Any, *, context: Any) -> Any:
        del context
        attempts["n"] += 1
        raise ConnectionError("vendor down")

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="publish", entries=[Step(fn=flaky, output=JSONDataset(name="publish"))])
        ],
    )
    with pytest.raises(ConnectionError):
        caravel.run(
            pipeline,
            pipeline_id=PIPELINE_ID,
            job_id=JOB,
            source=InMemorySource({"p1": 1}),
            storage=DurableStorage(store, output, restart_safe=True),
        )
    assert attempts["n"] == 1


def test_step_retry_must_be_a_retry_policy() -> None:
    with pytest.raises(TypeError, match="RetryPolicy"):
        Step(fn=publish, output=JSONDataset(name="x"), retry="aggressive")
