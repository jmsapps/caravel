"""Durable publication tests: immutable attempts, manifests, and reconciliation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

from caravel.datasets import (
    JSONDataset,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    TextDataset,
)
from caravel.durability import (
    AttemptOutputStore,
    BatchPublisher,
    CommitUncertainError,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    FenceViolationError,
    OutputIntegrityError,
    OutputReference,
    SQLiteDurableStore,
    WorkUnitKey,
)
from caravel.durability import commits as commits_module
from caravel.durability.output import encode_segment

JOB = "job-1"
PIPELINE = "customer-42/documents"
OWNER = "worker-a"


class _InterruptedError(RuntimeError):
    """Raised by armed failpoints to simulate an interrupted process."""


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


@pytest.fixture()
def publisher(store: SQLiteDurableStore, output: AttemptOutputStore) -> BatchPublisher:
    return BatchPublisher(store, output)


def _partition(partition_id: str, *nodes: str) -> DiscoveredPartition:
    return DiscoveredPartition(
        partition_id=partition_id,
        input_ref=f"source://{partition_id}",
        work_units=tuple(
            DiscoveredWorkUnit(
                node_id=node_id,
                step_revision="rev-1",
                params_fingerprint="params-1",
                depends_on_nodes=(nodes[index - 1],) if index > 0 else (),
            )
            for index, node_id in enumerate(nodes)
        ),
    )


def _prepare(store: SQLiteDurableStore, *partitions: DiscoveredPartition) -> None:
    job = store.initialize_job(
        JOB,
        pipeline_id=PIPELINE,
        source_mode="snapshot",
        declaration_fingerprint="decl-v1",
        snapshot_id="v7",
    )
    store.register_discovery(JOB, expected_generation=job.generation, partitions=list(partitions))


def _only_file(directory: str) -> Path:
    """Return the single file a single-file dataset wrote under an attempt path."""
    files = sorted(path for path in Path(directory).iterdir() if path.is_file())
    assert len(files) == 1, f"expected one output file under {directory}, found {files}"
    return files[0]


def _claim(store: SQLiteDurableStore, key: WorkUnitKey) -> Any:
    unit = store.load_work_unit(key)
    assert unit is not None
    return store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)


# ---------------------------------------------------------------------------
# Round trips across dataset types (coverage 1 and 2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dataset,payload",
    [
        (JSONDataset(name="docs"), {"a": 1, "b": [2, 3]}),
        (TextDataset(name="docs"), "hello durable world"),
        (PartitionedJSONDataset(name="docs"), {"k1": {"v": 1}, "k2": {"v": 2}}),
        (PartitionedTextDataset(name="docs"), {"k1": "one", "k2": "two"}),
    ],
)
def test_payloads_round_trip_through_publication_and_reopen(
    tmp_path: Path, dataset: Any, payload: Any
) -> None:
    store_path = tmp_path / "durable.sqlite3"
    artifacts = tmp_path / "artifacts"
    key = WorkUnitKey(JOB, "load", "p1")

    opened = SQLiteDurableStore(store_path)
    try:
        _prepare(opened, _partition("p1", "load"))
        claimed = _claim(opened, key)
        BatchPublisher(opened, AttemptOutputStore(artifacts)).publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=dataset,
            payload=payload,
            input_exhausted=True,
        )
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        reader = BatchPublisher(reopened, AttemptOutputStore(artifacts))
        assert reader.load_batch(key, "b1", dataset=dataset) == payload
        assert reader.load_work_unit(key, dataset=dataset) == (payload,)
    finally:
        reopened.close()


def test_a_committed_empty_partitioned_payload_reloads_from_its_manifest(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "durable.sqlite3"
    artifacts = tmp_path / "artifacts"
    dataset = PartitionedJSONDataset(name="docs", allow_empty=True)
    key = WorkUnitKey(JOB, "load", "p1")

    opened = SQLiteDurableStore(store_path)
    try:
        _prepare(opened, _partition("p1", "load"))
        claimed = _claim(opened, key)
        commit = BatchPublisher(opened, AttemptOutputStore(artifacts)).publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=dataset,
            payload={},
            input_exhausted=True,
        )
        assert commit.reference.committed_empty is True
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        reader = BatchPublisher(reopened, AttemptOutputStore(artifacts))
        assert reader.load_batch(key, "b1", dataset=dataset) == {}
    finally:
        reopened.close()


def test_multiple_batches_load_in_commit_order(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    state = claimed
    for index in range(3):
        commit = publisher.publish(
            key,
            owner_id=OWNER,
            fence_token=state.fence_token,
            expected_generation=state.generation,
            batch_id=f"b{index}",
            dataset=dataset,
            payload={"index": index},
            next_cursor=f"cursor-{index}",
        )
        state = commit.state
    assert publisher.load_work_unit(key, dataset=dataset) == (
        {"index": 0},
        {"index": 1},
        {"index": 2},
    )
    assert state.cursor == "cursor-2"
    assert state.batch_count == 3


# ---------------------------------------------------------------------------
# Declaration validation and integrity (coverage 3 and 4)
# ---------------------------------------------------------------------------


def test_loading_rejects_a_changed_dataset_declaration(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=JSONDataset(name="docs"),
        payload={"a": 1},
    )
    with pytest.raises(OutputIntegrityError, match="current declaration"):
        publisher.load_batch(key, "b1", dataset=TextDataset(name="docs"))
    with pytest.raises(OutputIntegrityError, match="current declaration"):
        publisher.load_batch(key, "b1", dataset=JSONDataset(name="renamed"))


def test_missing_committed_output_fails_explicitly(
    store: SQLiteDurableStore, publisher: BatchPublisher, output: AttemptOutputStore
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"a": 1},
    )

    _only_file(commit.reference.path).unlink()
    with pytest.raises(OutputIntegrityError, match="missing or does not match"):
        publisher.load_batch(key, "b1", dataset=dataset)


def test_corrupt_committed_output_fails_explicitly(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"a": 1},
    )

    _only_file(commit.reference.path).write_text("{not json", encoding="utf-8")
    with pytest.raises(OutputIntegrityError, match="could not be loaded"):
        publisher.load_batch(key, "b1", dataset=dataset)


def test_a_partitioned_payload_missing_one_key_fails_explicitly(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    dataset = PartitionedJSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"k1": {"v": 1}, "k2": {"v": 2}},
    )
    Path(commit.reference.path, "k2.json").unlink()
    with pytest.raises(OutputIntegrityError, match="missing or does not match"):
        publisher.load_batch(key, "b1", dataset=dataset)


def test_an_uncommitted_batch_has_no_loadable_output(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    assert publisher.committed_reference(key, "b1") is None
    with pytest.raises(OutputIntegrityError, match="no committed output reference"):
        publisher.load_batch(key, "b1", dataset=JSONDataset(name="docs"))


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "{}",
        '{"schema_version": 2, "path": "p", "dataset": {"type": "JSONDataset", '
        '"name": "d"}, "partition_keys": null, "count": 1}',
        '{"schema_version": 1, "path": "p", "dataset": {"type": "JSONDataset"}, '
        '"partition_keys": null, "count": 1}',
        '{"schema_version": 1, "path": "p", "dataset": {"type": "JSONDataset", '
        '"name": "d"}, "partition_keys": null, "count": 4}',
    ],
)
def test_a_malformed_output_reference_is_rejected(payload: str) -> None:
    with pytest.raises(OutputIntegrityError):
        OutputReference.from_json(payload)


def test_an_output_reference_round_trips_through_json() -> None:
    reference = OutputReference(
        path="/tmp/attempt",
        dataset_type="PartitionedJSONDataset",
        dataset_name="docs",
        partition_keys=["k1", "k2"],
        count=2,
    )
    assert OutputReference.from_json(reference.to_json()) == reference


# ---------------------------------------------------------------------------
# Isolation and identity collisions (coverage 5 and 6)
# ---------------------------------------------------------------------------


def test_replacement_in_one_work_unit_preserves_another(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    dataset = PartitionedJSONDataset(name="docs")
    _prepare(store, _partition("p1", "load"), _partition("p2", "load"))

    commits = {}
    for partition_id in ("p1", "p2"):
        key = WorkUnitKey(JOB, "load", partition_id)
        claimed = _claim(store, key)
        commits[partition_id] = publisher.publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=dataset,
            payload={partition_id: {"v": 1}},
        )

    assert commits["p1"].reference.path != commits["p2"].reference.path

    # A second attempt for p1 replaces only its own isolated attempt location.
    key = WorkUnitKey(JOB, "load", "p1")
    publisher.output.write_attempt(key, "b2", dataset=dataset, payload={"replacement": {"v": 9}})
    assert publisher.load_batch(WorkUnitKey(JOB, "load", "p2"), "b1", dataset=dataset) == {
        "p2": {"v": 1}
    }
    assert publisher.load_batch(key, "b1", dataset=dataset) == {"p1": {"v": 1}}


def test_execution_identities_that_would_collide_resolve_distinctly(
    output: AttemptOutputStore,
) -> None:
    nested = output.attempt_location(WorkUnitKey(JOB, "load", "a/b"), "b1", "t1")
    split = output.attempt_location(WorkUnitKey(JOB, "load/a", "b"), "b1", "t1")
    assert nested != split
    assert "a/b" not in nested
    assert "a%2Fb" in nested


@pytest.mark.parametrize("partition_id", ["../../escape", "..", ".", "a/../b"])
def test_an_execution_identity_can_never_traverse_upward(
    output: AttemptOutputStore, partition_id: str
) -> None:
    location = output.attempt_location(WorkUnitKey(JOB, "load", partition_id), "b1", "t1")
    assert location.startswith(str(output.root))
    segments = location[len(str(output.root)) :].split("/")
    assert ".." not in segments
    assert "." not in segments
    assert Path(location).resolve().is_relative_to(Path(str(output.root)).resolve())


def test_relative_path_identities_encode_to_distinct_safe_segments() -> None:
    assert encode_segment("..") not in (".", "..")
    assert encode_segment(".") not in (".", "..")
    assert encode_segment(".") != encode_segment("..")


def test_a_payload_storage_key_cannot_collide_with_an_execution_partition(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    """A storage key inside a payload lives under its own execution partition."""
    dataset = PartitionedJSONDataset(name="docs")
    _prepare(store, _partition("p1", "load"), _partition("k1", "load"))

    for partition_id, payload in (("p1", {"k1": {"from": "p1"}}), ("k1", {"k1": {"from": "k1"}})):
        key = WorkUnitKey(JOB, "load", partition_id)
        claimed = _claim(store, key)
        publisher.publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=dataset,
            payload=payload,
        )

    assert publisher.load_batch(WorkUnitKey(JOB, "load", "p1"), "b1", dataset=dataset) == {
        "k1": {"from": "p1"}
    }
    assert publisher.load_batch(WorkUnitKey(JOB, "load", "k1"), "b1", dataset=dataset) == {
        "k1": {"from": "k1"}
    }


def test_long_identities_encode_to_bounded_distinct_segments() -> None:
    first = encode_segment("x" * 400 + "-one")
    second = encode_segment("x" * 400 + "-two")
    assert first != second
    assert len(first) <= 64
    assert len(second) <= 64


def test_each_attempt_gets_a_fresh_isolated_location(
    output: AttemptOutputStore,
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    first = output.write_attempt(key, "b1", dataset=dataset, payload={"attempt": 1})
    second = output.write_attempt(key, "b1", dataset=dataset, payload={"attempt": 2})
    assert first.path != second.path
    assert output.load(first, dataset=dataset) == {"attempt": 1}
    assert output.load(second, dataset=dataset) == {"attempt": 2}


# ---------------------------------------------------------------------------
# Lost acknowledgments and stale writers (coverage 7, 8, 9)
# ---------------------------------------------------------------------------


def test_a_lost_acknowledgment_with_a_visible_commit_continues_from_it(
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)

    real_commit = store.commit_batch

    def commit_then_lose_the_acknowledgment(*args: Any, **kwargs: Any) -> Any:
        real_commit(*args, **kwargs)
        raise ConnectionResetError("acknowledgment lost")

    monkeypatch.setattr(store, "commit_batch", commit_then_lose_the_acknowledgment)
    commit = BatchPublisher(store, output).publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"a": 1},
    )
    assert commit.reconciled is True
    assert commit.state.batch_count == 1
    monkeypatch.undo()
    assert BatchPublisher(store, output).load_batch(key, "b1", dataset=dataset) == {"a": 1}


def test_a_failed_publication_with_no_commit_reraises(
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)

    def never_commits(*args: Any, **kwargs: Any) -> Any:
        raise ConnectionResetError("publication failed")

    monkeypatch.setattr(store, "commit_batch", never_commits)
    with pytest.raises(ConnectionResetError):
        BatchPublisher(store, output).publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=JSONDataset(name="docs"),
            payload={"a": 1},
        )
    monkeypatch.undo()
    assert store.lookup_batch(key, "b1") is None
    unit = store.load_work_unit(key)
    assert unit is not None and unit.batch_count == 0


def test_an_unreadable_outcome_during_reconciliation_raises_commit_uncertain(
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    calls = {"lookup": 0}

    def never_commits(*args: Any, **kwargs: Any) -> Any:
        raise ConnectionResetError("publication failed")

    def lookup(*args: Any, **kwargs: Any) -> Any:
        calls["lookup"] += 1
        if calls["lookup"] == 1:
            return None
        raise OSError("authoritative state unreadable")

    monkeypatch.setattr(store, "commit_batch", never_commits)
    monkeypatch.setattr(store, "lookup_batch", lookup)
    with pytest.raises(CommitUncertainError, match="could not be established"):
        BatchPublisher(store, output).publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=JSONDataset(name="docs"),
            payload={"a": 1},
        )
    monkeypatch.undo()
    unit = store.load_work_unit(key)
    assert unit is not None and unit.batch_count == 0


def test_a_concurrent_commit_of_a_different_reference_is_uncertain(
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A competing writer wins the batch while this attempt's ack is lost."""
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    rival = output.write_attempt(key, "b1", dataset=dataset, payload={"rival": True})
    real_commit = store.commit_batch

    def commit_the_rival_then_lose_the_acknowledgment(*args: Any, **kwargs: Any) -> Any:
        rewritten = dict(kwargs)
        rewritten["output_ref"] = rival.to_json()
        real_commit(*args, **rewritten)
        raise ConnectionResetError("acknowledgment lost")

    monkeypatch.setattr(store, "commit_batch", commit_the_rival_then_lose_the_acknowledgment)
    with pytest.raises(CommitUncertainError, match="different output reference"):
        BatchPublisher(store, output).publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=dataset,
            payload={"mine": True},
        )
    monkeypatch.undo()
    assert BatchPublisher(store, output).load_batch(key, "b1", dataset=dataset) == {"rival": True}


def test_republishing_a_committed_batch_continues_from_the_committed_record(
    store: SQLiteDurableStore, publisher: BatchPublisher, output: AttemptOutputStore
) -> None:
    """A replayed batch never writes a second attempt or a second manifest entry."""
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)
    first = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"attempt": 1},
    )
    assert first.reconciled is False

    replayed = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"attempt": 2},
    )
    assert replayed.reconciled is True
    assert replayed.reference == first.reference
    assert replayed.state.batch_count == 1
    assert publisher.load_batch(key, "b1", dataset=dataset) == {"attempt": 1}
    assert len(publisher.committed_references(key)) == 1


def test_an_attempt_written_but_never_published_is_invisible(
    store: SQLiteDurableStore, publisher: BatchPublisher, output: AttemptOutputStore
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))

    orphan = output.write_attempt(key, "b1", dataset=dataset, payload={"orphan": True})
    assert output.exists(orphan) is True
    assert publisher.committed_reference(key, "b1") is None
    assert publisher.committed_references(key) == ()
    assert publisher.load_work_unit(key, dataset=dataset) == ()

    claimed = _claim(store, key)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"committed": True},
    )
    assert commit.reference.path != orphan.path
    assert publisher.load_batch(key, "b1", dataset=dataset) == {"committed": True}
    assert output.exists(orphan) is True


def test_a_stale_fenced_writer_cannot_publish_after_takeover(
    store: SQLiteDurableStore, publisher: BatchPublisher, output: AttemptOutputStore
) -> None:
    dataset = JSONDataset(name="docs")
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    stale = _claim(store, key)
    fresh = store.claim_work_unit(key, owner_id=OWNER, expected_generation=stale.generation)
    assert fresh.fence_token > stale.fence_token

    with pytest.raises(FenceViolationError):
        publisher.publish(
            key,
            owner_id=OWNER,
            fence_token=stale.fence_token,
            expected_generation=fresh.generation,
            batch_id="b1",
            dataset=dataset,
            payload={"stale": True},
        )
    assert publisher.committed_references(key) == ()

    winner = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=fresh.fence_token,
        expected_generation=fresh.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"winner": True},
    )
    assert publisher.load_batch(key, "b1", dataset=dataset) == {"winner": True}
    assert output.exists(winner.reference) is True


def test_an_interrupted_attempt_write_publishes_nothing(
    store: SQLiteDurableStore,
    publisher: BatchPublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = WorkUnitKey(JOB, "load", "p1")
    _prepare(store, _partition("p1", "load"))
    claimed = _claim(store, key)

    def hook(name: str) -> None:
        if name == "after_attempt_write":
            raise _InterruptedError(name)

    monkeypatch.setattr(commits_module, "_failpoint_hook", hook)
    with pytest.raises(_InterruptedError):
        publisher.publish(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            dataset=JSONDataset(name="docs"),
            payload={"a": 1},
        )
    monkeypatch.setattr(commits_module, "_failpoint_hook", None)

    assert store.lookup_batch(key, "b1") is None
    unit = store.load_work_unit(key)
    assert unit is not None
    assert unit.batch_count == 0
    assert unit.generation == claimed.generation


def test_publication_sealing_and_dependent_eligibility_agree(
    store: SQLiteDurableStore, publisher: BatchPublisher
) -> None:
    dataset = JSONDataset(name="docs")
    _prepare(store, _partition("p1", "load", "enrich"))
    key = WorkUnitKey(JOB, "load", "p1")
    claimed = _claim(store, key)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={"a": 1},
        input_exhausted=True,
    )
    assert store.eligible_work(JOB, limit=10) == ()

    store.seal_work_unit(
        key,
        owner_id=OWNER,
        fence_token=commit.state.fence_token,
        expected_generation=commit.state.generation,
    )
    eligible = store.eligible_work(JOB, limit=10)
    assert [unit.key.node_id for unit in eligible] == ["enrich"]

    sealed = store.load_work_unit(key)
    assert sealed is not None
    assert sealed.status == "sealed"
    assert sealed.owner_id is None
    assert sealed.batch_count == len(publisher.committed_references(key))
    assert publisher.load_work_unit(key, dataset=dataset) == ({"a": 1},)
