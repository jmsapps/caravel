"""Contract tests for the C-8 authoritative durable execution store.

Every test runs against a real SQLite store on disk. Memory fakes would not
establish durability, so none are used here.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pytest

from caravel.durability import sqlite as sqlite_module
from caravel.durability import (
    DURABLE_SCHEMA_VERSION,
    DependencyEdge,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    DurableStore,
    FenceViolationError,
    GenerationConflictError,
    InvalidTransitionError,
    SQLiteDurableStore,
    StorageCapabilityError,
    StoreCapabilities,
    StoreUnavailableError,
    UnsupportedStoreVersionError,
    WorkUnitKey,
    validate_capabilities,
)

JOB = "job-1"
PIPELINE = "customer-42/documents"
OWNER = "worker-a"
FINGERPRINT = "decl-v1"


class _InterruptedError(RuntimeError):
    """Raised by armed failpoints to simulate an interrupted process."""


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InterruptedError(name)

    monkeypatch.setattr(sqlite_module, "_failpoint_hook", hook)


def _disarm_failpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqlite_module, "_failpoint_hook", None)


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "durable.sqlite3"


@pytest.fixture()
def store(store_path: Path) -> Iterator[SQLiteDurableStore]:
    opened = SQLiteDurableStore(store_path)
    try:
        yield opened
    finally:
        opened.close()


def _partition(partition_id: str, *nodes: str, depends: bool = True) -> DiscoveredPartition:
    units: list[DiscoveredWorkUnit] = []
    for index, node_id in enumerate(nodes):
        upstream = (nodes[index - 1],) if depends and index > 0 else ()
        units.append(
            DiscoveredWorkUnit(
                node_id=node_id,
                step_revision="rev-1",
                params_fingerprint="params-1",
                depends_on_nodes=upstream,
            )
        )
    return DiscoveredPartition(
        partition_id=partition_id,
        input_ref=f"source://{partition_id}",
        work_units=tuple(units),
    )


def _open_job(store: SQLiteDurableStore, *, mode: str = "snapshot") -> Any:
    if mode == "snapshot":
        return store.initialize_job(
            JOB,
            pipeline_id=PIPELINE,
            source_mode="snapshot",
            declaration_fingerprint=FINGERPRINT,
            snapshot_id="v7",
        )
    return store.initialize_job(
        JOB,
        pipeline_id=PIPELINE,
        source_mode="incremental",
        declaration_fingerprint=FINGERPRINT,
        range_start="pos-0",
        range_end="pos-100",
    )


def _drive_unit_to_sealed(
    store: SQLiteDurableStore, key: WorkUnitKey, *, batch_id: str = "b1"
) -> None:
    """Claim, publish one batch, and seal one work unit."""
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    committed = store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id=batch_id,
        output_ref=f"output://{key.node_id}/{key.partition_id}/{batch_id}",
        input_exhausted=True,
    )
    store.seal_work_unit(
        key,
        owner_id=OWNER,
        fence_token=committed.fence_token,
        expected_generation=committed.generation,
    )


# ---------------------------------------------------------------------------
# Capability validation (required coverage 7) and schema version (8)
# ---------------------------------------------------------------------------


class _DegradedStore(SQLiteDurableStore):
    """A store whose probe reports an incomplete durability guarantee."""

    def _probe_capabilities(self) -> StoreCapabilities:
        return StoreCapabilities(
            atomic_conditional_transitions=True,
            generation_validation=True,
            fencing_validation=True,
            durable_reads=False,
            transactional_publication=True,
        )


def test_sqlite_store_declares_every_required_capability(store: SQLiteDurableStore) -> None:
    assert store.capabilities.missing() == ()


def test_validate_capabilities_rejects_a_backend_without_guarantees() -> None:
    with pytest.raises(StorageCapabilityError) as excinfo:
        validate_capabilities(StoreCapabilities(), backend="fsspec")
    message = str(excinfo.value)
    assert "fsspec" in message
    assert "atomic_conditional_transitions" in message
    assert "durable_reads" in message


def test_degraded_capabilities_reject_before_any_durable_mutation(store_path: Path) -> None:
    with pytest.raises(StorageCapabilityError) as excinfo:
        _DegradedStore(store_path)
    assert "durable_reads" in str(excinfo.value)

    connection = sqlite3.connect(store_path)
    try:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        connection.close()
    assert tables == []


def test_unsupported_schema_version_is_rejected(store_path: Path) -> None:
    opened = SQLiteDurableStore(store_path)
    opened.close()

    connection = sqlite3.connect(store_path)
    try:
        connection.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
            (str(DURABLE_SCHEMA_VERSION + 1),),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(UnsupportedStoreVersionError):
        SQLiteDurableStore(store_path)


def test_wal_is_refused_on_builds_without_the_wal_reset_fix(store_path: Path) -> None:
    if sqlite_module._wal_is_fixed(sqlite3.sqlite_version_info):
        opened = SQLiteDurableStore(store_path, wal=True)
        try:
            assert opened.capabilities.missing() == ()
        finally:
            opened.close()
        return
    with pytest.raises(ValueError, match="WAL-reset fix"):
        SQLiteDurableStore(store_path, wal=True)


def test_store_timeout_must_be_a_bounded_positive_number(store_path: Path) -> None:
    for bad in (0, -1, 120, float("inf")):
        with pytest.raises(ValueError, match="timeout"):
            SQLiteDurableStore(store_path, timeout=bad)


def test_sqlite_store_satisfies_the_durable_store_protocol(store: SQLiteDurableStore) -> None:
    assert isinstance(store, DurableStore)


# ---------------------------------------------------------------------------
# Happy-path transitions and durable reopen (required coverage 1)
# ---------------------------------------------------------------------------


def test_initialize_job_is_idempotent_for_an_identical_declaration(
    store: SQLiteDurableStore,
) -> None:
    first = _open_job(store)
    second = _open_job(store)
    assert first == second
    assert first.generation == 0
    assert store.load_checkpoint(PIPELINE, "default") is not None


def test_initialize_job_rejects_a_changed_declaration(store: SQLiteDurableStore) -> None:
    _open_job(store)
    with pytest.raises(InvalidTransitionError, match="different declaration"):
        store.initialize_job(
            JOB,
            pipeline_id=PIPELINE,
            source_mode="snapshot",
            declaration_fingerprint="decl-v2",
            snapshot_id="v7",
        )


def test_register_discovery_publishes_partitions_and_advances_position(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    advanced = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich"), _partition("p2", "load", "enrich")],
        next_position="cursor-2",
    )
    assert advanced.discovery_position == "cursor-2"
    assert advanced.generation == job.generation + 1
    assert advanced.discovery_sealed is False

    partition = store.load_partition(JOB, "p1")
    assert partition is not None
    assert partition.input_ref == "source://p1"

    load_unit = store.load_work_unit(WorkUnitKey(JOB, "load", "p1"))
    enrich_unit = store.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
    assert load_unit is not None and load_unit.pending_deps == 0
    assert enrich_unit is not None and enrich_unit.pending_deps == 1


def test_every_transition_survives_closing_and_reopening_the_store(store_path: Path) -> None:
    opened = SQLiteDurableStore(store_path)
    try:
        job = _open_job(opened, mode="incremental")
        job = opened.register_discovery(
            JOB,
            expected_generation=job.generation,
            partitions=[_partition("p1", "load", "enrich")],
            next_position="cursor-1",
            seal=True,
        )
        _drive_unit_to_sealed(opened, WorkUnitKey(JOB, "load", "p1"))
        _drive_unit_to_sealed(opened, WorkUnitKey(JOB, "enrich", "p1"))
        job = opened.load_job(JOB)
        assert job is not None
        opened.complete_job(JOB, expected_generation=job.generation, checkpoint_position="pos-100")
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        reloaded = reopened.load_job(JOB)
        assert reloaded is not None
        assert reloaded.status == "completed"
        assert reloaded.discovery_sealed is True

        unit = reopened.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
        assert unit is not None
        assert unit.status == "sealed"
        assert unit.batch_count == 1
        assert unit.owner_id is None

        batch = reopened.lookup_batch(WorkUnitKey(JOB, "enrich", "p1"), "b1")
        assert batch is not None
        assert batch.output_ref == "output://enrich/p1/b1"

        seals = {seal.node_id: seal for seal in reopened.node_seals(JOB)}
        assert set(seals) == {"load", "enrich"}
        assert seals["enrich"].final_position is not None

        checkpoint = reopened.load_checkpoint(PIPELINE, "default")
        assert checkpoint is not None
        assert checkpoint.completed_position == "pos-100"
    finally:
        reopened.close()


def test_sealing_a_unit_activates_its_dependents(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich")],
    )
    assert [unit.key.node_id for unit in store.eligible_work(JOB, limit=10)] == ["load"]

    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))

    enrich = store.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
    assert enrich is not None
    assert enrich.pending_deps == 0
    assert [unit.key.node_id for unit in store.eligible_work(JOB, limit=10)] == ["enrich"]


def test_a_partition_advances_while_another_partition_lags(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich"), _partition("p2", "load", "enrich")],
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))

    eligible = {
        (unit.key.node_id, unit.key.partition_id) for unit in store.eligible_work(JOB, limit=10)
    }
    assert ("enrich", "p1") in eligible
    assert ("load", "p2") in eligible
    assert ("enrich", "p2") not in eligible


def test_a_barrier_edge_registered_after_sealing_is_already_satisfied(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[
            _partition("p1", "load"),
            DiscoveredPartition(
                partition_id="__aggregate__",
                input_ref="source://aggregate",
                work_units=(
                    DiscoveredWorkUnit(
                        node_id="summarize",
                        step_revision="rev-1",
                        params_fingerprint="params-1",
                        input_scope="all",
                    ),
                ),
            ),
        ],
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))

    current = store.load_job(JOB)
    assert current is not None
    store.register_discovery(
        JOB,
        expected_generation=current.generation,
        extra_dependencies=[
            DependencyEdge(
                node_id="summarize",
                partition_id="__aggregate__",
                depends_on_node_id="load",
                depends_on_partition_id="p1",
            )
        ],
    )
    summarize = store.load_work_unit(WorkUnitKey(JOB, "summarize", "__aggregate__"))
    assert summarize is not None
    assert summarize.pending_deps == 0


def test_a_barrier_edge_registered_before_sealing_blocks_then_opens(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[
            _partition("p1", "load"),
            DiscoveredPartition(
                partition_id="__aggregate__",
                input_ref="source://aggregate",
                work_units=(
                    DiscoveredWorkUnit(
                        node_id="summarize",
                        step_revision="rev-1",
                        params_fingerprint="params-1",
                        input_scope="all",
                    ),
                ),
            ),
        ],
        extra_dependencies=[
            DependencyEdge(
                node_id="summarize",
                partition_id="__aggregate__",
                depends_on_node_id="load",
                depends_on_partition_id="p1",
            )
        ],
    )
    blocked = store.load_work_unit(WorkUnitKey(JOB, "summarize", "__aggregate__"))
    assert blocked is not None and blocked.pending_deps == 1

    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))

    opened = store.load_work_unit(WorkUnitKey(JOB, "summarize", "__aggregate__"))
    assert opened is not None and opened.pending_deps == 0


def test_an_empty_unit_seals_through_explicit_end_of_input(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    exhausted = store.mark_input_exhausted(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
    )
    sealed = store.seal_work_unit(
        key,
        owner_id=OWNER,
        fence_token=exhausted.fence_token,
        expected_generation=exhausted.generation,
    )
    assert sealed.status == "sealed"
    assert sealed.batch_count == 0


def test_fail_work_unit_records_the_failure_without_activating_dependents(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich")],
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    failed = store.fail_work_unit(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        error_type="VendorError",
    )
    assert failed.status == "failed"
    assert failed.error_type == "VendorError"

    enrich = store.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
    assert enrich is not None and enrich.pending_deps == 1
    assert store.eligible_work(JOB, limit=10) == ()


# ---------------------------------------------------------------------------
# Generation conflicts (required coverage 2)
# ---------------------------------------------------------------------------


def test_register_discovery_rejects_a_stale_generation(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    with pytest.raises(GenerationConflictError):
        store.register_discovery(
            JOB, expected_generation=job.generation, partitions=[_partition("p2", "load")]
        )
    assert store.load_partition(JOB, "p2") is None


@pytest.mark.parametrize("transition", ["claim", "commit", "exhaust", "seal", "fail"])
def test_work_unit_transitions_reject_a_stale_generation(
    store: SQLiteDurableStore, transition: str
) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None

    if transition == "claim":
        with pytest.raises(GenerationConflictError):
            store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation + 5)
        assert store.load_work_unit(key) == unit
        return

    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    stale = claimed.generation + 5
    with pytest.raises(GenerationConflictError):
        if transition == "commit":
            store.commit_batch(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=stale,
                batch_id="b1",
                output_ref="output://load/p1/b1",
            )
        elif transition == "exhaust":
            store.mark_input_exhausted(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=stale,
            )
        elif transition == "seal":
            store.seal_work_unit(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=stale,
            )
        else:
            store.fail_work_unit(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=stale,
                error_type="VendorError",
            )
    assert store.load_work_unit(key) == claimed
    assert store.lookup_batch(key, "b1") is None


def test_complete_job_rejects_a_stale_generation(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load")],
        seal=True,
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))
    with pytest.raises(GenerationConflictError):
        store.complete_job(JOB, expected_generation=job.generation + 9)
    reloaded = store.load_job(JOB)
    assert reloaded is not None and reloaded.status == "open"


# ---------------------------------------------------------------------------
# Fencing (required coverage 3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transition", ["commit", "exhaust", "seal", "fail"])
def test_ownership_checked_transitions_reject_a_stale_fence(
    store: SQLiteDurableStore, transition: str
) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    stale_fence = claimed.fence_token - 1

    with pytest.raises(FenceViolationError):
        if transition == "commit":
            store.commit_batch(
                key,
                owner_id=OWNER,
                fence_token=stale_fence,
                expected_generation=claimed.generation,
                batch_id="b1",
                output_ref="output://load/p1/b1",
            )
        elif transition == "exhaust":
            store.mark_input_exhausted(
                key,
                owner_id=OWNER,
                fence_token=stale_fence,
                expected_generation=claimed.generation,
            )
        elif transition == "seal":
            store.seal_work_unit(
                key,
                owner_id=OWNER,
                fence_token=stale_fence,
                expected_generation=claimed.generation,
            )
        else:
            store.fail_work_unit(
                key,
                owner_id=OWNER,
                fence_token=stale_fence,
                expected_generation=claimed.generation,
                error_type="VendorError",
            )
    assert store.load_work_unit(key) == claimed


def test_a_different_owner_is_fenced_out(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)

    with pytest.raises(FenceViolationError, match="another owner"):
        store.commit_batch(
            key,
            owner_id="worker-b",
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            output_ref="output://load/p1/b1",
        )


def test_an_unclaimed_unit_fences_every_writer(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    with pytest.raises(FenceViolationError, match="no active claim"):
        store.commit_batch(
            key,
            owner_id=OWNER,
            fence_token=0,
            expected_generation=unit.generation,
            batch_id="b1",
            output_ref="output://load/p1/b1",
        )


def test_a_live_claim_blocks_takeover_and_an_expired_one_allows_it(store_path: Path) -> None:
    moment = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)
    opened = SQLiteDurableStore(store_path, clock=lambda: moment)
    try:
        job = _open_job(opened)
        opened.register_discovery(
            JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
        )
        key = WorkUnitKey(JOB, "load", "p1")
        unit = opened.load_work_unit(key)
        assert unit is not None
        claimed = opened.claim_work_unit(
            key, owner_id=OWNER, expected_generation=unit.generation, lease_seconds=60
        )

        with pytest.raises(FenceViolationError, match="live owner"):
            opened.claim_work_unit(key, owner_id="worker-b", expected_generation=claimed.generation)

        moment = moment + timedelta(seconds=61)
        taken = opened.claim_work_unit(
            key, owner_id="worker-b", expected_generation=claimed.generation
        )
        assert taken.owner_id == "worker-b"
        assert taken.fence_token > claimed.fence_token
        assert taken.claim_epoch > claimed.claim_epoch

        with pytest.raises(FenceViolationError):
            opened.commit_batch(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=taken.generation,
                batch_id="b1",
                output_ref="output://load/p1/b1",
            )
    finally:
        opened.close()


def test_an_expired_lease_fences_its_own_holder(store_path: Path) -> None:
    moment = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)
    opened = SQLiteDurableStore(store_path, clock=lambda: moment)
    try:
        job = _open_job(opened)
        opened.register_discovery(
            JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
        )
        key = WorkUnitKey(JOB, "load", "p1")
        unit = opened.load_work_unit(key)
        assert unit is not None
        claimed = opened.claim_work_unit(
            key, owner_id=OWNER, expected_generation=unit.generation, lease_seconds=30
        )
        moment = moment + timedelta(seconds=31)
        with pytest.raises(FenceViolationError, match="expired"):
            opened.commit_batch(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=claimed.generation,
                batch_id="b1",
                output_ref="output://load/p1/b1",
            )
    finally:
        opened.close()


# ---------------------------------------------------------------------------
# Illegal transitions (required coverage 4)
# ---------------------------------------------------------------------------


def test_sealing_requires_exhausted_input(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    committed = store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        output_ref="output://load/p1/b1",
    )
    assert committed.input_exhausted is False
    with pytest.raises(InvalidTransitionError, match="end of input"):
        store.seal_work_unit(
            key,
            owner_id=OWNER,
            fence_token=committed.fence_token,
            expected_generation=committed.generation,
        )


def test_committing_to_a_sealed_unit_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    _drive_unit_to_sealed(store, key)
    sealed = store.load_work_unit(key)
    assert sealed is not None
    with pytest.raises(FenceViolationError):
        store.commit_batch(
            key,
            owner_id=OWNER,
            fence_token=sealed.fence_token,
            expected_generation=sealed.generation,
            batch_id="b2",
            output_ref="output://load/p1/b2",
        )


def test_claiming_a_sealed_unit_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    _drive_unit_to_sealed(store, key)
    sealed = store.load_work_unit(key)
    assert sealed is not None
    with pytest.raises(InvalidTransitionError, match="sealed"):
        store.claim_work_unit(key, owner_id=OWNER, expected_generation=sealed.generation)


def test_claiming_a_dependency_blocked_unit_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich")],
    )
    key = WorkUnitKey(JOB, "enrich", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    with pytest.raises(InvalidTransitionError, match="unsatisfied dependencies"):
        store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)


def test_registering_discovery_after_sealing_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    sealed = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load")],
        seal=True,
    )
    with pytest.raises(InvalidTransitionError, match="sealed"):
        store.register_discovery(
            JOB,
            expected_generation=sealed.generation,
            partitions=[_partition("p2", "load")],
        )


def test_completing_a_job_with_unsealed_work_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load", "enrich")],
        seal=True,
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))
    current = store.load_job(JOB)
    assert current is not None
    with pytest.raises(InvalidTransitionError, match="unsealed work units"):
        store.complete_job(JOB, expected_generation=current.generation)


def test_completing_a_job_with_open_discovery_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))
    with pytest.raises(InvalidTransitionError, match="not sealed"):
        store.complete_job(JOB, expected_generation=job.generation)


def test_completing_an_already_completed_job_is_rejected(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load")],
        seal=True,
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))
    completed = store.complete_job(JOB, expected_generation=job.generation)
    with pytest.raises(InvalidTransitionError, match="already completed"):
        store.complete_job(JOB, expected_generation=completed.generation)


def test_a_snapshot_job_cannot_advance_a_lineage_checkpoint(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load")],
        seal=True,
    )
    _drive_unit_to_sealed(store, WorkUnitKey(JOB, "load", "p1"))
    with pytest.raises(ValueError, match="incremental"):
        store.complete_job(JOB, expected_generation=job.generation, checkpoint_position="pos-100")


def test_replayed_discovery_must_present_the_same_input_reference(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    conflicting = DiscoveredPartition(partition_id="p1", input_ref="source://changed")
    with pytest.raises(InvalidTransitionError, match="different input identity"):
        store.register_discovery(JOB, expected_generation=job.generation, partitions=[conflicting])


def test_replayed_discovery_with_identical_input_is_a_no_op(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    job = store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    again = store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    assert again.generation == job.generation + 1
    unit = store.load_work_unit(WorkUnitKey(JOB, "load", "p1"))
    assert unit is not None and unit.pending_deps == 0


# ---------------------------------------------------------------------------
# Rollback (required coverage 5)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failpoint", ["after_batch_insert", "after_batch_progress"])
def test_an_interrupted_batch_commit_leaves_no_partial_state(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint: str
) -> None:
    opened = SQLiteDurableStore(store_path)
    try:
        job = _open_job(opened)
        opened.register_discovery(
            JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
        )
        key = WorkUnitKey(JOB, "load", "p1")
        unit = opened.load_work_unit(key)
        assert unit is not None
        claimed = opened.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)

        _arm_failpoint(monkeypatch, failpoint)
        with pytest.raises(_InterruptedError):
            opened.commit_batch(
                key,
                owner_id=OWNER,
                fence_token=claimed.fence_token,
                expected_generation=claimed.generation,
                batch_id="b1",
                output_ref="output://load/p1/b1",
                input_exhausted=True,
            )
        _disarm_failpoints(monkeypatch)
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        unit = reopened.load_work_unit(WorkUnitKey(JOB, "load", "p1"))
        assert unit is not None
        assert unit.batch_count == 0
        assert unit.input_exhausted is False
        assert unit.generation == claimed.generation
        assert reopened.lookup_batch(WorkUnitKey(JOB, "load", "p1"), "b1") is None
    finally:
        reopened.close()


@pytest.mark.parametrize("failpoint", ["after_unit_sealed", "after_dependencies_activated"])
def test_an_interrupted_seal_leaves_the_unit_unsealed(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint: str
) -> None:
    opened = SQLiteDurableStore(store_path)
    try:
        job = _open_job(opened)
        opened.register_discovery(
            JOB,
            expected_generation=job.generation,
            partitions=[_partition("p1", "load", "enrich")],
        )
        key = WorkUnitKey(JOB, "load", "p1")
        unit = opened.load_work_unit(key)
        assert unit is not None
        claimed = opened.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
        committed = opened.commit_batch(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            output_ref="output://load/p1/b1",
            input_exhausted=True,
        )

        _arm_failpoint(monkeypatch, failpoint)
        with pytest.raises(_InterruptedError):
            opened.seal_work_unit(
                key,
                owner_id=OWNER,
                fence_token=committed.fence_token,
                expected_generation=committed.generation,
            )
        _disarm_failpoints(monkeypatch)
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        unit = reopened.load_work_unit(WorkUnitKey(JOB, "load", "p1"))
        assert unit is not None
        assert unit.status == "executing"
        enrich = reopened.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
        assert enrich is not None and enrich.pending_deps == 1
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "failpoint", ["after_node_seals", "after_job_completed", "after_checkpoint_advanced"]
)
def test_an_interrupted_completion_leaves_the_job_open(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint: str
) -> None:
    opened = SQLiteDurableStore(store_path)
    try:
        job = _open_job(opened, mode="incremental")
        job = opened.register_discovery(
            JOB,
            expected_generation=job.generation,
            partitions=[_partition("p1", "load")],
            seal=True,
        )
        _drive_unit_to_sealed(opened, WorkUnitKey(JOB, "load", "p1"))

        _arm_failpoint(monkeypatch, failpoint)
        with pytest.raises(_InterruptedError):
            opened.complete_job(
                JOB, expected_generation=job.generation, checkpoint_position="pos-100"
            )
        _disarm_failpoints(monkeypatch)
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        reloaded = reopened.load_job(JOB)
        assert reloaded is not None
        assert reloaded.status == "open"
        assert reloaded.generation == job.generation
        assert reopened.node_seals(JOB) == ()
        checkpoint = reopened.load_checkpoint(PIPELINE, "default")
        assert checkpoint is not None and checkpoint.completed_position is None
    finally:
        reopened.close()


def test_an_interrupted_discovery_registers_nothing(
    store_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened = SQLiteDurableStore(store_path)
    try:
        job = _open_job(opened)
        _arm_failpoint(monkeypatch, "after_discovery_insert")
        with pytest.raises(_InterruptedError):
            opened.register_discovery(
                JOB,
                expected_generation=job.generation,
                partitions=[_partition("p1", "load")],
                next_position="cursor-1",
            )
        _disarm_failpoints(monkeypatch)
    finally:
        opened.close()

    reopened = SQLiteDurableStore(store_path)
    try:
        reloaded = reopened.load_job(JOB)
        assert reloaded is not None
        assert reloaded.discovery_position is None
        assert reloaded.generation == job.generation
        assert reopened.load_partition(JOB, "p1") is None
        assert reopened.load_work_unit(WorkUnitKey(JOB, "load", "p1")) is None
    finally:
        reopened.close()


# ---------------------------------------------------------------------------
# Idempotent publication and uncertain commits (required coverage 6)
# ---------------------------------------------------------------------------


def test_repeating_a_batch_commit_is_idempotent(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    first = store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        output_ref="output://load/p1/b1",
        record_count=7,
    )
    second = store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        output_ref="output://load/p1/b1",
        record_count=7,
    )
    assert first == second
    assert second.batch_count == 1


def test_a_repeated_batch_id_with_different_output_is_rejected(
    store: SQLiteDurableStore,
) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        output_ref="output://load/p1/b1",
    )
    with pytest.raises(InvalidTransitionError, match="immutable"):
        store.commit_batch(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation + 1,
            batch_id="b1",
            output_ref="output://load/p1/b1-rewritten",
        )


def test_lookup_batch_resolves_a_lost_acknowledgment(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB, expected_generation=job.generation, partitions=[_partition("p1", "load")]
    )
    key = WorkUnitKey(JOB, "load", "p1")
    assert store.lookup_batch(key, "b1") is None

    unit = store.load_work_unit(key)
    assert unit is not None
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    store.commit_batch(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        output_ref="output://load/p1/b1",
    )
    resolved = store.lookup_batch(key, "b1")
    assert resolved is not None
    assert resolved.output_ref == "output://load/p1/b1"
    assert resolved.key == key
    assert store.lookup_batch(key, "b2") is None


def test_batch_positions_are_strictly_increasing(store: SQLiteDurableStore) -> None:
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition("p1", "load"), _partition("p2", "load")],
    )
    positions = []
    for partition_id in ("p1", "p2"):
        key = WorkUnitKey(JOB, "load", partition_id)
        unit = store.load_work_unit(key)
        assert unit is not None
        claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
        store.commit_batch(
            key,
            owner_id=OWNER,
            fence_token=claimed.fence_token,
            expected_generation=claimed.generation,
            batch_id="b1",
            output_ref=f"output://load/{partition_id}/b1",
        )
        record = store.lookup_batch(key, "b1")
        assert record is not None
        positions.append(record.position)
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)


# ---------------------------------------------------------------------------
# Bounded eligible-work query (required coverage: boundedness proof)
# ---------------------------------------------------------------------------


def _register_many(store: SQLiteDurableStore, count: int) -> None:
    """Register ``count`` partitions whose dependency-blocked units sort first.

    Each partition declares ``enrich`` depending on ``load``. Primary-key order
    is (job_id, node_id, partition_id), so every blocked ``enrich`` row precedes
    every eligible ``load`` row. A naive table scan must therefore traverse all
    ``count`` ineligible rows before reaching the first eligible one, while an
    index-backed seek goes straight to it. That asymmetry is what makes the
    boundedness assertion below meaningful rather than vacuous.
    """
    job = _open_job(store)
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[_partition(f"p{index:05d}", "load", "enrich") for index in range(count)],
    )


def _eligible_query_steps(store_path: Path, count: int) -> int:
    """Return the number of VDBE steps the eligible query costs over ``count`` units."""
    opened = SQLiteDurableStore(store_path)
    try:
        _register_many(opened, count)
    finally:
        opened.close()

    connection = sqlite3.connect(store_path)
    try:
        connection.row_factory = sqlite3.Row
        steps = 0

        def handler() -> int:
            nonlocal steps
            steps += 1
            return 0

        connection.set_progress_handler(handler, 1)
        connection.execute(sqlite_module.ELIGIBLE_WORK_SQL, (JOB, 5)).fetchall()
        connection.set_progress_handler(None, 1)
    finally:
        connection.close()
    return steps


def test_eligible_work_query_uses_its_index_and_never_scans(store: SQLiteDurableStore) -> None:
    _register_many(store, 50)
    with store._read() as connection:
        plan = connection.execute(
            f"EXPLAIN QUERY PLAN {sqlite_module.ELIGIBLE_WORK_SQL}", (JOB, 5)
        ).fetchall()
    detail = " ".join(str(row["detail"]) for row in plan)
    assert "work_units_eligible" in detail
    assert "USING INDEX" in detail or "USING COVERING INDEX" in detail
    assert "SCAN work_units" not in detail
    assert "TEMP B-TREE" not in detail


def test_eligible_work_cost_does_not_grow_with_total_partitions(tmp_path: Path) -> None:
    small = _eligible_query_steps(tmp_path / "small.sqlite3", 200)
    large = _eligible_query_steps(tmp_path / "large.sqlite3", 2000)
    assert large < small * 2, (
        f"Eligible-work query cost grew from {small} to {large} VDBE steps for a tenfold "
        "increase in work units; the query is not bounded."
    )


def test_eligible_work_respects_its_limit(store: SQLiteDurableStore) -> None:
    _register_many(store, 100)
    assert len(store.eligible_work(JOB, limit=5)) == 5
    assert len(store.eligible_work(JOB, limit=1)) == 1
    assert {unit.key.node_id for unit in store.eligible_work(JOB, limit=50)} == {"load"}


@pytest.mark.parametrize("limit", [0, -1, 1.5, True, None])
def test_eligible_work_rejects_an_invalid_limit(store: SQLiteDurableStore, limit: Any) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        store.eligible_work(JOB, limit=limit)


# ---------------------------------------------------------------------------
# Record hygiene and value validation
# ---------------------------------------------------------------------------


def test_work_unit_key_requires_bounded_identifiers() -> None:
    with pytest.raises(ValueError, match="job_id"):
        WorkUnitKey("", "load", "p1")
    with pytest.raises(ValueError, match="node_id"):
        WorkUnitKey(JOB, "x" * 513, "p1")


def test_a_work_unit_may_not_depend_on_itself() -> None:
    with pytest.raises(ValueError, match="depend on itself"):
        DiscoveredWorkUnit(
            node_id="load",
            step_revision="rev-1",
            params_fingerprint="params-1",
            depends_on_nodes=("load",),
        )
    with pytest.raises(ValueError, match="depend on itself"):
        DependencyEdge(
            node_id="load",
            partition_id="p1",
            depends_on_node_id="load",
            depends_on_partition_id="p1",
        )


def test_a_partition_may_not_declare_a_node_twice() -> None:
    unit = DiscoveredWorkUnit(node_id="load", step_revision="rev-1", params_fingerprint="params-1")
    with pytest.raises(ValueError, match="same node id twice"):
        DiscoveredPartition(partition_id="p1", input_ref="source://p1", work_units=(unit, unit))


def test_a_closed_store_rejects_further_use(store_path: Path) -> None:
    opened = SQLiteDurableStore(store_path)
    opened.close()
    with pytest.raises(StoreUnavailableError):
        opened.load_job(JOB)
