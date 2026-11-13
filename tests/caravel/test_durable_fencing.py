"""Fencing: claim conflicts, lease expiry, takeover, and stale writers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pytest

from caravel.durability import (
    ConcurrencyUnsupportedError,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    FenceViolationError,
    GenerationConflictError,
    InvalidTransitionError,
    LeasePolicy,
    SQLiteDurableStore,
    StoreCapabilities,
    WorkUnitKey,
    validate_concurrency,
)
from caravel.durability.leases import LeaseHolder, acquire

JOB = "job-1"
PIPELINE = "customer-42/documents"
NODE = "load"


class _Clock:
    """A controlled clock so lease expiry needs no sleeping."""

    def __init__(self) -> None:
        self.now = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


@pytest.fixture()
def clock() -> _Clock:
    return _Clock()


@pytest.fixture()
def store(tmp_path: Path, clock: _Clock) -> Iterator[SQLiteDurableStore]:
    opened = SQLiteDurableStore(tmp_path / "durable.sqlite3", clock=clock)
    try:
        yield opened
    finally:
        opened.close()


def _prepare(store: SQLiteDurableStore, *partition_ids: str) -> None:
    job = store.initialize_job(
        JOB,
        pipeline_id=PIPELINE,
        source_mode="snapshot",
        declaration_fingerprint="decl-v1",
        snapshot_id="v7",
    )
    store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[
            DiscoveredPartition(
                partition_id=partition_id,
                input_ref=f"source://{partition_id}",
                work_units=(
                    DiscoveredWorkUnit(
                        node_id=NODE, step_revision="rev-1", params_fingerprint="params-1"
                    ),
                ),
            )
            for partition_id in partition_ids
        ],
        seal=True,
    )


def _key(partition_id: str = "p1") -> WorkUnitKey:
    return WorkUnitKey(JOB, NODE, partition_id)


def _state(store: SQLiteDurableStore, partition_id: str = "p1") -> Any:
    unit = store.load_work_unit(_key(partition_id))
    assert unit is not None
    return unit


# ---------------------------------------------------------------------------
# Claim conflicts (coverage 1)
# ---------------------------------------------------------------------------


def test_exactly_one_of_two_racing_claims_wins(store: SQLiteDurableStore) -> None:
    _prepare(store, "p1")
    unit = _state(store)

    winner = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=60
    )
    with pytest.raises(GenerationConflictError):
        store.claim_work_unit(
            _key(), owner_id="worker-b", expected_generation=unit.generation, lease_seconds=60
        )

    current = _state(store)
    assert current.owner_id == "worker-a"
    assert current.fence_token == winner.fence_token


def test_a_loser_reloading_still_cannot_take_a_live_claim(
    store: SQLiteDurableStore,
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=60
    )

    reloaded = _state(store)
    with pytest.raises(FenceViolationError, match="live owner"):
        store.claim_work_unit(
            _key(),
            owner_id="worker-b",
            expected_generation=reloaded.generation,
            lease_seconds=60,
        )


# ---------------------------------------------------------------------------
# Lease expiry and takeover (coverage 2 and 3)
# ---------------------------------------------------------------------------


def test_an_expired_lease_can_be_taken_over_with_a_higher_fence(
    store: SQLiteDurableStore, clock: _Clock
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    first = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=30
    )

    clock.advance(31)
    second = store.claim_work_unit(
        _key(), owner_id="worker-b", expected_generation=first.generation, lease_seconds=30
    )
    assert second.owner_id == "worker-b"
    assert second.fence_token > first.fence_token
    assert second.claim_epoch > first.claim_epoch


@pytest.mark.parametrize("transition", ["commit", "exhaust", "seal", "fail", "renew", "release"])
def test_a_stale_writer_cannot_act_after_takeover(
    store: SQLiteDurableStore, clock: _Clock, transition: str
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    stale = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=30
    )
    clock.advance(31)
    fresh = store.claim_work_unit(
        _key(), owner_id="worker-b", expected_generation=stale.generation, lease_seconds=30
    )

    with pytest.raises(FenceViolationError):
        if transition == "commit":
            store.commit_batch(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
                batch_id="b1",
                output_ref="output://stale",
            )
        elif transition == "exhaust":
            store.mark_input_exhausted(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
            )
        elif transition == "seal":
            store.seal_work_unit(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
            )
        elif transition == "fail":
            store.fail_work_unit(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
                error_type="VendorError",
            )
        elif transition == "renew":
            store.renew_lease(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
                lease_seconds=30,
            )
        else:
            store.release_work_unit(
                _key(),
                owner_id="worker-a",
                fence_token=stale.fence_token,
                expected_generation=fresh.generation,
            )

    current = _state(store)
    assert current.owner_id == "worker-b"
    assert store.lookup_batch(_key(), "b1") is None


def test_a_live_lease_is_never_seized_without_exclusivity(
    store: SQLiteDurableStore, clock: _Clock
) -> None:
    """The narrowed contract: only a single-worker policy may seize a live lease."""
    _prepare(store, "p1")
    unit = _state(store)
    held = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=300
    )
    clock.advance(10)

    with pytest.raises(FenceViolationError, match="live owner"):
        store.claim_work_unit(
            _key(), owner_id="worker-b", expected_generation=held.generation, lease_seconds=30
        )

    seized = store.claim_work_unit(
        _key(),
        owner_id="worker-b",
        expected_generation=held.generation,
        lease_seconds=30,
        exclusive=True,
    )
    assert seized.owner_id == "worker-b"
    assert seized.fence_token > held.fence_token


# ---------------------------------------------------------------------------
# Heartbeats (coverage 4)
# ---------------------------------------------------------------------------


def test_a_heartbeat_extends_a_lease_and_blocks_takeover(
    store: SQLiteDurableStore, clock: _Clock
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    lease = acquire(
        store,
        _key(),
        owner_id="worker-a",
        expected_generation=unit.generation,
        policy=LeasePolicy(ttl_seconds=30, heartbeat_seconds=10),
    )

    clock.advance(20)
    renewed = lease.heartbeat()
    assert renewed.lease_expires_at is not None

    clock.advance(20)  # 40s after the original claim, but renewed at 20s.
    with pytest.raises(FenceViolationError, match="live owner"):
        store.claim_work_unit(
            _key(),
            owner_id="worker-b",
            expected_generation=renewed.generation,
            lease_seconds=30,
        )


def test_a_stalled_heartbeat_lets_the_lease_expire(
    store: SQLiteDurableStore, clock: _Clock
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    lease = acquire(
        store,
        _key(),
        owner_id="worker-a",
        expected_generation=unit.generation,
        policy=LeasePolicy(ttl_seconds=30, heartbeat_seconds=10),
    )

    clock.advance(31)  # the worker stalled and never renewed
    taken = store.claim_work_unit(
        _key(),
        owner_id="worker-b",
        expected_generation=lease.state.generation,
        lease_seconds=30,
    )
    assert taken.owner_id == "worker-b"

    with pytest.raises(FenceViolationError):
        lease.heartbeat()


def test_a_lease_policy_rejects_a_heartbeat_longer_than_its_ttl() -> None:
    with pytest.raises(ValueError, match="shorter than ttl_seconds"):
        LeasePolicy(ttl_seconds=10, heartbeat_seconds=10)
    with pytest.raises(ValueError, match="positive"):
        LeasePolicy(ttl_seconds=0, heartbeat_seconds=1)


# ---------------------------------------------------------------------------
# Clean shutdown (coverage 5)
# ---------------------------------------------------------------------------


def test_releasing_a_claim_lets_another_worker_proceed_immediately(
    store: SQLiteDurableStore, clock: _Clock
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    lease = acquire(
        store,
        _key(),
        owner_id="worker-a",
        expected_generation=unit.generation,
        policy=LeasePolicy(ttl_seconds=300, heartbeat_seconds=10),
    )
    released = lease.release()
    assert released.status == "eligible"
    assert released.owner_id is None

    # No waiting out the lease: the unit is eligible again right away.
    assert [item.key for item in store.eligible_work(JOB, limit=5)] == [_key()]
    taken = store.claim_work_unit(
        _key(), owner_id="worker-b", expected_generation=released.generation, lease_seconds=30
    )
    assert taken.owner_id == "worker-b"


def test_a_released_lease_cannot_heartbeat(
    store: SQLiteDurableStore,
) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    lease = acquire(
        store,
        _key(),
        owner_id="worker-a",
        expected_generation=unit.generation,
        policy=LeasePolicy(ttl_seconds=300, heartbeat_seconds=10),
    )
    lease.release()
    with pytest.raises(FenceViolationError, match="already released"):
        lease.heartbeat()


def test_releasing_a_sealed_unit_is_rejected(store: SQLiteDurableStore) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    claimed = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=60
    )
    exhausted = store.mark_input_exhausted(
        _key(),
        owner_id="worker-a",
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
    )
    sealed = store.seal_work_unit(
        _key(),
        owner_id="worker-a",
        fence_token=exhausted.fence_token,
        expected_generation=exhausted.generation,
    )
    with pytest.raises(FenceViolationError):
        store.release_work_unit(
            _key(),
            owner_id="worker-a",
            fence_token=sealed.fence_token,
            expected_generation=sealed.generation,
        )


# ---------------------------------------------------------------------------
# Completion races (coverage 8)
# ---------------------------------------------------------------------------


def test_only_one_of_two_racing_completions_commits(store: SQLiteDurableStore) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    claimed = store.claim_work_unit(
        _key(), owner_id="worker-a", expected_generation=unit.generation, lease_seconds=60
    )
    exhausted = store.mark_input_exhausted(
        _key(),
        owner_id="worker-a",
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
    )
    store.seal_work_unit(
        _key(),
        owner_id="worker-a",
        fence_token=exhausted.fence_token,
        expected_generation=exhausted.generation,
    )

    job = store.load_job(JOB)
    assert job is not None
    # Both workers read the same generation, as concurrent workers would.
    store.complete_job(JOB, expected_generation=job.generation)
    with pytest.raises(InvalidTransitionError, match="already completed"):
        store.complete_job(JOB, expected_generation=job.generation)

    completed = store.load_job(JOB)
    assert completed is not None and completed.status == "completed"
    assert len(store.node_seals(JOB)) == 1


def test_a_lineage_checkpoint_advances_exactly_once(tmp_path: Path) -> None:
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        job = store.initialize_job(
            "range-1",
            pipeline_id=PIPELINE,
            source_mode="incremental",
            declaration_fingerprint="decl-v1",
            range_start="pos-0",
            range_end="pos-9",
        )
        job = store.register_discovery("range-1", expected_generation=job.generation, seal=True)
        store.complete_job(
            "range-1", expected_generation=job.generation, checkpoint_position="pos-9"
        )

        with pytest.raises(InvalidTransitionError):
            store.complete_job(
                "range-1", expected_generation=job.generation, checkpoint_position="pos-9"
            )

        checkpoint = store.load_checkpoint(PIPELINE, "default")
        assert checkpoint is not None
        assert checkpoint.completed_position == "pos-9"
        assert checkpoint.generation == 1, "the checkpoint advanced exactly once"
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Backend capability rejection (coverage 10)
# ---------------------------------------------------------------------------


def test_concurrency_is_rejected_on_a_backend_without_fencing(
    store: SQLiteDurableStore,
) -> None:
    class _Degraded:
        backend = "degraded"
        capabilities = StoreCapabilities(
            atomic_conditional_transitions=True,
            generation_validation=True,
            fencing_validation=False,
            durable_reads=True,
            transactional_publication=True,
        )

    with pytest.raises(ConcurrencyUnsupportedError, match="fencing_validation"):
        validate_concurrency(_Degraded(), workers=4)  # type: ignore[arg-type]

    # One worker needs no concurrency guarantees.
    validate_concurrency(_Degraded(), workers=1)  # type: ignore[arg-type]
    validate_concurrency(store, workers=4)


def test_validate_concurrency_rejects_a_nonsense_worker_count(
    store: SQLiteDurableStore,
) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        validate_concurrency(store, workers=0)


def test_a_lease_holder_reports_its_fence_token(store: SQLiteDurableStore) -> None:
    _prepare(store, "p1")
    unit = _state(store)
    lease = acquire(
        store,
        _key(),
        owner_id="worker-a",
        expected_generation=unit.generation,
        policy=LeasePolicy(),
    )
    assert isinstance(lease, LeaseHolder)
    assert lease.fence_token == lease.state.fence_token
    assert lease.fence_token > 0
