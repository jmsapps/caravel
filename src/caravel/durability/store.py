"""The backend-neutral durable execution store contract.

Every backend implements :class:`DurableStore` with identical semantics. The
authoritative schema, transition preconditions, and error mapping live in the
C-8 task record's ``references/storage-contract.md``.

Capabilities are validated before any durable mutation, user code, or cleanup.
Generic object-store or filesystem access declares none of them and is rejected
rather than assumed to work.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from .types import (
    BatchRecord,
    DependencyEdge,
    DiscoveredPartition,
    JobState,
    NodeSeal,
    PartitionRecord,
    PipelineCheckpoint,
    SourceMode,
    StorageCapabilityError,
    StoreCapabilities,
    WorkUnitKey,
    WorkUnitState,
)


def validate_capabilities(capabilities: StoreCapabilities, *, backend: str) -> None:
    """Raise unless ``capabilities`` declares every required durability guarantee."""
    missing = capabilities.missing()
    if missing:
        raise StorageCapabilityError(
            f"Durable store backend '{backend}' does not provide required "
            f"capabilities: {', '.join(missing)}. A durable metadata backend must "
            "support atomic conditional transitions, generation validation, "
            "fencing validation, durable reads, and transactional publication."
        )


@runtime_checkable
class DurableStore(Protocol):
    """One authoritative durable execution state machine.

    Each method below is exactly one atomic transition or one durable read.
    Mutating methods validate ownership, fence token, applicable lease expiry,
    and expected generation inside the same transaction that writes.
    """

    @property
    def capabilities(self) -> StoreCapabilities:
        """Return the durability capabilities this backend provides."""
        ...

    # -- durable reads -----------------------------------------------------

    def load_job(self, job_id: str) -> JobState | None: ...

    def load_work_unit(self, key: WorkUnitKey) -> WorkUnitState | None: ...

    def load_partition(self, job_id: str, partition_id: str) -> PartitionRecord | None: ...

    def load_checkpoint(self, pipeline_id: str, lineage: str) -> PipelineCheckpoint | None: ...

    def lookup_batch(self, key: WorkUnitKey, batch_id: str) -> BatchRecord | None:
        """Resolve a batch publication whose acknowledgment was lost."""
        ...

    def work_unit_batches(self, key: WorkUnitKey) -> tuple[BatchRecord, ...]:
        """Return one work unit's committed batches in commit-position order."""
        ...

    def work_unit_partitions(self, job_id: str, node_id: str) -> tuple[str, ...]:
        """Return the partition ids a node has work units for, in stable order."""
        ...

    def unsealed_statuses(self, job_id: str) -> frozenset[str]:
        """Return the distinct statuses of this job's unsealed work units."""
        ...

    def node_seals(self, job_id: str) -> tuple[NodeSeal, ...]: ...

    def eligible_work(self, job_id: str, *, limit: int) -> tuple[WorkUnitState, ...]:
        """Return at most ``limit`` work units whose dependencies are satisfied.

        The query is index-backed and bounded: its cost must not grow in
        proportion to the job's total partition count.
        """
        ...

    # -- transitions -------------------------------------------------------

    def reclaimable_work(
        self, job_id: str, *, limit: int, include_live: bool = False
    ) -> tuple[WorkUnitState, ...]:
        """Return work units a prior invocation left unfinished (executing or failed)."""
        ...

    def initialize_job(
        self,
        job_id: str,
        *,
        pipeline_id: str,
        source_mode: SourceMode,
        declaration_fingerprint: str,
        lineage: str = "default",
        snapshot_id: str | None = None,
        range_start: str | None = None,
        range_end: str | None = None,
    ) -> JobState: ...

    def register_discovery(
        self,
        job_id: str,
        *,
        expected_generation: int,
        partitions: Sequence[DiscoveredPartition] = (),
        extra_dependencies: Sequence[DependencyEdge] = (),
        next_position: str | None = None,
        seal: bool = False,
    ) -> JobState: ...

    def claim_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        expected_generation: int,
        lease_seconds: float | None = None,
        exclusive: bool = False,
    ) -> WorkUnitState: ...

    def renew_lease(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        lease_seconds: float,
    ) -> WorkUnitState:
        """Extend a live claim's lease as a heartbeat."""
        ...

    def release_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
    ) -> WorkUnitState:
        """Give up a claim without failing the unit."""
        ...

    def commit_batch(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        batch_id: str,
        output_ref: str,
        next_cursor: str | None = None,
        record_count: int = 0,
        input_exhausted: bool = False,
    ) -> WorkUnitState: ...

    def mark_input_exhausted(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        next_cursor: str | None = None,
    ) -> WorkUnitState:
        """Record end of the unit's selected input scope.

        End of input is a distinct durable result from an empty batch, so a
        unit with no output can still be sealed.
        """
        ...

    def seal_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
    ) -> WorkUnitState: ...

    def fail_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        error_type: str,
    ) -> WorkUnitState: ...

    def complete_job(
        self,
        job_id: str,
        *,
        expected_generation: int,
        checkpoint_position: str | None = None,
    ) -> JobState: ...


__all__ = ["DurableStore", "validate_capabilities"]
