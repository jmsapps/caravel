"""State, identity, and error contracts for the C-8 durable execution store.

This module defines the durable state machine's value types and its error
taxonomy. Its only Caravel dependency is the core type contracts, from which it
takes the one definition of ``InputScope`` that ``Step`` declares. The
authoritative schema and transition table live in the C-8 task record's
``references/storage-contract.md``.

Records carry identifiers, positions, counters, and output references only.
They never carry credentials, storage options, payloads, parameter values, or
raw error text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from ..types import InputScope

DURABLE_SCHEMA_VERSION = 2

MAX_IDENTIFIER_LENGTH = 512
MAX_OPAQUE_LENGTH = 65536

SourceMode = Literal["snapshot", "incremental"]
JobStatus = Literal["open", "completed"]
WorkUnitStatus = Literal["eligible", "executing", "sealed", "failed"]
ChangeOperation = Literal["insert", "update", "delete"]

SOURCE_MODES: frozenset[str] = frozenset({"snapshot", "incremental"})
JOB_STATUSES: frozenset[str] = frozenset({"open", "completed"})
INPUT_SCOPES: frozenset[str] = frozenset({"partition", "all"})
WORK_UNIT_STATUSES: frozenset[str] = frozenset({"eligible", "executing", "sealed", "failed"})
CHANGE_OPERATIONS: frozenset[str] = frozenset({"insert", "update", "delete"})


class DurabilityError(Exception):
    """Base class for every durable-store failure."""


class StorageCapabilityError(DurabilityError):
    """Raised when a backend cannot prove a required durability capability.

    Raised before any durable mutation, user code, or cleanup runs.
    """


class UnsupportedStoreVersionError(DurabilityError):
    """Raised when stored state carries an unsupported schema version."""


class GenerationConflictError(DurabilityError):
    """Raised when a conditional transition observes an unexpected generation."""


class FenceViolationError(DurabilityError):
    """Raised when a writer's ownership, claim epoch, fence token, or lease is stale."""


class InvalidTransitionError(DurabilityError):
    """Raised when a transition is attempted from an illegal source state."""


class WorkUnitUnavailableError(InvalidTransitionError):
    """Raised when a work unit can no longer be claimed.

    It is already sealed, or its dependencies are not satisfied. Under
    concurrency this is the ordinary signal that another worker got there
    first, so a competing worker moves to the next unit instead of failing.
    It subclasses :class:`InvalidTransitionError` because for a single worker
    it is still an illegal transition.
    """


class CommitUncertainError(DurabilityError):
    """Raised when a publication's outcome can be neither confirmed nor denied."""


class OutputIntegrityError(DurabilityError):
    """Raised when committed output is missing, corrupt, or contradicts its manifest.

    Loading never returns a partial payload and never treats missing committed
    output as empty.
    """


class StoreUnavailableError(DurabilityError):
    """Raised when the durable store cannot be reached or has been closed."""


def validate_identifier(name: str, value: object) -> str:
    """Return ``value`` as a bounded, non-empty identifier string."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string.")
    if len(value) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"{name} must be at most {MAX_IDENTIFIER_LENGTH} characters.")
    return value


def validate_optional_identifier(name: str, value: object) -> str | None:
    """Return ``value`` as a bounded identifier string, or None."""
    if value is None:
        return None
    return validate_identifier(name, value)


def validate_opaque(name: str, value: object) -> str:
    """Return ``value`` as a bounded opaque serialized token.

    Opaque tokens are source positions, cursors, and output references. The
    store never parses, compares, or orders them.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string.")
    if len(value) > MAX_OPAQUE_LENGTH:
        raise ValueError(f"{name} must be at most {MAX_OPAQUE_LENGTH} characters.")
    return value


def validate_optional_opaque(name: str, value: object) -> str | None:
    """Return ``value`` as a bounded opaque token, or None."""
    if value is None:
        return None
    return validate_opaque(name, value)


def validate_counter(name: str, value: object) -> int:
    """Return ``value`` as a non-negative integer counter."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")
    return value


def validate_member(name: str, value: object, allowed: frozenset[str]) -> str:
    """Return ``value`` when it is one of ``allowed``."""
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}.")
    return value


@dataclass(frozen=True)
class WorkUnitKey:
    """Durable identity of one partition-step pair."""

    job_id: str
    node_id: str
    partition_id: str

    def __post_init__(self) -> None:
        validate_identifier("job_id", self.job_id)
        validate_identifier("node_id", self.node_id)
        validate_identifier("partition_id", self.partition_id)


@dataclass(frozen=True)
class StoreCapabilities:
    """Durability capabilities a backend declares and must actually provide."""

    atomic_conditional_transitions: bool = False
    generation_validation: bool = False
    fencing_validation: bool = False
    durable_reads: bool = False
    transactional_publication: bool = False

    def missing(self) -> tuple[str, ...]:
        """Return the names of required capabilities this backend lacks."""
        return tuple(
            name
            for name in (
                "atomic_conditional_transitions",
                "generation_validation",
                "fencing_validation",
                "durable_reads",
                "transactional_publication",
            )
            if not getattr(self, name)
        )


@dataclass(frozen=True)
class JobState:
    """Durable state of one bounded logical run."""

    job_id: str
    pipeline_id: str
    lineage: str
    source_mode: SourceMode
    declaration_fingerprint: str
    snapshot_id: str | None = None
    range_start: str | None = None
    range_end: str | None = None
    discovery_position: str | None = None
    discovery_sealed: bool = False
    status: JobStatus = "open"
    generation: int = 0
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        validate_identifier("job_id", self.job_id)
        validate_identifier("pipeline_id", self.pipeline_id)
        validate_identifier("lineage", self.lineage)
        validate_member("source_mode", self.source_mode, SOURCE_MODES)
        validate_identifier("declaration_fingerprint", self.declaration_fingerprint)
        validate_optional_identifier("snapshot_id", self.snapshot_id)
        validate_optional_opaque("range_start", self.range_start)
        validate_optional_opaque("range_end", self.range_end)
        validate_optional_opaque("discovery_position", self.discovery_position)
        validate_member("status", self.status, JOB_STATUSES)
        validate_counter("generation", self.generation)


@dataclass(frozen=True)
class PartitionRecord:
    """A discovered, replayable unit of input."""

    job_id: str
    partition_id: str
    input_ref: str
    input_version: str | None = None
    entity_key: str | None = None
    operation: ChangeOperation | None = None
    source_revision: str | None = None
    source_position: str | None = None
    discovered_at: str = ""

    def __post_init__(self) -> None:
        validate_identifier("job_id", self.job_id)
        validate_identifier("partition_id", self.partition_id)
        validate_opaque("input_ref", self.input_ref)
        validate_optional_identifier("input_version", self.input_version)
        validate_optional_identifier("entity_key", self.entity_key)
        if self.operation is not None:
            validate_member("operation", self.operation, CHANGE_OPERATIONS)
        validate_optional_identifier("source_revision", self.source_revision)
        validate_optional_opaque("source_position", self.source_position)


@dataclass(frozen=True)
class WorkUnitState:
    """Durable state of one partition-step pair."""

    key: WorkUnitKey
    input_scope: InputScope
    step_revision: str
    params_fingerprint: str
    status: WorkUnitStatus = "eligible"
    cursor: str | None = None
    batch_count: int = 0
    input_exhausted: bool = False
    pending_deps: int = 0
    owner_id: str | None = None
    claim_epoch: int = 0
    fence_token: int = 0
    lease_expires_at: str | None = None
    generation: int = 0
    sealed_at: str | None = None
    error_type: str | None = None
    route_key: str | None = None
    """The committed branch route for this partition, once routing resolved."""
    output_generation: int = 0
    """Which generation of this unit's output is current. Invalidation bumps it,
    superseding earlier committed output without deleting it."""
    published_generation: int = -1
    """Which generation has been materialized into the published output tree.
    Equal to ``output_generation`` once published; never a second authority."""

    def __post_init__(self) -> None:
        validate_member("input_scope", self.input_scope, INPUT_SCOPES)
        validate_identifier("step_revision", self.step_revision)
        validate_identifier("params_fingerprint", self.params_fingerprint)
        validate_member("status", self.status, WORK_UNIT_STATUSES)
        validate_optional_opaque("cursor", self.cursor)
        validate_counter("batch_count", self.batch_count)
        validate_counter("pending_deps", self.pending_deps)
        validate_optional_identifier("owner_id", self.owner_id)
        validate_counter("claim_epoch", self.claim_epoch)
        validate_counter("fence_token", self.fence_token)
        validate_counter("generation", self.generation)
        validate_optional_identifier("error_type", self.error_type)
        validate_optional_identifier("route_key", self.route_key)
        validate_counter("output_generation", self.output_generation)


@dataclass(frozen=True)
class BatchRecord:
    """One committed batch: an immutable manifest entry."""

    position: int
    key: WorkUnitKey
    batch_id: str
    output_ref: str
    record_count: int = 0
    committed_at: str = ""
    output_generation: int = 0
    """The generation this batch was produced under."""

    def __post_init__(self) -> None:
        validate_counter("position", self.position)
        validate_identifier("batch_id", self.batch_id)
        validate_opaque("output_ref", self.output_ref)
        validate_counter("record_count", self.record_count)
        validate_counter("output_generation", self.output_generation)


@dataclass(frozen=True)
class NodeSeal:
    """A node's sealed final manifest position within one job."""

    job_id: str
    node_id: str
    final_position: int | None
    sealed_at: str = ""

    def __post_init__(self) -> None:
        validate_identifier("job_id", self.job_id)
        validate_identifier("node_id", self.node_id)
        if self.final_position is not None:
            validate_counter("final_position", self.final_position)


@dataclass(frozen=True)
class PipelineCheckpoint:
    """A pipeline lineage's durable completed-range checkpoint."""

    pipeline_id: str
    lineage: str
    completed_position: str | None = None
    generation: int = 0
    updated_at: str = ""

    def __post_init__(self) -> None:
        validate_identifier("pipeline_id", self.pipeline_id)
        validate_identifier("lineage", self.lineage)
        validate_optional_opaque("completed_position", self.completed_position)
        validate_counter("generation", self.generation)


@dataclass(frozen=True)
class DiscoveredWorkUnit:
    """A partition-step pair to register during discovery."""

    node_id: str
    step_revision: str
    params_fingerprint: str
    input_scope: InputScope = "partition"
    depends_on_nodes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_identifier("node_id", self.node_id)
        validate_identifier("step_revision", self.step_revision)
        validate_identifier("params_fingerprint", self.params_fingerprint)
        validate_member("input_scope", self.input_scope, INPUT_SCOPES)
        for node_id in self.depends_on_nodes:
            validate_identifier("depends_on_nodes entry", node_id)
        if len(set(self.depends_on_nodes)) != len(self.depends_on_nodes):
            raise ValueError("depends_on_nodes must not repeat a node id.")
        if self.node_id in self.depends_on_nodes:
            raise ValueError("A work unit must not depend on itself.")


@dataclass(frozen=True)
class DiscoveredPartition:
    """A replayable input partition and the work units it creates."""

    partition_id: str
    input_ref: str
    input_version: str | None = None
    entity_key: str | None = None
    operation: ChangeOperation | None = None
    source_revision: str | None = None
    source_position: str | None = None
    work_units: tuple[DiscoveredWorkUnit, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        validate_identifier("partition_id", self.partition_id)
        validate_opaque("input_ref", self.input_ref)
        validate_optional_identifier("input_version", self.input_version)
        validate_optional_identifier("entity_key", self.entity_key)
        if self.operation is not None:
            validate_member("operation", self.operation, CHANGE_OPERATIONS)
        validate_optional_identifier("source_revision", self.source_revision)
        validate_optional_opaque("source_position", self.source_position)
        node_ids = [unit.node_id for unit in self.work_units]
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("A partition must not declare the same node id twice.")


@dataclass(frozen=True)
class DependencyEdge:
    """One readiness edge from a dependent work unit to an upstream work unit."""

    node_id: str
    partition_id: str
    depends_on_node_id: str
    depends_on_partition_id: str

    def __post_init__(self) -> None:
        validate_identifier("node_id", self.node_id)
        validate_identifier("partition_id", self.partition_id)
        validate_identifier("depends_on_node_id", self.depends_on_node_id)
        validate_identifier("depends_on_partition_id", self.depends_on_partition_id)
        if (self.node_id, self.partition_id) == (
            self.depends_on_node_id,
            self.depends_on_partition_id,
        ):
            raise ValueError("A work unit must not depend on itself.")


__all__ = [
    "CHANGE_OPERATIONS",
    "DURABLE_SCHEMA_VERSION",
    "INPUT_SCOPES",
    "JOB_STATUSES",
    "MAX_IDENTIFIER_LENGTH",
    "MAX_OPAQUE_LENGTH",
    "SOURCE_MODES",
    "WORK_UNIT_STATUSES",
    "BatchRecord",
    "ChangeOperation",
    "CommitUncertainError",
    "DependencyEdge",
    "DiscoveredPartition",
    "DiscoveredWorkUnit",
    "DurabilityError",
    "FenceViolationError",
    "GenerationConflictError",
    "InputScope",
    "InvalidTransitionError",
    "JobState",
    "JobStatus",
    "NodeSeal",
    "OutputIntegrityError",
    "PartitionRecord",
    "PipelineCheckpoint",
    "SourceMode",
    "StorageCapabilityError",
    "StoreCapabilities",
    "StoreUnavailableError",
    "UnsupportedStoreVersionError",
    "WorkUnitKey",
    "WorkUnitState",
    "WorkUnitStatus",
    "WorkUnitUnavailableError",
    "validate_counter",
    "validate_identifier",
    "validate_member",
    "validate_opaque",
    "validate_optional_identifier",
    "validate_optional_opaque",
]
