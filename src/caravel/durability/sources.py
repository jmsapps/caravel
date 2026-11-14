"""Declared in-memory sources for durable partition execution.

This module provides the minimal source shape ST-3 needs to exercise durable
scheduling. Real snapshot and incremental adapters, bounded range allocation,
and replay contracts are a later concern; the protocol here is deliberately
small so generalizing it does not require rewriting the scheduler.

An in-memory source cannot replay historical contents by itself, so it captures
each partition's payload durably at discovery, exactly as the execution contract
requires of any non-replayable source.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .types import (
    CHANGE_OPERATIONS,
    SourceMode,
    ChangeOperation,
    DurabilityError,
    validate_identifier,
    validate_member,
    validate_opaque,
)

SINGLE_PARTITION_ID = "__single__"
AGGREGATE_PARTITION_ID = "__all__"


class SourceError(DurabilityError):
    """Raised when a source cannot produce replayable durable input."""


class SourceVersionError(SourceError):
    """Raised when pinned input changed instead of staying immutable.

    A pinned snapshot is validated before any processing, and a partition's
    recorded version is revalidated when that partition is read, so a changed
    partition inside an unchanged snapshot is caught too.
    """


class RetentionGapError(SourceError):
    """Raised when a persisted range is no longer replayable from the source.

    Positions are never silently reset. Recovery requires an
    application-approved resnapshot or explicit replay lineage.
    """


@dataclass(frozen=True)
class SourcePartition:
    """One independently recoverable unit of input."""

    partition_id: str
    input_ref: str
    input_version: str | None = None
    entity_key: str | None = None
    operation: ChangeOperation | None = None
    source_revision: str | None = None
    source_position: str | None = None

    def __post_init__(self) -> None:
        validate_identifier("partition_id", self.partition_id)
        if self.partition_id == AGGREGATE_PARTITION_ID:
            raise ValueError(
                f"'{AGGREGATE_PARTITION_ID}' is reserved for all-input aggregate output."
            )


@dataclass(frozen=True)
class ChangeEvent:
    """One replayable change from an incremental source."""

    position: str
    entity_key: str
    operation: ChangeOperation
    revision: str
    payload_ref: str

    def __post_init__(self) -> None:
        validate_opaque("position", self.position)
        validate_identifier("entity_key", self.entity_key)
        validate_member("operation", self.operation, CHANGE_OPERATIONS)
        validate_identifier("revision", self.revision)
        validate_opaque("payload_ref", self.payload_ref)


@dataclass(frozen=True)
class AllocatedRange:
    """Bounds selected for one incremental run.

    ``start`` and ``end`` are opaque adapter positions. The engine stores and
    returns them and never compares, orders, or does arithmetic on them, so
    vector and sharded positions work without engine changes.
    """

    start: str
    end: str

    def __post_init__(self) -> None:
        validate_opaque("range start", self.start)
        validate_opaque("range end", self.end)


@runtime_checkable
class SnapshotAdapter(Protocol):
    """Supplies an immutable, identity-verifiable snapshot of input."""

    def current_snapshot_id(self) -> str:
        """Return the adapter's current snapshot identity."""
        ...

    def discover(self, snapshot_id: str) -> tuple[SourcePartition, ...]: ...

    def load(self, snapshot_id: str, partition: SourcePartition) -> Any: ...


@runtime_checkable
class IncrementalAdapter(Protocol):
    """Supplies a replayable, bounded change feed."""

    def allocate_range(self, after: str | None, *, max_events: int) -> AllocatedRange | None:
        """Select bounds for the next run, or None when caught up."""
        ...

    def is_replayable(self, allocated: AllocatedRange) -> bool:
        """Whether this exact range can still be read from the source."""
        ...

    def read_range(self, allocated: AllocatedRange) -> tuple[ChangeEvent, ...]: ...

    def load(self, event_ref: str) -> Any: ...


@runtime_checkable
class PartitionSource(Protocol):
    """Supplies durable, replayable execution partitions."""

    def discover(self) -> tuple[SourcePartition, ...]:
        """Return every partition of the selected input."""
        ...

    def load(self, partition: SourcePartition) -> Any:
        """Reconstruct a partition's payload from its replayable reference."""
        ...


class InMemorySource:
    """A declared in-memory source that captures its payloads at discovery.

    ``InMemorySource(record=...)`` declares no partitions, so its input is one
    partition. That preserves simple function-based declarations: an unsplit
    input is a single recovery unit and the runner never infers how to split it.

    ``InMemorySource(partitions={...})`` declares explicit stable partitions.
    """

    def __init__(
        self,
        partitions: Mapping[str, Any] | None = None,
        *,
        record: Any = None,
        name: str = "memory",
    ) -> None:
        if partitions is not None and record is not None:
            raise ValueError("Declare either partitions or a single record, not both.")
        self.name = validate_identifier("source name", name)
        if partitions is None:
            self._records: dict[str, Any] = {SINGLE_PARTITION_ID: record}
            self.declared_partitions = False
        else:
            if not isinstance(partitions, Mapping):
                raise TypeError("InMemorySource partitions must be a mapping.")
            self._records = dict(partitions)
            self.declared_partitions = True

    def discover(self) -> tuple[SourcePartition, ...]:
        """Capture each partition's payload durably and return its identity."""
        discovered: list[SourcePartition] = []
        for partition_id in sorted(self._records):
            try:
                captured = json.dumps(self._records[partition_id], ensure_ascii=False)
            except (TypeError, ValueError) as error:
                raise SourceError(
                    f"Source '{self.name}' cannot durably capture partition "
                    f"'{partition_id}'; its payload is not JSON-serializable and the "
                    "source cannot replay historical contents."
                ) from error
            discovered.append(SourcePartition(partition_id=partition_id, input_ref=captured))
        return tuple(discovered)

    def load(self, partition: SourcePartition) -> Any:
        """Reconstruct a payload from the reference captured at discovery."""
        try:
            return json.loads(partition.input_ref)
        except ValueError as error:
            raise SourceError(
                f"Source '{self.name}' captured an unreadable reference for partition "
                f"'{partition.partition_id}'."
            ) from error


__all__ = [
    "AGGREGATE_PARTITION_ID",
    "SINGLE_PARTITION_ID",
    "AllocatedRange",
    "ChangeEvent",
    "InMemorySource",
    "IncrementalAdapter",
    "LoaderSource",
    "IncrementalInput",
    "ListChangeAdapter",
    "ListSnapshotAdapter",
    "PartitionSource",
    "RetentionGapError",
    "SnapshotAdapter",
    "SnapshotInput",
    "SourceError",
    "SourcePartition",
    "SourceVersionError",
    "as_source",
]


class SnapshotInput:
    """A pinned snapshot of input, validated before any processing.

    The pin is checked twice. The adapter's currently reported snapshot
    identity must still equal the pinned one before work starts, and each
    partition's recorded version is revalidated when that partition is read, so
    a changed partition inside an unchanged snapshot is caught too.
    """

    source_mode: SourceMode = "snapshot"

    def __init__(self, adapter: SnapshotAdapter, *, snapshot: str) -> None:
        if not isinstance(adapter, SnapshotAdapter):
            raise TypeError("SnapshotInput requires a snapshot adapter.")
        self.adapter = adapter
        self.snapshot = validate_identifier("snapshot", snapshot)
        self._versions: dict[str, str | None] = {}

    def validate_pin(self) -> None:
        """Reject a snapshot whose identity moved under the pin."""
        current = self.adapter.current_snapshot_id()
        if current != self.snapshot:
            raise SourceVersionError(
                f"Pinned snapshot '{self.snapshot}' is no longer the adapter's current "
                f"snapshot ('{current}'). A changed pinned input fails validation "
                "rather than silently reading a newer version."
            )

    def discover(self) -> tuple[SourcePartition, ...]:
        self.validate_pin()
        discovered = self.adapter.discover(self.snapshot)
        self._versions = {
            partition.partition_id: partition.input_version for partition in discovered
        }
        return discovered

    def load(self, partition: SourcePartition) -> Any:
        self.validate_pin()
        current = self.adapter.discover(self.snapshot)
        versions = {item.partition_id: item.input_version for item in current}
        if partition.partition_id not in versions:
            raise SourceVersionError(
                f"Partition '{partition.partition_id}' is no longer present in pinned "
                f"snapshot '{self.snapshot}'."
            )
        if versions[partition.partition_id] != partition.input_version:
            raise SourceVersionError(
                f"Partition '{partition.partition_id}' changed version under pinned "
                f"snapshot '{self.snapshot}'; committed input identity is immutable."
            )
        return self.adapter.load(self.snapshot, partition)


class IncrementalInput:
    """A bounded, replayable range of a change feed.

    Bounds are allocated once and then read from durable state forever. A
    resumed job never re-asks the adapter for bounds, which is what prevents a
    retried job from skipping or expanding its range.
    """

    source_mode: SourceMode = "incremental"

    def __init__(self, adapter: IncrementalAdapter, *, max_events: int = 1000) -> None:
        if not isinstance(adapter, IncrementalAdapter):
            raise TypeError("IncrementalInput requires an incremental adapter.")
        if not isinstance(max_events, int) or isinstance(max_events, bool) or max_events < 1:
            raise ValueError("IncrementalInput max_events must be a positive integer.")
        self.adapter = adapter
        self.max_events = max_events
        self._allocated: AllocatedRange | None = None

    @property
    def allocated(self) -> AllocatedRange | None:
        return self._allocated

    def allocate(self, after: str | None) -> AllocatedRange | None:
        """Select bounds for a job that has none. Returns None when caught up."""
        allocated = self.adapter.allocate_range(after, max_events=self.max_events)
        self._allocated = allocated
        return allocated

    def adopt(self, start: str, end: str) -> AllocatedRange:
        """Reuse bounds already persisted for this job, validating replayability."""
        allocated = AllocatedRange(start=start, end=end)
        if not self.adapter.is_replayable(allocated):
            raise RetentionGapError(
                f"Range [{start}, {end}] is no longer available from the source. "
                "Positions are never silently reset; an application-approved "
                "resnapshot or explicit replay lineage is required."
            )
        self._allocated = allocated
        return allocated

    def discover(self) -> tuple[SourcePartition, ...]:
        """Turn this range's change events into replayable execution partitions."""
        if self._allocated is None:
            raise SourceError("IncrementalInput has no allocated range to discover.")
        partitions: list[SourcePartition] = []
        for index, event in enumerate(self.adapter.read_range(self._allocated)):
            partitions.append(
                SourcePartition(
                    partition_id=f"{index:08d}-{event.entity_key}",
                    input_ref=event.payload_ref,
                    input_version=event.revision,
                    entity_key=event.entity_key,
                    operation=event.operation,
                    source_revision=event.revision,
                    source_position=event.position,
                )
            )
        return tuple(partitions)

    def load(self, partition: SourcePartition) -> Any:
        return self.adapter.load(partition.input_ref)


class ListSnapshotAdapter:
    """A small reference snapshot adapter over in-memory versioned records."""

    def __init__(self, records: Mapping[str, Any], *, snapshot_id: str = "v1") -> None:
        self._records = dict(records)
        self._snapshot_id = snapshot_id

    def set_snapshot(self, snapshot_id: str, records: Mapping[str, Any]) -> None:
        """Publish a new snapshot identity and contents."""
        self._snapshot_id = snapshot_id
        self._records = dict(records)

    def replace_record(self, partition_id: str, record: Any) -> None:
        """Change one record's contents without changing the snapshot identity."""
        self._records[partition_id] = record

    def current_snapshot_id(self) -> str:
        return self._snapshot_id

    def discover(self, snapshot_id: str) -> tuple[SourcePartition, ...]:
        return tuple(
            SourcePartition(
                partition_id=partition_id,
                input_ref=json.dumps(self._records[partition_id], ensure_ascii=False),
                input_version=_content_version(self._records[partition_id]),
            )
            for partition_id in sorted(self._records)
        )

    def load(self, snapshot_id: str, partition: SourcePartition) -> Any:
        del snapshot_id
        return json.loads(partition.input_ref)


class ListChangeAdapter:
    """A small reference incremental adapter over an ordered event list.

    Positions are opaque strings. The adapter owns ordering and inclusivity;
    the engine never interprets them.
    """

    def __init__(self, events: Sequence[ChangeEvent] = (), *, retained_from: int = 0) -> None:
        self._events = list(events)
        self._retained_from = retained_from

    def append(self, *events: ChangeEvent) -> None:
        self._events.extend(events)

    def expire_through(self, index: int) -> None:
        """Simulate source retention expiring everything before ``index``."""
        self._retained_from = index

    def _index_of(self, position: str) -> int:
        for index, event in enumerate(self._events):
            if event.position == position:
                return index
        raise RetentionGapError(f"Position '{position}' is not available from the source.")

    def allocate_range(self, after: str | None, *, max_events: int) -> AllocatedRange | None:
        start_index = 0 if after is None else self._index_of(after) + 1
        if start_index >= len(self._events):
            return None
        end_index = min(start_index + max_events, len(self._events)) - 1
        return AllocatedRange(
            start=self._events[start_index].position,
            end=self._events[end_index].position,
        )

    def is_replayable(self, allocated: AllocatedRange) -> bool:
        try:
            start_index = self._index_of(allocated.start)
            self._index_of(allocated.end)
        except RetentionGapError:
            return False
        return start_index >= self._retained_from

    def read_range(self, allocated: AllocatedRange) -> tuple[ChangeEvent, ...]:
        if not self.is_replayable(allocated):
            raise RetentionGapError(
                f"Range [{allocated.start}, {allocated.end}] has expired from the source."
            )
        start_index = self._index_of(allocated.start)
        end_index = self._index_of(allocated.end)
        return tuple(self._events[start_index : end_index + 1])

    def load(self, event_ref: str) -> Any:
        return json.loads(event_ref)


def _content_version(record: Any) -> str:
    """Return a stable content version for a reference snapshot record."""
    return hashlib.sha256(
        json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


class LoaderSource:
    """Adapts a :class:`~caravel.types.Loader` into a durable partition source.

    A loader already returns records keyed by partition id, which is exactly an
    execution partition map. Loaders therefore remain a valid way to describe
    input under the one durable execution model; they are not a second path.
    """

    def __init__(self, loader: Any, *, name: str | None = None) -> None:
        if not hasattr(loader, "load"):
            raise TypeError(
                f"LoaderSource requires a Loader with load(), got {type(loader).__name__}."
            )
        self.loader = loader
        self.name = validate_identifier(
            "source name", name or getattr(loader, "name", None) or "loader"
        )

    def discover(self) -> tuple[SourcePartition, ...]:
        partitions = self.loader.load()
        if not isinstance(partitions, Mapping):
            raise SourceError(
                f"Loader '{self.name}' must return a mapping of partition id to record, "
                f"got {type(partitions).__name__}."
            )
        discovered: list[SourcePartition] = []
        for partition_id in sorted(partitions):
            try:
                captured = json.dumps(partitions[partition_id], ensure_ascii=False)
            except (TypeError, ValueError) as error:
                raise SourceError(
                    f"Loader '{self.name}' cannot durably capture partition "
                    f"'{partition_id}'; its record is not JSON-serializable."
                ) from error
            discovered.append(SourcePartition(partition_id=str(partition_id), input_ref=captured))
        return tuple(discovered)

    def load(self, partition: SourcePartition) -> Any:
        try:
            return json.loads(partition.input_ref)
        except ValueError as error:
            raise SourceError(
                f"Loader '{self.name}' captured an unreadable reference for partition "
                f"'{partition.partition_id}'."
            ) from error


def as_source(candidate: Any) -> Any:
    """Return ``candidate`` as a partition source, adapting a loader if needed."""
    if candidate is None:
        raise SourceError(
            "No input source. Supply source= to run(), or declare one on the Pipeline."
        )
    if hasattr(candidate, "discover"):
        return candidate
    if hasattr(candidate, "load"):
        return LoaderSource(candidate)
    raise TypeError(f"{type(candidate).__name__} is neither a partition source nor a Loader.")
