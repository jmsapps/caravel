"""The committed batch feed for independent consumers.

The committed manifest is authoritative. A consumer reads committed references
after its own durable position, applies them to its destination, and only then
acknowledges. A crash between application and acknowledgment replays that batch,
which is why destinations must be idempotent or transactional.

Producer cursors and consumer positions are entirely separate: acknowledging a
batch never modifies producer progress, and one consumer's failure never blocks
another.

An empty feed while production is ongoing means "no new batches yet". It is a
different state from a sealed empty result, and the two are distinguishable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..types import Dataset
from .identity import logical_output_id
from .output import AttemptOutputStore, OutputReference
from .sqlite import SQLiteDurableStore
from .types import DurabilityError, validate_counter, validate_identifier

DEFAULT_PAGE_SIZE = 100


class RetentionExpiredError(DurabilityError):
    """Raised when a lagging consumer reads below the job's retention floor.

    Retention expiry is explicit to lagging consumers rather than silently
    skipping unavailable batches.
    """


@dataclass(frozen=True)
class FeedEntry:
    """One committed batch, as seen by a consumer.

    ``logical_id`` is how a consumer addresses this output: a source
    partition's own identity, or an all-input step's declared dataset name.
    ``partition_id`` is the execution identity underneath it, kept for
    diagnostics and for correlating with recovery state.
    """

    position: int
    job_id: str
    node_id: str
    partition_id: str
    logical_id: str
    batch_id: str
    reference: OutputReference
    record_count: int
    output_generation: int = 0
    """Which generation produced this entry. A higher generation for the same
    ``(partition_id, batch_id)`` supersedes an earlier one, which is how a
    consumer tells a recomputed result from new data."""
    entity_key: str | None = None
    operation: str | None = None
    source_revision: str | None = None


@dataclass(frozen=True)
class FeedPage:
    """A bounded page of committed entries plus the feed's completion state."""

    entries: tuple[FeedEntry, ...]
    next_position: int
    node_sealed: bool
    job_completed: bool
    final_position: int | None = None

    @property
    def is_empty(self) -> bool:
        return not self.entries

    @property
    def at_end(self) -> bool:
        """Whether this consumer has reached the node's sealed final position."""
        if not self.node_sealed:
            return False
        if self.final_position is None:
            return True
        return self.next_position >= self.final_position


class BatchFeed:
    """A bounded, ordered reader over one node's committed batch output."""

    def __init__(
        self,
        store: SQLiteDurableStore,
        *,
        pipeline_id: str,
        job_id: str,
        node_id: str,
        output: AttemptOutputStore | None = None,
        node: Any = None,
    ) -> None:
        """``node`` is the declaration whose output this feed carries.

        It is what resolves an entry's logical identity.
        :meth:`caravel.durability.JobResults.feed` always supplies it. Without
        it the feed reports execution identity, which is all an unnamed node
        has; that is the low-level reading, not the supported addressing.
        """
        self._store = store
        self.pipeline_id = validate_identifier("pipeline_id", pipeline_id)
        self.job_id = validate_identifier("job_id", job_id)
        self.node_id = validate_identifier("node_id", node_id)
        self._output = output
        self._node = node

    # -- retention ---------------------------------------------------------

    def retention_floor(self) -> int:
        """Return the lowest commit position still guaranteed available.

        The floor is declarative data, not an inference from the filesystem, so
        correctness never depends on whether cleanup has run.
        """
        return self._store.retention_floor(self.job_id)

    def _check_retention(self, after_position: int) -> None:
        floor = self.retention_floor()
        if floor > 0 and after_position < floor:
            raise RetentionExpiredError(
                f"Consumer position {after_position} is below the retention floor "
                f"{floor} for job '{self.job_id}'. Expired batches are reported "
                "explicitly rather than silently skipped."
            )

    def _logical_id(self, partition_id: str) -> str:
        if self._node is None:
            return partition_id
        return logical_output_id(self._node, partition_id)

    # -- reading -----------------------------------------------------------

    def read(self, *, after_position: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> FeedPage:
        """Return committed entries after a position, in order, bounded by ``limit``."""
        validate_counter("after_position", after_position)
        self._check_retention(after_position)

        records = self._store.list_committed_batches(
            self.job_id, self.node_id, after_position=after_position, limit=limit
        )
        entries: list[FeedEntry] = []
        for record in records:
            partition = self._store.load_partition(self.job_id, record.key.partition_id)
            entries.append(
                FeedEntry(
                    position=record.position,
                    job_id=record.key.job_id,
                    node_id=record.key.node_id,
                    partition_id=record.key.partition_id,
                    logical_id=self._logical_id(record.key.partition_id),
                    batch_id=record.batch_id,
                    reference=OutputReference.from_json(record.output_ref),
                    record_count=record.record_count,
                    output_generation=record.output_generation,
                    entity_key=None if partition is None else partition.entity_key,
                    operation=None if partition is None else partition.operation,
                    source_revision=None if partition is None else partition.source_revision,
                )
            )

        seals = {seal.node_id: seal for seal in self._store.node_seals(self.job_id)}
        seal = seals.get(self.node_id)
        job = self._store.load_job(self.job_id)
        return FeedPage(
            entries=tuple(entries),
            next_position=entries[-1].position if entries else after_position,
            node_sealed=seal is not None,
            job_completed=job is not None and job.status == "completed",
            final_position=None if seal is None else seal.final_position,
        )

    def load(self, entry: FeedEntry, *, dataset: Dataset) -> Any:
        """Load one entry's committed payload through the declared dataset."""
        if self._output is None:
            raise DurabilityError(
                "This feed was constructed without an output store and cannot load payloads."
            )
        return self._output.load(entry.reference, dataset=dataset)

    # -- consumer positions ------------------------------------------------

    def position_of(self, consumer_id: str) -> int:
        """Return one consumer's own durable position, zero when it has none."""
        stored = self._store.load_consumer_position(
            self.pipeline_id, consumer_id, self.job_id, self.node_id
        )
        return 0 if stored is None else stored

    def acknowledge(self, consumer_id: str, position: int) -> int:
        """Record that a consumer durably applied everything through ``position``.

        Acknowledgment is the consumer's own state. It never advances, rolls
        back, or otherwise touches producer progress.
        """
        return self._store.acknowledge_consumer_position(
            self.pipeline_id, consumer_id, self.job_id, self.node_id, position=position
        )

    def read_for(self, consumer_id: str, *, limit: int = DEFAULT_PAGE_SIZE) -> FeedPage:
        """Read the next bounded page for one consumer, from its own position."""
        validate_identifier("consumer_id", consumer_id)
        return self.read(after_position=self.position_of(consumer_id), limit=limit)


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "BatchFeed",
    "FeedEntry",
    "FeedPage",
    "RetentionExpiredError",
]
