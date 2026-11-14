"""The durable batch commit protocol.

One committed batch is produced by this exact sequence:

1. Read committed state and its generation.
2. Process the batch at the committed cursor (the caller's responsibility).
3. Write output to a unique immutable attempt location and verify persistence.
4. In one authoritative transaction, validate ownership, fencing token, lease
   expiry, and expected generation; publish the manifest reference and advance
   progress together.
5. Only after publication is the batch complete.

If the publication's outcome is lost, authoritative state is reloaded rather
than guessed. A visible matching record means the commit succeeded. A clearly
absent record means it did not. An unreadable outcome raises
:class:`CommitUncertainError`; progress is never advanced speculatively.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..types import Dataset
from .output import AttemptOutputStore, OutputReference
from .sqlite import SQLiteDurableStore
from .types import (
    CommitUncertainError,
    DurabilityError,
    OutputIntegrityError,
    WorkUnitKey,
    WorkUnitState,
    validate_identifier,
)

# Deterministic failure injection for crash and ack-loss tests. Production code
# leaves the hook unset.
_failpoint_hook: Callable[[str], None] | None = None


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


@dataclass(frozen=True)
class BatchCommit:
    """The durable outcome of one committed batch."""

    key: WorkUnitKey
    batch_id: str
    reference: OutputReference
    state: WorkUnitState
    reconciled: bool = False
    """True when the outcome was recovered from authoritative state after a
    lost acknowledgment rather than observed directly."""


class BatchPublisher:
    """Publishes immutable batch output under the authoritative commit protocol."""

    def __init__(self, store: SQLiteDurableStore, output: AttemptOutputStore) -> None:
        self._store = store
        self._output = output

    @property
    def store(self) -> SQLiteDurableStore:
        return self._store

    @property
    def output(self) -> AttemptOutputStore:
        return self._output

    def publish(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        batch_id: str,
        dataset: Dataset,
        payload: Any,
        next_cursor: str | None = None,
        record_count: int = 0,
        input_exhausted: bool = False,
    ) -> BatchCommit:
        """Write immutable attempt output, then publish it conditionally."""
        validate_identifier("owner_id", owner_id)
        validate_identifier("batch_id", batch_id)

        already = self._already_committed(key, batch_id)
        if already is not None:
            return already

        _fire_failpoint("before_attempt_write")
        reference = self._output.write_attempt(key, batch_id, dataset=dataset, payload=payload)
        _fire_failpoint("after_attempt_write")

        try:
            state = self._store.commit_batch(
                key,
                owner_id=owner_id,
                fence_token=fence_token,
                expected_generation=expected_generation,
                batch_id=batch_id,
                output_ref=reference.to_json(),
                next_cursor=next_cursor,
                record_count=record_count,
                input_exhausted=input_exhausted,
            )
        except DurabilityError:
            raise
        except Exception as publication_error:
            recovered = self._reconcile(key, batch_id, reference, publication_error)
            return recovered
        _fire_failpoint("after_publication")
        return BatchCommit(key=key, batch_id=batch_id, reference=reference, state=state)

    def _already_committed(self, key: WorkUnitKey, batch_id: str) -> BatchCommit | None:
        """Return the committed outcome when this batch was already published.

        A replayed batch continues from its committed record instead of writing
        another attempt. Committed output is immutable, so republishing could
        only produce an unreferenced duplicate.
        """
        record = self._store.lookup_batch(key, batch_id)
        if record is None:
            return None
        state = self._store.load_work_unit(key)
        if state is None:
            raise CommitUncertainError(
                f"Batch '{batch_id}' is committed for node='{key.node_id}' "
                f"partition='{key.partition_id}' but its work unit is absent."
            )
        return BatchCommit(
            key=key,
            batch_id=batch_id,
            reference=OutputReference.from_json(record.output_ref),
            state=state,
            reconciled=True,
        )

    def _reconcile(
        self,
        key: WorkUnitKey,
        batch_id: str,
        reference: OutputReference,
        publication_error: Exception,
    ) -> BatchCommit:
        """Resolve a publication whose acknowledgment was lost."""
        try:
            committed = self._store.lookup_batch(key, batch_id)
            state = self._store.load_work_unit(key)
        except Exception as reload_error:
            raise CommitUncertainError(
                f"Publication outcome for batch '{batch_id}' of node='{key.node_id}' "
                f"partition='{key.partition_id}' could not be established; authoritative "
                "state is unreadable. Progress was not advanced."
            ) from reload_error

        if committed is None or state is None:
            raise publication_error

        if committed.output_ref != reference.to_json():
            raise CommitUncertainError(
                f"Batch '{batch_id}' of node='{key.node_id}' partition='{key.partition_id}' "
                "is committed against a different output reference than this attempt "
                "produced. Progress was not advanced."
            )

        return BatchCommit(
            key=key,
            batch_id=batch_id,
            reference=reference,
            state=state,
            reconciled=True,
        )

    # -- manifest-backed reading -------------------------------------------

    def committed_reference(self, key: WorkUnitKey, batch_id: str) -> OutputReference | None:
        """Return a batch's committed reference, or None when uncommitted."""
        record = self._store.lookup_batch(key, batch_id)
        if record is None:
            return None
        return OutputReference.from_json(record.output_ref)

    def committed_references(self, key: WorkUnitKey) -> tuple[OutputReference, ...]:
        """Return a work unit's committed references in commit order."""
        return tuple(
            OutputReference.from_json(record.output_ref)
            for record in self._store.work_unit_batches(key)
        )

    def load_work_unit(self, key: WorkUnitKey, *, dataset: Dataset) -> tuple[Any, ...]:
        """Load every committed batch payload of a work unit, in commit order.

        Each batch is returned separately. How batches combine into a logical
        result is the caller's contract, not the manifest's.
        """
        return tuple(
            self._output.load(reference, dataset=dataset)
            for reference in self.committed_references(key)
        )

    def load_batch(self, key: WorkUnitKey, batch_id: str, *, dataset: Dataset) -> Any:
        """Load one committed batch's payload through the declared dataset."""
        reference = self.committed_reference(key, batch_id)
        if reference is None:
            raise OutputIntegrityError(
                f"Batch '{batch_id}' of node='{key.node_id}' partition='{key.partition_id}' "
                "has no committed output reference."
            )
        return self._output.load(reference, dataset=dataset)


__all__ = ["BatchCommit", "BatchPublisher"]
