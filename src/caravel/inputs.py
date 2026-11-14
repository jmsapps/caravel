"""The runtime input interface every processor receives.

A processor is ordinary processing code, not a declaration builder. The durable
engine is its only caller, so every invocation has a claimed lease, a durable
batch identity, and a transactional commit behind it. Nothing here runs during
planning.

One class, :class:`StepInputs`, is passed in every scope, and it yields handles
of one class, :class:`InputPartition`. ``input_scope`` on ``Step`` decides when
a step becomes ready and which handles it sees; it never changes the argument
type, the handle type, or their interfaces. A processor therefore cannot tell
its scope from what it was handed, which is what lets the same function run
under either declaration.

Payloads are loaded through the handle, on demand, so constructing the inputs
for an aggregate barrier over a large job does not materialize that job.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterator


class StepInputsError(Exception):
    """Raised when a processor uses its inputs as something they are not.

    Plan-shape problems raise ``UnsupportedDurableDeclarationError`` instead;
    this type covers what one processor did with its ``StepInputs`` object.
    """


_REPLACEMENT = (
    "A processor is ordinary processing code: iterate inputs.partitions() and "
    'return this unit\'s payload. Declare scope with Step(fn, input_scope="all") '
    "instead of declaring a managed input at runtime."
)


class InputPartition:
    """One input partition, readable on demand.

    ``partition_id`` is the input's declared identity: a real source partition
    keeps its own id, including a hierarchical one, and an input derived from an
    upstream barrier carries that step's declared output name. The reserved
    aggregate execution id is never exposed here.

    ``count`` is the number of records this handle covers, which is one for a
    whole non-sequence payload.
    """

    __slots__ = ("partition_id", "batch_id", "count", "_loader")

    def __init__(
        self,
        *,
        partition_id: str,
        batch_id: str,
        count: int,
        loader: Callable[[], Any],
    ) -> None:
        self.partition_id = partition_id
        self.batch_id = batch_id
        self.count = count
        self._loader = loader

    def load(self) -> Any:
        """Return this handle's input payload, in its dataset-defined type."""
        return self._loader()

    def __repr__(self) -> str:
        return (
            f"InputPartition(partition_id={self.partition_id!r}, "
            f"batch_id={self.batch_id!r}, count={self.count})"
        )


class StepInputs:
    """The input handed to every processor, in every scope.

    Under ``input_scope="partition"`` this holds exactly one handle: the unit's
    own input, or one durable batch of it when ``batch_size`` is set. Under
    ``input_scope="all"`` it holds every required upstream handle, or one
    contiguous batch of them. ``batch_id`` identifies the durable batch this
    call commits.
    """

    __slots__ = ("batch_id", "_handles")

    def __init__(self, *, batch_id: str, handles: tuple[InputPartition, ...]) -> None:
        self.batch_id = batch_id
        self._handles = handles

    def partitions(self) -> Iterator[InputPartition]:
        """Iterate this batch's handles without loading any payload.

        Every call returns a fresh iterator over the same handles, so a
        processor may iterate more than once.
        """
        return iter(self._handles)

    def __len__(self) -> int:
        return len(self._handles)

    @property
    def is_empty(self) -> bool:
        """True when this batch's sealed input contains no partitions."""
        return not self._handles

    def each(self, *args: Any, **kwargs: Any) -> Any:
        """Removed. Present only to fail an obsolete declaration builder clearly."""
        del args, kwargs
        raise StepInputsError(f"inputs.each(...) no longer exists. {_REPLACEMENT}")

    def all(self, *args: Any, **kwargs: Any) -> Any:
        """Removed. Present only to fail an obsolete declaration builder clearly."""
        del args, kwargs
        raise StepInputsError(f"inputs.all(...) no longer exists. {_REPLACEMENT}")

    def __repr__(self) -> str:
        return f"StepInputs(batch_id={self.batch_id!r}, partitions={len(self._handles)})"


_SENTINEL = object()


def validate_processor_signature(fn: Callable[..., Any], *, what: str) -> None:
    """Reject a processor that cannot be called as ``fn(inputs, context=...)``.

    The check binds a signature; it never invokes ``fn``, so validation is safe
    before source discovery and before any durable mutation. ``functools.partial``
    and callable objects are supported, because :func:`inspect.signature` already
    resolves both. A callable whose signature cannot be introspected is accepted
    rather than rejected: refusing a working step is worse than deferring the
    error to its first call. Annotations are read by nothing here -- a signature
    decides call compatibility only, never an execution mode.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return
    try:
        signature.bind(_SENTINEL, context=_SENTINEL)
    except TypeError as error:
        raise TypeError(
            f"{what} must be callable as fn(inputs, context=...): one positional "
            f"input argument and a keyword-only context argument. {error}"
        ) from error


__all__ = [
    "InputPartition",
    "StepInputs",
    "StepInputsError",
    "validate_processor_signature",
]
