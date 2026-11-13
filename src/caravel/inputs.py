"""Managed input declarations: one descriptor per declared step.

A processor is a declaration builder, not a computation. Compilation invokes it
once with an :class:`Inputs` builder and a read-only :class:`DeclarationContext`,
before source discovery and before any durable mutation. Its only permitted
result is the descriptor returned by exactly one ``inputs.each(...)`` or
``inputs.all(...)`` call.

``each`` and ``all`` register a descriptor in memory. They do not iterate data
and they do not invoke callbacks: the durable engine is the only caller of a
managed callback, so every invocation has a claimed lease, a durable batch
identity, and a transactional commit behind it.

Builders must be deterministic and free of I/O and side effects. This API cannot
sandbox arbitrary Python, so the restriction is a contract rather than an
enforcement: a builder that reads a database or mutates global state will be
re-run on every resume and its effects will not be replayed durably. What the
builder *can* reach is deliberately empty of data -- the builder exposes only
``each`` and ``all``, and the declaration context carries names and parameters
only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence

from .retry import RetryOptions

HelperKind = Literal["each", "all"]


class ManagedInputError(Exception):
    """Raised when a declaration builder violates the managed-input contract.

    Plan-shape problems raise ``UnsupportedDurableDeclarationError`` instead;
    this type covers what one builder did with its ``Inputs`` object.
    """


@dataclass(frozen=True)
class DeclarationContext:
    """Read-only context handed to a declaration builder at compilation.

    It carries declaration identity and invocation parameters only. There is no
    partition payload, storage handle, worker identity, or live retry control
    here, because none of those exist yet when a builder runs.
    """

    pipeline_name: str
    stage_name: str
    stage_index: int
    step_name: str
    step_index: int
    params: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Copy before freezing so neither builders nor the original caller can
        # change another declaration's configuration through a shared mapping.
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))


@dataclass(frozen=True)
class ManagedDescriptor:
    """One step's declared managed work.

    ``issuer`` identifies the builder that produced this descriptor, so a
    descriptor returned by some other builder is rejected by identity rather
    than by structural resemblance.
    """

    kind: HelperKind
    callback: Callable[..., Any]
    retry: RetryOptions | None
    issuer: object

    def __post_init__(self) -> None:
        if not callable(self.callback):
            raise ManagedInputError(
                f"A managed callback must be callable, got {type(self.callback).__name__}."
            )
        if self.retry is not None and not isinstance(self.retry, RetryOptions):
            raise ManagedInputError(
                f"retry= must be a RetryOptions or None, got {type(self.retry).__name__}."
            )


_NO_DATA = (
    "The declaration builder exposes no data. It runs before source discovery, "
    "so there is nothing to read, iterate, or index. Declare the work with "
    "inputs.each(callback) or inputs.all(callback) and read the input inside "
    "the callback instead."
)


class Inputs:
    """The declaration builder passed to every processor.

    Exactly one of :meth:`each` or :meth:`all` may be called, once. Everything
    else raises :class:`ManagedInputError`, including attribute access,
    iteration, indexing, and length, so a processor that tries to compute
    during declaration fails at compilation rather than at execution.
    """

    __slots__ = ("_issuer", "_descriptor")

    def __init__(self) -> None:
        object.__setattr__(self, "_issuer", object())
        object.__setattr__(self, "_descriptor", None)

    def each(
        self, callback: Callable[..., Any], *, retry: RetryOptions | None = None
    ) -> ManagedDescriptor:
        """Declare per-partition work, preserving existing partition identities.

        The callback receives a :class:`PartitionInput` for one durable batch
        and returns that partition-step pair's committed output.
        """
        return self._register("each", callback, retry)

    def all(
        self, callback: Callable[..., Any], *, retry: RetryOptions | None = None
    ) -> ManagedDescriptor:
        """Declare the aggregate barrier over every required upstream commit.

        The callback receives an :class:`AggregateInput` reader and returns one
        logical aggregate payload.
        """
        return self._register("all", callback, retry)

    def _register(
        self, kind: HelperKind, callback: Callable[..., Any], retry: RetryOptions | None
    ) -> ManagedDescriptor:
        if self._descriptor is not None:
            raise ManagedInputError(
                "A processor declares exactly one managed input. This builder already "
                f"declared inputs.{self._descriptor.kind}(...). Multiple helpers, "
                "registration loops, and runtime-dependent helper kinds are not "
                "supported; split the work into separate steps instead."
            )
        descriptor = ManagedDescriptor(
            kind=kind, callback=callback, retry=retry, issuer=self._issuer
        )
        object.__setattr__(self, "_descriptor", descriptor)
        return descriptor

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            # Protocol probes (copy, pickle, inspect) must see a normal miss.
            raise AttributeError(name)
        raise ManagedInputError(f"'Inputs' has no attribute '{name}'. {_NO_DATA}")

    def __iter__(self) -> Iterator[Any]:
        raise ManagedInputError(_NO_DATA)

    def __len__(self) -> int:
        raise ManagedInputError(_NO_DATA)

    def __getitem__(self, key: Any) -> Any:
        raise ManagedInputError(_NO_DATA)


def compile_descriptor(
    processor: Callable[..., Any], *, context: DeclarationContext
) -> ManagedDescriptor:
    """Compile one processor into its single managed descriptor.

    This runs before source discovery and before durable mutation, so a bad
    declaration fails without having touched any state.
    """
    builder = Inputs()
    result: Any = processor(builder, context=context)
    declared: ManagedDescriptor | None = builder._descriptor

    if not isinstance(result, ManagedDescriptor):
        raise ManagedInputError(
            f"Step '{context.step_name}' must return the descriptor from "
            "inputs.each(callback) or inputs.all(callback), got "
            f"{type(result).__name__}. A processor declares its work; it does not "
            "compute a payload."
        )
    if result.issuer is not builder._issuer:
        raise ManagedInputError(
            f"Step '{context.step_name}' returned a descriptor built by another "
            "declaration. Each processor must return the descriptor its own "
            "inputs builder produced."
        )
    if declared is not result:  # pragma: no cover - issuer check already covers this
        raise ManagedInputError(
            f"Step '{context.step_name}' returned a descriptor it did not declare."
        )
    descriptor: ManagedDescriptor = result
    return descriptor


def internal_descriptor(callback: Callable[..., Any]) -> ManagedDescriptor:
    """Build a descriptor for a synthetic node the scheduler owns.

    Branch and merge nodes are scheduler operations, not user declarations, so
    they carry a descriptor without going through builder validation.
    """
    builder = Inputs()
    return builder.each(callback)


class PartitionInput:
    """One durable batch of one partition's input, readable on demand.

    With the default ``batch_size=None`` this is the whole partition and
    ``batch_id`` is the partition's single batch.
    """

    __slots__ = ("partition_id", "batch_id", "_payload")

    def __init__(self, *, partition_id: str, batch_id: str, payload: Any) -> None:
        self.partition_id = partition_id
        self.batch_id = batch_id
        self._payload = payload

    def load(self) -> Any:
        """Return this batch's input payload."""
        return self._payload

    def __repr__(self) -> str:
        return f"PartitionInput(partition_id={self.partition_id!r}, batch_id={self.batch_id!r})"


class AggregateInput:
    """A reader over the partition views feeding one aggregate work unit.

    Views are read on demand, so an ``all`` callback stays bounded in memory
    even when the job has many partitions. Under an explicit ``batch_size`` this
    reader is restricted to one contiguous slice of views, identified by
    ``batch_id``.
    """

    __slots__ = ("partition_id", "batch_id", "_views")

    def __init__(self, views: Sequence[Any], *, partition_id: str, batch_id: str) -> None:
        self.partition_id = partition_id
        self.batch_id = batch_id
        self._views = tuple(views)

    def partitions(self) -> Iterator[Any]:
        """Iterate this batch's partition views without materializing payloads."""
        return iter(self._views)

    def __len__(self) -> int:
        return len(self._views)

    @property
    def is_empty(self) -> bool:
        """True when the sealed input for this batch contains no partitions."""
        return not self._views

    def __repr__(self) -> str:
        return f"AggregateInput(batch_id={self.batch_id!r}, partitions={len(self._views)})"


__all__ = [
    "AggregateInput",
    "DeclarationContext",
    "HelperKind",
    "Inputs",
    "ManagedDescriptor",
    "ManagedInputError",
    "PartitionInput",
    "compile_descriptor",
    "internal_descriptor",
]
